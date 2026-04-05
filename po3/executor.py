"""
PO3 交易执行器

状态机：
    IDLE → ENTERING → IN_POSITION → PARTIAL_EXIT → CLOSED

职责：
1. 入场：市价单开仓
2. 挂 SL 止损单（stop-market）
3. 挂 TP1 限价单（50% 仓位）
4. TP2 Trailing Stop 管理（轮询调整 SL 单）
5. 持仓监控与平仓处理
"""
import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

import ccxt.async_support as ccxt
from loguru import logger

from po3.detector import EntrySignal, ManipulationEvent
from po3.risk_manager import RiskManager, TradeRecord
from po3.logger import TradeLogger


class PositionState(str, Enum):
    IDLE = "idle"
    ENTERING = "entering"
    IN_POSITION = "in_position"
    PARTIAL_EXIT = "partial_exit"   # TP1 已成交，剩余仓位在 trailing
    CLOSED = "closed"


@dataclass
class ActivePosition:
    """当前持仓快照"""
    trade_id: str
    direction: str                  # "long" | "short"
    symbol: str
    entry_price: float
    contracts_total: float          # 开仓总张数
    contracts_remaining: float      # 当前剩余张数
    stop_loss: float
    tp1: float
    tp2: float
    sl_order_id: Optional[str]      # 交易所 SL 订单 ID
    tp1_order_id: Optional[str]     # 交易所 TP1 限价单 ID
    manipulation: ManipulationEvent
    opened_at: datetime = field(default_factory=datetime.now)
    state: PositionState = PositionState.IN_POSITION
    trailing_sl: Optional[float] = None   # trailing stop 当前价位


class PO3Executor:
    """
    PO3 策略执行器

    支持：
    - Binance / Bybit 永续合约
    - Testnet 模式
    - Dry run 模式（不真实下单）
    """

    # 各交易所平仓参数
    _CLOSE_PARAMS = {
        "binance": {"reduceOnly": True},
        "bybit":   {"reduceOnly": True},
    }

    def __init__(self, exchange: ccxt.Exchange, config, risk_manager: RiskManager,
                 trade_logger: TradeLogger, dry_run: bool = False):
        self.exchange = exchange
        self.cfg = config
        self.risk = risk_manager
        self.tlog = trade_logger
        self.dry_run = dry_run
        self.position: Optional[ActivePosition] = None
        self.state: PositionState = PositionState.IDLE
        self._trailing_task: Optional[asyncio.Task] = None

    # ──────────────────── 入场 ────────────────────

    async def enter(
        self,
        signal: EntrySignal,
        equity: float,
        atr: float,
    ) -> bool:
        """
        执行入场全流程：
        1. 计算仓位
        2. 设置杠杆
        3. 市价开仓
        4. 挂 SL 止损单
        5. 挂 TP1 限价单
        """
        if self.state != PositionState.IDLE:
            logger.warning(f"[EXE] 当前状态 {self.state}，跳过入场")
            return False

        direction = signal.direction
        entry = signal.entry_price
        sl, tp1, tp2 = self.risk.calculate_tp_sl(
            entry, signal.manipulation.extreme, atr, direction
        )
        contracts = self.risk.calculate_position_size(equity, entry, sl)

        if contracts <= 0:
            logger.error("[EXE] 仓位为0，中止入场")
            return False

        trade_id = str(uuid.uuid4())[:8]
        self.state = PositionState.ENTERING
        logger.info(
            f"[EXE] 准备入场 {direction.upper()} | "
            f"合约:{contracts} 入场:{entry:.2f} "
            f"SL:{sl:.2f} TP1:{tp1:.2f} TP2:{tp2:.2f}"
        )

        # ── 1. 设置杠杆 ──
        await self._set_leverage()

        # ── 2. 市价开仓 ──
        side = "buy" if direction == "long" else "sell"
        entry_order = await self._place_market_order(side, contracts)
        if entry_order is None and not self.dry_run:
            self.state = PositionState.IDLE
            logger.error("[EXE] 市价开仓失败")
            return False

        actual_entry = (
            float(entry_order.get("average") or entry_order.get("price") or entry)
            if entry_order else entry
        )

        # ── 3. 挂 SL 止损单 ──
        sl_order_id = await self._place_sl_order(direction, contracts, sl)

        # ── 4. 挂 TP1 限价单（50% 仓位）──
        tp1_contracts = round(contracts * self.cfg.tp1_close_pct, 4)
        tp1_order_id = await self._place_tp1_order(direction, tp1_contracts, tp1)

        # ── 记录持仓 ──
        self.position = ActivePosition(
            trade_id=trade_id,
            direction=direction,
            symbol=self.cfg.symbol,
            entry_price=actual_entry,
            contracts_total=contracts,
            contracts_remaining=contracts,
            stop_loss=sl,
            tp1=tp1,
            tp2=tp2,
            sl_order_id=sl_order_id,
            tp1_order_id=tp1_order_id,
            manipulation=signal.manipulation,
        )
        self.state = PositionState.IN_POSITION

        record = TradeRecord(
            trade_id=trade_id,
            direction=direction,
            entry_price=actual_entry,
            stop_loss=sl,
            tp1=tp1,
            tp2=tp2,
            contracts=contracts,
            risk_usdt=equity * self.cfg.risk_per_trade,
            equity_at_entry=equity,
        )
        self.risk.record_trade_open(record)
        self.tlog.log_entry(self.position, signal, equity, atr)

        # ── 5. 启动 Trailing Stop 后台任务 ──
        self._trailing_task = asyncio.create_task(
            self._manage_trailing_stop(), name="trailing_stop"
        )

        logger.info(f"[EXE] 入场完成 trade_id={trade_id}")
        return True

    # ──────────────────── Trailing Stop ──────────────────

    async def _manage_trailing_stop(self) -> None:
        """
        后台任务：监控持仓，调整 trailing stop。

        逻辑：
        - TP1 成交后激活 trailing
        - 每 poll_interval_1m 秒更新一次
        - ATR 动态计算 trail 距离（ATR * 1.5）
        - 持续向有利方向移动 SL，不可回退
        """
        pos = self.position
        if pos is None:
            return

        logger.info("[TRAIL] Trailing stop 监控已启动")

        while self.state in (PositionState.IN_POSITION, PositionState.PARTIAL_EXIT):
            try:
                await asyncio.sleep(self.cfg.poll_interval_1m)

                if self.position is None:
                    break

                # 检查 TP1 是否已成交
                if (self.state == PositionState.IN_POSITION
                        and pos.tp1_order_id):
                    filled = await self._check_order_filled(pos.tp1_order_id)
                    if filled:
                        remaining = round(
                            pos.contracts_total * (1 - self.cfg.tp1_close_pct), 4
                        )
                        pos.contracts_remaining = remaining
                        self.state = PositionState.PARTIAL_EXIT
                        logger.info(
                            f"[TRAIL] TP1 已成交 @ {pos.tp1:.2f} "
                            f"剩余仓位: {remaining} 张"
                        )
                        self.tlog.log_tp1(pos)

                # 检查 SL 是否触发（持仓已被平）
                if pos.sl_order_id:
                    sl_filled = await self._check_order_filled(pos.sl_order_id)
                    if sl_filled:
                        logger.warning(
                            f"[TRAIL] SL 已触发 @ {pos.stop_loss:.2f}"
                        )
                        await self._on_position_closed("sl", pos.stop_loss)
                        break

                # ── Trailing Stop 更新 ──
                if self.state == PositionState.PARTIAL_EXIT:
                    ticker = await self._fetch_ticker()
                    if ticker is None:
                        continue
                    current_price = float(ticker["last"])

                    # 用 1m 图 ATR 动态计算 trail 距离
                    from po3.detector import PO3Detector
                    df_1m = await self._fetch_ohlcv("1m", 20)
                    atr = PO3Detector.get_current_atr(df_1m) if df_1m is not None else 0
                    trail_dist = atr * 1.5 if atr > 0 else pos.stop_loss * 0.005

                    if pos.direction == "long":
                        new_trail = current_price - trail_dist
                        # 只向上移动，不后退
                        if pos.trailing_sl is None or new_trail > pos.trailing_sl:
                            if new_trail > pos.stop_loss:
                                pos.trailing_sl = new_trail
                                await self._update_sl_order(pos, new_trail)
                                logger.debug(
                                    f"[TRAIL] SL 上移至 {new_trail:.2f} "
                                    f"(价格:{current_price:.2f})"
                                )
                    else:
                        new_trail = current_price + trail_dist
                        if pos.trailing_sl is None or new_trail < pos.trailing_sl:
                            if new_trail < pos.stop_loss:
                                pos.trailing_sl = new_trail
                                await self._update_sl_order(pos, new_trail)
                                logger.debug(
                                    f"[TRAIL] SL 下移至 {new_trail:.2f} "
                                    f"(价格:{current_price:.2f})"
                                )

                    # 检查是否已达 TP2
                    if pos.direction == "long" and current_price >= pos.tp2:
                        logger.info(f"[TRAIL] 达到 TP2 目标 {pos.tp2:.2f}，平仓剩余")
                        await self._close_remaining(pos, current_price, "tp2")
                        break
                    elif pos.direction == "short" and current_price <= pos.tp2:
                        logger.info(f"[TRAIL] 达到 TP2 目标 {pos.tp2:.2f}，平仓剩余")
                        await self._close_remaining(pos, current_price, "tp2")
                        break

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[TRAIL] 异常: {e}", exc_info=True)
                await asyncio.sleep(5)

        logger.info("[TRAIL] Trailing stop 监控结束")

    # ──────────────────── 下单辅助 ────────────────────

    async def _set_leverage(self) -> None:
        if self.dry_run:
            logger.info(f"[DRY] set_leverage {self.cfg.leverage}x")
            return
        try:
            await self.exchange.set_leverage(self.cfg.leverage, self.cfg.symbol)
            logger.info(f"[EXE] 杠杆设置: {self.cfg.leverage}x")
        except Exception as e:
            logger.warning(f"[EXE] set_leverage 失败（可能已设置）: {e}")

    async def _place_market_order(
        self, side: str, amount: float
    ) -> Optional[dict]:
        if self.dry_run:
            logger.info(f"[DRY] 市价 {side} {amount} {self.cfg.symbol}")
            return {"average": None, "price": None, "id": "dry_entry"}
        try:
            order = await self.exchange.create_order(
                self.cfg.symbol, "market", side, amount
            )
            logger.info(
                f"[EXE] 市价单成交 {side} {amount} "
                f"@ {order.get('average') or order.get('price')} "
                f"ID:{order.get('id')}"
            )
            return order
        except Exception as e:
            logger.error(f"[EXE] 市价单失败: {e}")
            return None

    async def _place_sl_order(
        self, direction: str, amount: float, sl_price: float
    ) -> Optional[str]:
        """挂止损单（stop-market）"""
        side = "sell" if direction == "long" else "buy"
        close_params = self._CLOSE_PARAMS.get(self.cfg.exchange, {"reduceOnly": True})

        if self.dry_run:
            logger.info(f"[DRY] SL {side} {amount} @ stop {sl_price:.2f}")
            return "dry_sl"
        try:
            order = await self.exchange.create_order(
                self.cfg.symbol, "stop_market", side, amount,
                None,
                {**close_params, "stopPrice": sl_price},
            )
            oid = order.get("id")
            logger.info(f"[EXE] SL 单挂出 @ {sl_price:.2f} ID:{oid}")
            return oid
        except Exception as e:
            logger.error(f"[EXE] SL 单挂单失败: {e}")
            return None

    async def _place_tp1_order(
        self, direction: str, amount: float, tp1_price: float
    ) -> Optional[str]:
        """挂 TP1 限价单"""
        side = "sell" if direction == "long" else "buy"
        close_params = self._CLOSE_PARAMS.get(self.cfg.exchange, {"reduceOnly": True})

        if self.dry_run:
            logger.info(f"[DRY] TP1 {side} {amount} @ limit {tp1_price:.2f}")
            return "dry_tp1"
        try:
            order = await self.exchange.create_order(
                self.cfg.symbol, "limit", side, amount, tp1_price,
                close_params,
            )
            oid = order.get("id")
            logger.info(f"[EXE] TP1 单挂出 @ {tp1_price:.2f} ID:{oid}")
            return oid
        except Exception as e:
            logger.error(f"[EXE] TP1 单挂单失败: {e}")
            return None

    async def _update_sl_order(
        self, pos: ActivePosition, new_sl: float
    ) -> None:
        """取消旧 SL 单，挂新 SL 单"""
        side = "sell" if pos.direction == "long" else "buy"
        close_params = self._CLOSE_PARAMS.get(self.cfg.exchange, {"reduceOnly": True})

        if self.dry_run:
            logger.debug(f"[DRY] update SL → {new_sl:.2f}")
            pos.stop_loss = new_sl
            return

        # 取消旧单
        if pos.sl_order_id and pos.sl_order_id != "dry_sl":
            try:
                await self.exchange.cancel_order(pos.sl_order_id, self.cfg.symbol)
            except Exception as e:
                logger.warning(f"[EXE] 取消旧SL单失败: {e}")

        # 挂新单
        try:
            order = await self.exchange.create_order(
                self.cfg.symbol, "stop_market", side,
                pos.contracts_remaining, None,
                {**close_params, "stopPrice": new_sl},
            )
            pos.sl_order_id = order.get("id")
            pos.stop_loss = new_sl
        except Exception as e:
            logger.error(f"[EXE] 更新SL单失败: {e}")

    async def _close_remaining(
        self, pos: ActivePosition, price: float, reason: str
    ) -> None:
        """市价平掉剩余仓位"""
        side = "sell" if pos.direction == "long" else "buy"
        close_params = self._CLOSE_PARAMS.get(self.cfg.exchange, {"reduceOnly": True})

        if not self.dry_run and pos.contracts_remaining > 0:
            try:
                await self.exchange.create_order(
                    self.cfg.symbol, "market", side,
                    pos.contracts_remaining, None, close_params
                )
            except Exception as e:
                logger.error(f"[EXE] 平仓剩余失败: {e}")

        await self._on_position_closed(reason, price)

    async def _on_position_closed(self, reason: str, close_price: float) -> None:
        """统一处理持仓关闭后续"""
        pos = self.position
        if pos is None:
            return

        if pos.direction == "long":
            pnl = (close_price - pos.entry_price) * pos.contracts_total
        else:
            pnl = (pos.entry_price - close_price) * pos.contracts_total

        logger.info(
            f"[EXE] 持仓关闭 reason={reason} "
            f"close_price={close_price:.2f} PnL≈{pnl:+.4f} USDT"
        )
        self.tlog.log_close(pos, close_price, pnl, reason)
        self.risk.record_trade_close(pnl)

        self.position = None
        self.state = PositionState.IDLE

    # ──────────────────── 查询辅助 ────────────────────

    async def _check_order_filled(self, order_id: str) -> bool:
        if self.dry_run or order_id.startswith("dry_"):
            return False
        try:
            order = await self.exchange.fetch_order(order_id, self.cfg.symbol)
            return order.get("status") in ("closed", "filled")
        except Exception as e:
            logger.debug(f"[EXE] fetch_order {order_id}: {e}")
            return False

    async def _fetch_ticker(self) -> Optional[dict]:
        try:
            return await self.exchange.fetch_ticker(self.cfg.symbol)
        except Exception as e:
            logger.warning(f"[EXE] fetch_ticker 失败: {e}")
            return None

    async def _fetch_ohlcv(self, timeframe: str, limit: int):
        try:
            raw = await self.exchange.fetch_ohlcv(
                self.cfg.symbol, timeframe, limit=limit
            )
            from po3.detector import PO3Detector
            return PO3Detector.candles_to_df(raw)
        except Exception as e:
            logger.warning(f"[EXE] fetch_ohlcv({timeframe}) 失败: {e}")
            return None

    # ──────────────────── 紧急平仓 ────────────────────

    async def emergency_close(self) -> None:
        """程序退出时强制平仓"""
        if self.position is None or self.state == PositionState.IDLE:
            return
        logger.warning("[EXE] 紧急平仓中...")
        if self._trailing_task and not self._trailing_task.done():
            self._trailing_task.cancel()

        pos = self.position
        side = "sell" if pos.direction == "long" else "buy"
        close_params = self._CLOSE_PARAMS.get(self.cfg.exchange, {"reduceOnly": True})

        if not self.dry_run:
            try:
                # 先取消所有挂单
                for oid in [pos.sl_order_id, pos.tp1_order_id]:
                    if oid and not oid.startswith("dry_"):
                        try:
                            await self.exchange.cancel_order(oid, self.cfg.symbol)
                        except Exception:
                            pass
                # 市价平仓
                await self.exchange.create_order(
                    self.cfg.symbol, "market", side,
                    pos.contracts_remaining, None, close_params
                )
                logger.info("[EXE] 紧急平仓完成")
            except Exception as e:
                logger.error(f"[EXE] 紧急平仓失败: {e}")

        self.position = None
        self.state = PositionState.IDLE

    @property
    def is_in_position(self) -> bool:
        return self.state not in (PositionState.IDLE, PositionState.CLOSED)
