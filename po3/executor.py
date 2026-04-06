"""
PO3 交易执行器（仅支持 Bitget 永续合约）

修复记录：
  - [BUG2]  SL 挂单失败后立即市价平仓，不留裸仓
  - [BUG3]  Bitget 专用止损单参数（triggerPrice + stop_market）
  - [BUG4]  PnL 计算：TP1 和最终平仓分别按各自的 contracts 计算，不重复
  - [BUG8]  移除热循环内动态 import
  - [C1]    TP1 成交后立即将 SL 单数量同步缩减至剩余仓位，防止超额平仓
  - [H1]    margin_mode 改由 cfg.margin_mode 读取，不再硬编码 "isolated"
  - [M5]    _update_sl_order 失败时将 sl_order_id 置 None，避免旧 ID 残留轮询
  - 架构简化：移除内部 _trailing_task，trailing 由主循环调用 tick() 驱动

状态机：
    IDLE → ENTERING → IN_POSITION → PARTIAL_EXIT → CLOSED
"""
import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

import ccxt.pro as ccxtpro
from loguru import logger

from po3.detector import EntrySignal, ManipulationEvent, PO3Detector
from po3.risk_manager import RiskManager, TradeRecord
from po3.logger import TradeLogger


class PositionState(str, Enum):
    IDLE = "idle"
    ENTERING = "entering"
    IN_POSITION = "in_position"
    PARTIAL_EXIT = "partial_exit"   # TP1 已成交，剩余仓位 trailing
    CLOSED = "closed"


@dataclass
class ActivePosition:
    """当前持仓快照"""
    trade_id: str
    direction: str                  # "long" | "short"
    symbol: str
    entry_price: float
    contracts_total: float          # 开仓总张数
    contracts_remaining: float      # TP1 后剩余张数
    stop_loss: float
    tp1: float
    tp2: float
    sl_order_id: Optional[str]
    tp1_order_id: Optional[str]
    manipulation: ManipulationEvent
    opened_at: datetime = field(default_factory=datetime.now)
    state: PositionState = PositionState.IN_POSITION
    trailing_sl: Optional[float] = None
    # [BUG4] 分别记录两段 PnL
    tp1_pnl: float = 0.0            # TP1 成交时锁定的 PnL
    realized_pnl: float = 0.0       # 最终关闭 PnL（仅 contracts_remaining 部分）


class PO3Executor:
    """
    Bitget 永续合约 PO3 执行器

    Trailing Stop 由外部（main.py）调用 tick(price, atr) 驱动，
    不再使用内部后台 Task，避免循环依赖和热循环 import。
    """

    def __init__(
        self,
        exchange: ccxtpro.Exchange,
        config,
        risk_manager: RiskManager,
        trade_logger: TradeLogger,
        dry_run: bool = False,
    ):
        self.exchange = exchange
        self.cfg = config
        self.risk = risk_manager
        self.tlog = trade_logger
        self.dry_run = dry_run
        self.position: Optional[ActivePosition] = None
        self.state: PositionState = PositionState.IDLE

        # [H1] margin_mode 由配置决定，不硬编码
        self._stop_params = {
            "triggerType": "fill_price",    # fill_price（最新价触发）| mark_price
            "reduceOnly": True,
            "marginMode": config.margin_mode,
        }
        self._close_params = {
            "reduceOnly": True,
            "marginMode": config.margin_mode,
        }

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
        4. 挂 SL 止损单；[BUG2] 失败则立即市价平仓
        5. 挂 TP1 限价单
        """
        if self.state != PositionState.IDLE:
            logger.warning(f"[EXE] 状态 {self.state}，跳过入场")
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
            logger.error("[EXE] 市价开仓失败，中止")
            return False

        actual_entry = (
            float(entry_order.get("average") or entry_order.get("price") or entry)
            if entry_order else entry
        )

        # ── 3. 挂 SL 止损单（[BUG2] 失败则立即平仓）──
        sl_order_id = await self._place_sl_order(direction, contracts, sl)
        if sl_order_id is None and not self.dry_run:
            logger.critical("[EXE] SL 挂单失败！立即市价平仓，不留裸仓")
            await self._emergency_market_close(direction, contracts)
            self.state = PositionState.IDLE
            return False

        # ── 4. 挂 TP1 限价单（tp1_close_pct 比例仓位）──
        tp1_contracts = round(contracts * self.cfg.tp1_close_pct, 4)
        tp1_order_id = await self._place_tp1_order(direction, tp1_contracts, tp1)
        if tp1_order_id is None and not self.dry_run:
            logger.warning("[EXE] TP1 挂单失败，将完全依赖 trailing stop")

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

        logger.info(f"[EXE] 入场完成 trade_id={trade_id}")
        return True

    # ──────────────────── 持仓 Tick（由主循环每 5s 调用）────────────────────

    async def tick(self, current_price: float, current_atr: float) -> None:
        """
        主循环每隔 poll_interval_1m 秒调用一次。
        传入最新价格和 1m ATR，负责：
          1. 检查 TP1 是否成交
          2. 检查 SL 是否触发
          3. 更新 trailing stop（仅在 TP1 成交后）
        """
        if self.position is None or self.state == PositionState.IDLE:
            return

        pos = self.position

        # ── 1. 检查 TP1 是否成交 ──
        if self.state == PositionState.IN_POSITION and pos.tp1_order_id:
            tp1_filled = await self._check_order_filled(pos.tp1_order_id)
            if tp1_filled:
                await self._on_tp1_filled(pos)

        # ── 2. 检查 SL 是否触发 ──
        if pos.sl_order_id:
            sl_filled = await self._check_order_filled(pos.sl_order_id)
            if sl_filled:
                logger.warning(f"[EXE] SL 触发 @ {pos.stop_loss:.2f}")
                await self._on_position_closed("sl", pos.stop_loss, pos.contracts_remaining)
                return

        # ── 3. Trailing Stop（TP1 成交后激活）──
        if self.state == PositionState.PARTIAL_EXIT:
            await self._update_trailing(pos, current_price, current_atr)

            # 检查是否达到 TP2 目标
            if pos.direction == "long" and current_price >= pos.tp2:
                logger.info(f"[EXE] 达到 TP2 {pos.tp2:.2f}，平仓剩余")
                await self._close_remaining(pos, current_price, "tp2")
            elif pos.direction == "short" and current_price <= pos.tp2:
                logger.info(f"[EXE] 达到 TP2 {pos.tp2:.2f}，平仓剩余")
                await self._close_remaining(pos, current_price, "tp2")

    # ──────────────────── Trailing Stop ────────────────────

    async def _on_tp1_filled(self, pos: ActivePosition) -> None:
        """TP1 成交：记录 PnL，更新剩余仓位，同步缩减 SL 单数量，激活 trailing"""
        # [BUG4] TP1 PnL 只算 tp1_close_pct 的那部分仓位
        tp1_contracts = round(pos.contracts_total * self.cfg.tp1_close_pct, 4)
        if pos.direction == "long":
            pos.tp1_pnl = (pos.tp1 - pos.entry_price) * tp1_contracts
        else:
            pos.tp1_pnl = (pos.entry_price - pos.tp1) * tp1_contracts

        remaining = round(pos.contracts_total * (1 - self.cfg.tp1_close_pct), 4)
        pos.contracts_remaining = remaining
        self.state = PositionState.PARTIAL_EXIT

        logger.info(
            f"[EXE] TP1 成交 @ {pos.tp1:.2f} | "
            f"TP1 PnL:{pos.tp1_pnl:+.4f} USDT | "
            f"剩余仓位:{remaining} 张"
        )
        self.tlog.log_tp1(pos)

        # [C1] TP1 成交后立即将 SL 单数量缩减至剩余仓位
        # 原始 SL 单是按 contracts_total 挂的，TP1 后只剩 contracts_remaining
        # 若不更新，SL 触发时会尝试平超出持仓的数量，导致挂单失败或裸仓
        if pos.sl_order_id and not self.dry_run:
            logger.info(f"[EXE] TP1 后同步缩减 SL 单至 {remaining} 张 @ {pos.stop_loss:.2f}")
            await self._update_sl_order(pos, pos.stop_loss)

    async def _update_trailing(
        self, pos: ActivePosition, price: float, atr: float
    ) -> None:
        """按 ATR * trailing_atr_mult 动态上移/下移 SL"""
        trail_dist = atr * self.cfg.trailing_atr_mult if atr > 0 else price * 0.005

        if pos.direction == "long":
            new_trail = price - trail_dist
            # 只向有利方向（上）移动，且必须高于原始 SL
            if (new_trail > pos.stop_loss
                    and (pos.trailing_sl is None or new_trail > pos.trailing_sl)):
                pos.trailing_sl = new_trail
                await self._update_sl_order(pos, new_trail)
                logger.debug(f"[TRAIL] SL 上移 → {new_trail:.2f} (价格:{price:.2f})")
        else:
            new_trail = price + trail_dist
            # 只向有利方向（下）移动，且必须低于原始 SL
            if (new_trail < pos.stop_loss
                    and (pos.trailing_sl is None or new_trail < pos.trailing_sl)):
                pos.trailing_sl = new_trail
                await self._update_sl_order(pos, new_trail)
                logger.debug(f"[TRAIL] SL 下移 → {new_trail:.2f} (价格:{price:.2f})")

    # ──────────────────── 下单辅助 ────────────────────

    async def _set_leverage(self) -> None:
        if self.dry_run:
            logger.info(f"[DRY] set_leverage {self.cfg.leverage}x")
            return
        try:
            await self.exchange.set_leverage(
                self.cfg.leverage, self.cfg.symbol,
                {"marginMode": self.cfg.margin_mode}   # [H1]
            )
            logger.info(f"[EXE] 杠杆设置: {self.cfg.leverage}x ({self.cfg.margin_mode})")
        except Exception as e:
            logger.warning(f"[EXE] set_leverage 警告（可能已设置）: {e}")

    async def _place_market_order(
        self, side: str, amount: float
    ) -> Optional[dict]:
        if self.dry_run:
            logger.info(f"[DRY] 市价 {side} {amount} {self.cfg.symbol}")
            return {"average": None, "price": None, "id": "dry_entry"}
        try:
            order = await self.exchange.create_order(
                self.cfg.symbol, "market", side, amount,
                None, {"marginMode": self.cfg.margin_mode},   # [H1]
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
        """
        [BUG3] Bitget 专用止损单：
          - 订单类型: stop_market
          - 参数: triggerPrice（Bitget 原生字段）+ stopPrice（ccxt 统一字段）
        """
        side = "sell" if direction == "long" else "buy"
        if self.dry_run:
            logger.info(f"[DRY] SL stop_market {side} {amount} @ {sl_price:.2f}")
            return "dry_sl"
        try:
            order = await self.exchange.create_order(
                self.cfg.symbol, "stop_market", side, amount,
                None,
                {
                    **self._stop_params,
                    "stopPrice": sl_price,       # ccxt 统一字段
                    "triggerPrice": sl_price,    # Bitget 原生字段（备用）
                },
            )
            oid = order.get("id")
            logger.info(f"[EXE] SL 单挂出 @ {sl_price:.2f} ID:{oid}")
            return oid
        except Exception as e:
            logger.error(f"[EXE] SL 挂单失败: {e}")
            return None

    async def _place_tp1_order(
        self, direction: str, amount: float, tp1_price: float
    ) -> Optional[str]:
        """挂 TP1 限价单"""
        side = "sell" if direction == "long" else "buy"
        if self.dry_run:
            logger.info(f"[DRY] TP1 limit {side} {amount} @ {tp1_price:.2f}")
            return "dry_tp1"
        try:
            order = await self.exchange.create_order(
                self.cfg.symbol, "limit", side, amount, tp1_price,
                self._close_params,
            )
            oid = order.get("id")
            logger.info(f"[EXE] TP1 单挂出 @ {tp1_price:.2f} ID:{oid}")
            return oid
        except Exception as e:
            logger.error(f"[EXE] TP1 挂单失败: {e}")
            return None

    async def _update_sl_order(
        self, pos: ActivePosition, new_sl: float
    ) -> None:
        """
        取消旧 SL 单，挂新 SL 单（trailing 更新 / C1 TP1 后缩量 通用）。
        新单数量始终使用 pos.contracts_remaining（已在调用前更新）。
        """
        side = "sell" if pos.direction == "long" else "buy"
        if self.dry_run:
            pos.stop_loss = new_sl
            return
        # 取消旧单
        if pos.sl_order_id and not pos.sl_order_id.startswith("dry_"):
            try:
                await self.exchange.cancel_order(pos.sl_order_id, self.cfg.symbol)
            except Exception as e:
                logger.warning(f"[EXE] 取消旧SL单失败（可能已触发）: {e}")
        # 挂新单
        try:
            order = await self.exchange.create_order(
                self.cfg.symbol, "stop_market", side,
                pos.contracts_remaining, None,
                {
                    **self._stop_params,
                    "stopPrice": new_sl,
                    "triggerPrice": new_sl,
                },
            )
            pos.sl_order_id = order.get("id")
            pos.stop_loss = new_sl
        except Exception as e:
            logger.error(f"[EXE] 更新SL单失败: {e}")
            # [M5] 挂新单失败时将旧 ID 置 None，避免继续用已取消 ID 轮询
            pos.sl_order_id = None

    async def _close_remaining(
        self, pos: ActivePosition, price: float, reason: str
    ) -> None:
        """市价平掉剩余仓位"""
        side = "sell" if pos.direction == "long" else "buy"
        if not self.dry_run and pos.contracts_remaining > 0:
            try:
                await self.exchange.create_order(
                    self.cfg.symbol, "market", side,
                    pos.contracts_remaining, None,
                    self._close_params,
                )
            except Exception as e:
                logger.error(f"[EXE] 平仓剩余失败: {e}")
        await self._on_position_closed(reason, price, pos.contracts_remaining)

    async def _emergency_market_close(
        self, direction: str, contracts: float
    ) -> None:
        """紧急市价平仓（SL 挂单失败时使用）"""
        side = "sell" if direction == "long" else "buy"
        if self.dry_run:
            return
        try:
            await self.exchange.create_order(
                self.cfg.symbol, "market", side, contracts,
                None, self._close_params,
            )
            logger.info("[EXE] 紧急平仓执行完成")
        except Exception as e:
            logger.critical(f"[EXE] 紧急平仓失败！请手动处理持仓: {e}")

    async def _on_position_closed(
        self, reason: str, close_price: float, contracts_closed: float
    ) -> None:
        """
        [BUG4 修复] PnL 只按实际平仓的 contracts 计算，不重复计 TP1 部分。
        [H3] 传入 trade_id 给 risk_manager 用于更新记录。
        """
        pos = self.position
        if pos is None:
            return

        if pos.direction == "long":
            pnl = (close_price - pos.entry_price) * contracts_closed
        else:
            pnl = (pos.entry_price - close_price) * contracts_closed

        pos.realized_pnl = pnl
        total_pnl = pos.tp1_pnl + pnl

        logger.info(
            f"[EXE] 持仓关闭 reason={reason} "
            f"close_price={close_price:.2f} | "
            f"TP1 PnL:{pos.tp1_pnl:+.4f} + "
            f"本次:{pnl:+.4f} = 总:{total_pnl:+.4f} USDT"
        )
        self.tlog.log_close(pos, close_price, total_pnl, reason)
        # [H3] 传入 trade_id，risk_manager 可以关联并更新记录
        self.risk.record_trade_close(pos.trade_id, total_pnl)

        self.position = None
        self.state = PositionState.IDLE

    # ──────────────────── 查询辅助 ────────────────────

    async def _check_order_filled(self, order_id: Optional[str]) -> bool:
        # [M5] 防御 None：SL 更新失败后 order_id 可能为 None
        if not order_id:
            return False
        if self.dry_run or order_id.startswith("dry_"):
            return False
        try:
            order = await self.exchange.fetch_order(order_id, self.cfg.symbol)
            return order.get("status") in ("closed", "filled")
        except Exception as e:
            logger.debug(f"[EXE] fetch_order {order_id}: {e}")
            return False

    # ──────────────────── 紧急退出 ────────────────────

    async def emergency_close(self) -> None:
        """程序退出时强制平仓"""
        if self.position is None or self.state == PositionState.IDLE:
            return
        logger.warning("[EXE] 程序退出，紧急平仓...")
        pos = self.position
        # 取消所有挂单（TP1 若已成交则取消会报错，静默处理）
        for oid in [pos.sl_order_id, pos.tp1_order_id]:
            if oid and not oid.startswith("dry_") and not self.dry_run:
                try:
                    await self.exchange.cancel_order(oid, self.cfg.symbol)
                except Exception:
                    pass
        await self._emergency_market_close(pos.direction, pos.contracts_remaining)
        self.position = None
        self.state = PositionState.IDLE

    @property
    def is_in_position(self) -> bool:
        return self.state not in (PositionState.IDLE, PositionState.CLOSED)
