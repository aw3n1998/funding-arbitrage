"""
PO3/AMD 剥头皮策略机器人 — 主入口（Bitget 专版）

数据驱动方式：
    WebSocket 流（ccxt.pro）→ DataFeed 维护实时 K 线
    → 15m K 线收盘事件触发 Accumulation/Manipulation 检测
    → 发现 Manipulation 后监听 1m K 线收盘事件触发入场信号检测
    → 入场后每 5s tick 驱动 trailing stop 管理

运行方式:
    python main.py                  # 实盘 / 测试网（按 .env 配置）
    python main.py --dry-run        # 不真实下单，只跑逻辑和日志
    python main.py --scan           # 一次性打印当前 PO3 阶段，不运行机器人
"""
import argparse
import asyncio
import signal as os_signal
import sys
from datetime import datetime

import ccxt.pro as ccxtpro
from loguru import logger

from config import load_config, PO3Config
from po3.data_feed import DataFeed
from po3.detector import PO3Detector
from po3.executor import PO3Executor, PositionState
from po3.logger import TradeLogger
from po3.risk_manager import RiskManager
from utils.logger import setup_logger


# ─────────────────────────── 交易所构建（仅 Bitget）────────────────────────────


def build_exchange(cfg: PO3Config) -> ccxtpro.Exchange:
    """构建 Bitget ccxt.pro 实例"""
    params = {
        "apiKey": cfg.api_key,
        "secret": cfg.api_secret,
        "password": cfg.api_passphrase,   # Bitget 必填 passphrase
        "enableRateLimit": True,
        "options": {
            "defaultType": "swap",
            "defaultSubType": "linear",
        },
    }
    if cfg.testnet:
        params["sandbox"] = True

    exchange = ccxtpro.bitget(params)
    return exchange


# ──────────────────────────── 机器人主体 ─────────────────────────────────────


class PO3Bot:
    """
    PO3/AMD 剥头皮机器人（事件驱动架构）

    任务拓扑：
        DataFeed.start()       ← 持续维护 WS 流
        _loop_15m()            ← 等待 15m K线收盘事件 → 检测 Acc/Manip
        _loop_1m()             ← 等待 1m K线收盘事件 → 检测入场信号
        _loop_trailing()       ← 每 5s tick → 管理 trailing stop
        _loop_status()         ← 每 60s 打印持仓状态
    """

    def __init__(self, cfg: PO3Config, dry_run: bool = False):
        self.cfg = cfg
        self.dry_run = dry_run
        self.exchange: ccxtpro.Exchange = build_exchange(cfg)
        self.feed = DataFeed(self.exchange, cfg.symbol)
        self.detector = PO3Detector(cfg)
        self.risk = RiskManager(cfg)
        self.tlog = TradeLogger()
        self.executor = PO3Executor(
            self.exchange, cfg, self.risk, self.tlog, dry_run=dry_run
        )
        self._running = False
        # [BUG9 修复] Manipulation 状态：None = 扫描中，有值 = 候信中
        self._current_manip = None   # ManipulationEvent | None

    # ──────────────────── 启动 / 停止 ────────────────────

    async def start(self) -> None:
        logger.info("=" * 65)
        logger.info("  PO3/AMD 剥头皮机器人 (Bitget WebSocket)")
        logger.info(f"  网络     : {'测试网' if self.cfg.testnet else '实盘'}")
        logger.info(f"  标的     : {self.cfg.symbol}")
        logger.info(f"  杠杆     : {self.cfg.leverage}x (isolated)")
        logger.info(f"  每笔风险 : {self.cfg.risk_per_trade*100:.1f}%")
        logger.info(f"  每日上限 : {self.cfg.max_daily_trades}次 / "
                    f"最大亏损{self.cfg.max_daily_loss*100:.1f}%")
        logger.info(f"  RR      : TP1={self.cfg.tp1_rr} TP2={self.cfg.tp2_rr}")
        logger.info(f"  模式     : {'DRY RUN' if self.dry_run else '实盘'}")
        logger.info("=" * 65)

        # 加载市场
        try:
            await self.exchange.load_markets()
            logger.info(f"市场加载完成")
        except Exception as e:
            logger.error(f"市场加载失败: {e}")
            return

        # 获取初始权益（[BUG10] 失败不使用默认值，只记录 warning）
        equity = await self._fetch_equity()
        if equity is not None:
            self.risk.set_daily_start_equity(equity)
            logger.info(f"账户权益: {equity:.2f} USDT")
        else:
            logger.warning("初始权益获取失败，每笔交易前会重新检查")

        self._running = True

        await asyncio.gather(
            self.feed.start(),          # WS 数据流（阻塞）
            self._loop_15m(),
            self._loop_1m(),
            self._loop_trailing(),
            self._loop_status(),
            return_exceptions=True,
        )

    async def shutdown(self) -> None:
        logger.info("正在安全退出...")
        self._running = False
        await self.feed.stop()
        await self.executor.emergency_close()
        try:
            await self.exchange.close()
        except Exception:
            pass
        logger.info("机器人已安全退出")

    # ──────────────────── 15m 事件循环 ────────────────────

    async def _loop_15m(self) -> None:
        """
        等待 15m K 线收盘事件。
        每次收盘运行 Accumulation → Manipulation 检测。
        """
        # 等待 DataFeed 初始化完成
        while self._running and not self.feed.is_ready:
            await asyncio.sleep(1)
        logger.info("[15M] 数据就绪，开始监听 15m K线收盘")

        while self._running:
            try:
                # 等待收盘事件（带超时，防止 WS 断开时永久阻塞）
                try:
                    await asyncio.wait_for(
                        self._wait_event(self.feed.candle_closed_15m),
                        timeout=1800,  # 最多等 30 分钟（一根 15m K线的时长）
                    )
                except asyncio.TimeoutError:
                    logger.warning("[15M] 等待 K线收盘超时，检查 WebSocket 连接")
                    continue

                if not self._running:
                    break

                # 持仓中不重新扫描（专注管理当前持仓）
                if self.executor.is_in_position:
                    continue

                await self._on_15m_close()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[15M] 循环异常: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def _on_15m_close(self) -> None:
        """15m 收盘时的检测逻辑"""
        df_15m = self.feed.get_df_15m()
        if len(df_15m) < 20:
            return

        # ── 检测 Accumulation ──
        acc = self.detector.detect_accumulation(df_15m)
        if acc is None:
            logger.debug("[15M] 无累积区间")
            # 没有累积区间时清除上次 Manipulation（避免持有过期状态）
            if self._current_manip is not None:
                self._clear_manipulation("累积区间消失，放弃旧 Manipulation")
            return

        self.tlog.log_po3_phase(
            "accumulation",
            f"H:{acc.high:.2f} L:{acc.low:.2f} ATR:{acc.atr:.2f}",
            self.cfg.symbol,
        )

        # ── 检测 Manipulation ──
        manip = self.detector.detect_manipulation(df_15m, acc)
        if manip is None:
            logger.debug(f"[15M] 等待 Manipulation... {acc}")
            return

        # [BUG9 修复] 去重检查已在 detector._is_new_manipulation 中完成
        # 这里额外检查：如果已在候信模式，且是同一个 Manipulation，忽略
        if self._current_manip is not None:
            if manip.fingerprint() == self._current_manip.fingerprint():
                logger.debug("[15M] 同一 Manipulation，保持候信模式")
                return

        self._current_manip = manip
        self.detector.reset_entry_dedup()   # 新 Manipulation 重置入场去重
        self.tlog.log_po3_phase("manipulation", str(manip), self.cfg.symbol)
        logger.info(f"[15M] Manipulation 识别: {manip} → 进入 1m 候信模式")

    # ──────────────────── 1m 事件循环 ────────────────────

    async def _loop_1m(self) -> None:
        """
        等待 1m K 线收盘事件。
        仅在有 Manipulation 信号时检测入场。
        """
        while self._running and not self.feed.is_ready:
            await asyncio.sleep(1)
        logger.info("[1M] 数据就绪，开始监听 1m K线收盘")

        while self._running:
            try:
                try:
                    await asyncio.wait_for(
                        self._wait_event(self.feed.candle_closed_1m),
                        timeout=120,
                    )
                except asyncio.TimeoutError:
                    continue

                if not self._running:
                    break

                # 没有 Manipulation 信号 或 已在持仓中 → 跳过
                if self._current_manip is None or self.executor.is_in_position:
                    continue

                await self._on_1m_close()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[1M] 循环异常: {e}", exc_info=True)
                await asyncio.sleep(2)

    async def _on_1m_close(self) -> None:
        """1m 收盘时的入场检测逻辑"""
        manip = self._current_manip

        # Manipulation 有效窗口：超过 manip_max_age_bars × 15min 放弃
        age_secs = (datetime.now() - manip.timestamp).total_seconds()
        max_age_secs = self.cfg.manip_max_age_bars * 15 * 60
        if age_secs > max_age_secs:
            self._clear_manipulation(
                f"Manipulation 超时 {age_secs/60:.0f}min > "
                f"{max_age_secs/60:.0f}min，放弃"
            )
            return

        # [BUG10] 风控检查：权益获取失败则拒绝
        equity = await self._fetch_equity()
        can, reason = self.risk.can_trade(equity)
        if not can:
            self.tlog.log_signal_rejected(reason, self.cfg.symbol)
            self._clear_manipulation(f"风控拒绝: {reason}")
            return

        # 拉取已收盘 1m K 线
        df_1m = self.feed.get_df_1m()
        if len(df_1m) < 5:
            return

        # 检测入场信号
        signal = self.detector.detect_entry_signal(df_1m, manip)
        if signal is None:
            logger.debug("[1M] 等待入场信号...")
            return

        logger.info(f"[1M] 入场信号确认: {signal}")
        self.tlog.log_po3_phase("distribution", str(signal), self.cfg.symbol)

        # 执行入场
        atr = PO3Detector.get_current_atr(df_1m)
        success = await self.executor.enter(signal, equity, atr)

        if success:
            logger.info("[1M] 入场成功，等待持仓结果")
        else:
            logger.warning("[1M] 入场失败")

        # 入场后（无论成功）清除当前 Manipulation，等待下一次机会
        self._clear_manipulation("入场尝试后清除")

    def _clear_manipulation(self, reason: str) -> None:
        """清除当前 Manipulation 状态，回到 15m 扫描模式"""
        if self._current_manip is not None:
            logger.info(f"[BOT] 清除 Manipulation: {reason}")
        self._current_manip = None
        self.detector.reset_entry_dedup()
        self.detector.reset_manip_fingerprint()

    # ──────────────────── Trailing Stop 循环 ────────────────────

    async def _loop_trailing(self) -> None:
        """
        每 poll_interval_1m 秒调用 executor.tick()。
        使用 DataFeed.last_price（WS ticker 实时价格），无需额外 API 请求。
        """
        while self._running:
            try:
                await asyncio.sleep(self.cfg.poll_interval_1m)

                if not self.executor.is_in_position:
                    continue
                if self.feed.last_price <= 0:
                    logger.debug("[TRAIL] 等待实时价格...")
                    continue

                # 用已收盘 1m K 线计算 ATR
                df_1m = self.feed.get_df_1m()
                atr = PO3Detector.get_current_atr(df_1m) if len(df_1m) >= 15 else 0.0

                await self.executor.tick(self.feed.last_price, atr)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[TRAIL] 循环异常: {e}", exc_info=True)
                await asyncio.sleep(5)

    # ──────────────────── 状态打印循环 ────────────────────

    async def _loop_status(self) -> None:
        """每 60s 打印一次机器人状态"""
        while self._running:
            try:
                await asyncio.sleep(60)
                await self._print_status()
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _print_status(self) -> None:
        price = self.feed.last_price
        equity = await self._fetch_equity()
        equity_str = f"{equity:.2f}" if equity else "N/A"
        pos = self.executor.position

        if pos:
            if price > 0:
                if pos.direction == "long":
                    unreal = (price - pos.entry_price) * pos.contracts_total
                else:
                    unreal = (pos.entry_price - price) * pos.contracts_total
            else:
                unreal = 0.0
            holding_min = (datetime.now() - pos.opened_at).total_seconds() / 60
            logger.info(
                f"[STATUS] {pos.direction.upper()} {pos.contracts_total:.4f}张 @ "
                f"{pos.entry_price:.2f} | 现价:{price:.2f} | "
                f"未实现:{unreal:+.4f} USDT | SL:{pos.stop_loss:.2f} | "
                f"持仓:{holding_min:.1f}min | 状态:{self.executor.state.value}"
            )
        else:
            manip_str = f"候信({self._current_manip.bias})" if self._current_manip else "扫描中"
            logger.info(
                f"[STATUS] 空仓 | 权益:{equity_str} USDT | "
                f"今日:{self.risk.daily_trades_count}笔 | 模式:{manip_str} | "
                f"WS价格:{price:.2f}"
            )

    # ──────────────────── 工具 ────────────────────

    @staticmethod
    async def _wait_event(event: asyncio.Event) -> None:
        """等待事件并自动清除（避免调用方忘记 clear）"""
        await event.wait()
        event.clear()

    async def _fetch_equity(self):
        """
        [BUG10 修复] 获取账户 USDT 权益。
        失败返回 None（不使用任何默认值）。
        """
        try:
            balance = await self.exchange.fetch_balance({"type": "swap"})
            usdt = balance.get("USDT") or balance.get("usdt") or {}
            if isinstance(usdt, dict):
                equity = float(usdt.get("total") or usdt.get("equity") or 0)
            else:
                equity = float(usdt)
            return equity if equity > 0 else None
        except Exception as e:
            logger.warning(f"[BOT] 获取权益失败: {e}")
            return None


# ──────────────────────────── Scan 模式 ─────────────────────────────────────


async def run_scan(cfg: PO3Config) -> None:
    """一次性打印当前 BTC/USDT PO3 阶段，不运行机器人"""
    import ccxt.async_support as ccxt_async

    # Scan 模式用 REST（不需要 WS）
    rest_cls = getattr(ccxt_async, "bitget")
    exchange = rest_cls({
        "apiKey": cfg.api_key,
        "secret": cfg.api_secret,
        "password": cfg.api_passphrase,
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
        **({"sandbox": True} if cfg.testnet else {}),
    })
    detector = PO3Detector(cfg)

    try:
        await exchange.load_markets()

        raw_15m = await exchange.fetch_ohlcv(cfg.symbol, "15m", limit=100)
        df_15m = PO3Detector.candles_to_df(raw_15m)
        df_15m_closed = df_15m.iloc[:-1]  # 排除未收盘蜡烛
        atr = PO3Detector.get_current_atr(df_15m_closed)

        print(f"\n{'='*62}")
        print(f"  PO3 阶段扫描  |  {cfg.symbol}  |  {datetime.now():%H:%M:%S}")
        print(f"{'='*62}")
        print(f"  ATR(14): {atr:.2f}  |  最新收盘: {float(df_15m_closed['close'].iloc[-1]):.2f}")

        acc = detector.detect_accumulation(df_15m_closed)
        if acc is None:
            print("  阶段: 无累积区间（趋势或高波动状态）")
        else:
            print(f"  阶段: ACCUMULATION")
            print(f"  区间: H={acc.high:.2f}  L={acc.low:.2f}  高度={acc.height:.2f}")

            manip = detector.detect_manipulation(df_15m_closed, acc)
            if manip is None:
                print("  等待 Manipulation 假突破...")
            else:
                print(f"  阶段: MANIPULATION  bias={manip.bias.upper()}")
                print(f"  假突破: {manip.direction}  极值={manip.extreme:.2f}")

                raw_1m = await exchange.fetch_ohlcv(cfg.symbol, "1m", limit=50)
                df_1m = PO3Detector.candles_to_df(raw_1m)
                df_1m_closed = df_1m.iloc[:-1]

                fvg = detector.find_fvg_1m(
                    df_1m_closed,
                    "bullish" if manip.bias == "bullish" else "bearish"
                )
                if fvg:
                    print(f"  FVG(1m): {fvg[0]:.2f} ~ {fvg[1]:.2f}")
                else:
                    print("  FVG(1m): 未发现")

                signal = detector.detect_entry_signal(df_1m_closed, manip)
                if signal:
                    print(f"  阶段: DISTRIBUTION  →  {signal}")
                else:
                    print("  等待 1m 入场信号...")

        print(f"{'='*62}\n")
    finally:
        await exchange.close()


# ──────────────────────────── 入口 ───────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="PO3/AMD 剥头皮机器人 (Bitget)")
    parser.add_argument("--dry-run", action="store_true",
                        help="不真实下单，只跑策略逻辑和日志")
    parser.add_argument("--scan",    action="store_true",
                        help="一次性打印当前 PO3 阶段，不运行机器人")
    args = parser.parse_args()

    cfg = load_config()
    setup_logger(cfg.log_level)

    if args.scan:
        asyncio.run(run_scan(cfg))
        return

    bot = PO3Bot(cfg, dry_run=args.dry_run)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _handle_signal():
        logger.info("收到退出信号...")
        loop.create_task(bot.shutdown())
        for task in asyncio.all_tasks(loop):
            task.cancel()

    for sig in (os_signal.SIGINT, os_signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    try:
        loop.run_until_complete(bot.start())
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        if not loop.is_closed():
            loop.run_until_complete(bot.shutdown())
            loop.close()


if __name__ == "__main__":
    main()
