"""
PO3/AMD 剥头皮策略机器人 — 主入口

交易逻辑：
    15m 图 → 识别 Accumulation / Manipulation / Bias
    1m  图 → 等待入场信号（吞没/Pinbar/FVG回测）
    执行   → 市价入场 + SL + TP1(50%) + Trailing TP2

运行方式:
    python main.py                  # 实盘/测试网（按 .env 配置）
    python main.py --dry-run        # 不真实下单，只跑逻辑+日志
    python main.py --scan           # 只打印一次当前 PO3 阶段，不交易
"""
import argparse
import asyncio
import signal
import sys
from datetime import datetime

import ccxt.async_support as ccxt
from loguru import logger

from config import load_config, PO3Config
from po3.detector import PO3Detector
from po3.risk_manager import RiskManager
from po3.executor import PO3Executor, PositionState
from po3.logger import TradeLogger
from utils.logger import setup_logger


# ─────────────────────────────── 交易所初始化 ────────────────────────────────


def build_exchange(cfg: PO3Config) -> ccxt.Exchange:
    """根据配置构建 CCXT 交易所实例"""
    exchange_classes = {
        "binance": ccxt.binance,
        "bybit": ccxt.bybit,
    }
    cls = exchange_classes.get(cfg.exchange.lower())
    if cls is None:
        raise ValueError(f"不支持的交易所: {cfg.exchange}（支持: binance, bybit）")

    params = {
        "apiKey": cfg.api_key,
        "secret": cfg.api_secret,
        "enableRateLimit": True,
        "options": {"defaultType": "swap"},
    }
    if cfg.testnet:
        params["sandbox"] = True

    return cls(params)


# ─────────────────────────────── 机器人主体 ──────────────────────────────────


class PO3Bot:
    """
    PO3/AMD 剥头皮机器人

    主循环状态机：
        SCANNING_15M  — 每 30s 拉 15m 图，检测 Accumulation/Manipulation
        WATCHING_1M   — 发现 Manipulation 后，每 5s 拉 1m 图等待入场信号
        IN_POSITION   — 持仓中，让 executor 的 trailing_task 管理
    """

    _MODE_SCAN = "scan_15m"
    _MODE_WATCH = "watch_1m"
    _MODE_HOLD = "in_position"

    def __init__(self, cfg: PO3Config, dry_run: bool = False):
        self.cfg = cfg
        self.dry_run = dry_run
        self.exchange: ccxt.Exchange = build_exchange(cfg)
        self.detector = PO3Detector(cfg)
        self.risk = RiskManager(cfg)
        self.tlog = TradeLogger()
        self.executor = PO3Executor(
            self.exchange, cfg, self.risk, self.tlog, dry_run=dry_run
        )
        self._running = False
        self._mode = self._MODE_SCAN
        self._current_manipulation = None  # ManipulationEvent | None

    # ──────────────────── 启动/停止 ────────────────────

    async def start(self) -> None:
        logger.info("=" * 65)
        logger.info("  PO3/AMD 剥头皮机器人 启动")
        logger.info(f"  交易所   : {self.cfg.exchange.upper()} "
                    f"({'测试网' if self.cfg.testnet else '实盘'})")
        logger.info(f"  标的     : {self.cfg.symbol}")
        logger.info(f"  杠杆     : {self.cfg.leverage}x")
        logger.info(f"  每笔风险 : {self.cfg.risk_per_trade*100:.1f}%")
        logger.info(f"  每日上限 : {self.cfg.max_daily_trades} 次 / "
                    f"最大亏损 {self.cfg.max_daily_loss*100:.1f}%")
        logger.info(f"  RR目标   : TP1={self.cfg.tp1_rr} TP2={self.cfg.tp2_rr}")
        logger.info(f"  模式     : {'DRY RUN' if self.dry_run else '实盘'}")
        logger.info("=" * 65)

        # 加载市场
        try:
            await self.exchange.load_markets()
            logger.info(f"市场加载完成，共 {len(self.exchange.markets)} 个")
        except Exception as e:
            logger.error(f"市场加载失败: {e}")
            return

        # 获取初始权益，设置日内起始值
        equity = await self._fetch_equity()
        self.risk.set_daily_start_equity(equity)
        logger.info(f"账户权益: {equity:.2f} USDT")

        self._running = True
        await self._main_loop()

    async def shutdown(self) -> None:
        logger.info("正在安全退出...")
        self._running = False
        await self.executor.emergency_close()
        try:
            await self.exchange.close()
        except Exception:
            pass
        logger.info("机器人已退出")

    # ──────────────────── 主循环 ────────────────────

    async def _main_loop(self) -> None:
        """
        双模式主循环：
        - SCAN 模式：宽间隔(30s)扫描 15m 图寻找 PO3 信号
        - WATCH 模式：窄间隔(5s)盯 1m 图等待入场
        """
        consecutive_errors = 0

        while self._running:
            try:
                if self.executor.is_in_position:
                    # 持仓中：主循环仅做状态打印，实际由 trailing_task 管理
                    await self._print_position_status()
                    await asyncio.sleep(30)
                    consecutive_errors = 0
                    continue

                if self._mode == self._MODE_SCAN:
                    await self._scan_15m()
                    await asyncio.sleep(self.cfg.poll_interval_15m)

                elif self._mode == self._MODE_WATCH:
                    await self._watch_1m()
                    await asyncio.sleep(self.cfg.poll_interval_1m)

                consecutive_errors = 0

            except asyncio.CancelledError:
                break
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"主循环异常 (连续:{consecutive_errors}): {e}",
                             exc_info=True)
                # 指数退避，最多等 120s
                backoff = min(2 ** consecutive_errors, 120)
                logger.info(f"等待 {backoff}s 后重试...")
                await asyncio.sleep(backoff)

    # ──────────────────── 15m 扫描 ────────────────────

    async def _scan_15m(self) -> None:
        """拉取 15m K线，识别 Accumulation → Manipulation"""
        df_15m = await self._fetch_ohlcv("15m", 80)
        if df_15m is None:
            return

        atr = PO3Detector.get_current_atr(df_15m)

        # 1. 检测累积区间
        acc = self.detector.detect_accumulation(df_15m)
        if acc is None:
            logger.debug("[SCAN] 未发现累积区间")
            return

        self.tlog.log_po3_phase(
            "accumulation",
            f"H:{acc.high:.2f} L:{acc.low:.2f} ATR:{acc.atr:.2f}",
            self.cfg.symbol,
        )

        # 2. 检测 Manipulation
        manip = self.detector.detect_manipulation(df_15m, acc)
        if manip is None:
            logger.debug(f"[SCAN] 有累积区间，等待 Manipulation... {acc}")
            return

        self.tlog.log_po3_phase(
            "manipulation",
            str(manip),
            self.cfg.symbol,
        )
        logger.info(f"[SCAN] Manipulation 识别: {manip}")

        # 3. 切换到 1m 候信模式
        self._current_manipulation = manip
        self._mode = self._MODE_WATCH
        logger.info("[SCAN] → 切换 1m 候信模式")

    # ──────────────────── 1m 候信 ────────────────────

    async def _watch_1m(self) -> None:
        """
        盯 1m 图等待入场信号。
        Manipulation 超过 N 根 1m 蜡烛未确认则放弃，回到扫描模式。
        """
        manip = self._current_manipulation
        if manip is None:
            self._mode = self._MODE_SCAN
            return

        # Manipulation 有效窗口：超过 30 分钟未入场则放弃
        age_secs = (datetime.now() - manip.timestamp).total_seconds()
        if age_secs > 30 * 60:
            logger.info("[WATCH] Manipulation 信号超时 30min，重新扫描")
            self._current_manipulation = None
            self._mode = self._MODE_SCAN
            return

        # 风控检查
        equity = await self._fetch_equity()
        can, reason = self.risk.can_trade(equity)
        if not can:
            self.tlog.log_signal_rejected(reason, self.cfg.symbol)
            self._current_manipulation = None
            self._mode = self._MODE_SCAN
            return

        # 拉取 1m 图
        df_1m = await self._fetch_ohlcv("1m", 30)
        if df_1m is None:
            return

        # 检测入场信号
        signal = self.detector.detect_entry_signal(df_1m, manip)
        if signal is None:
            logger.debug("[WATCH] 等待 1m 入场信号...")
            return

        logger.info(f"[WATCH] 入场信号确认: {signal}")
        self.tlog.log_po3_phase("distribution", str(signal), self.cfg.symbol)

        # 执行入场
        atr = PO3Detector.get_current_atr(df_1m)
        success = await self.executor.enter(signal, equity, atr)

        if success:
            logger.info(f"[WATCH] 入场成功，切换持仓监控模式")
        else:
            logger.warning("[WATCH] 入场失败，回到扫描模式")

        self._current_manipulation = None
        self._mode = self._MODE_SCAN

    # ──────────────────── 工具 ────────────────────

    async def _fetch_equity(self) -> float:
        """获取账户 USDT 权益"""
        try:
            balance = await self.exchange.fetch_balance({"type": "swap"})
            usdt = balance.get("USDT", {})
            equity = float(usdt.get("total") or usdt.get("equity") or 0)
            if equity <= 0:
                # Bybit 字段不同
                equity = float(balance.get("total", {}).get("USDT", 0))
            return equity if equity > 0 else 1000.0   # 兜底（dry run）
        except Exception as e:
            logger.warning(f"获取权益失败: {e}，使用默认 1000 USDT")
            return 1000.0

    async def _fetch_ohlcv(self, timeframe: str, limit: int):
        """获取 K 线并转换为 DataFrame"""
        try:
            raw = await self.exchange.fetch_ohlcv(
                self.cfg.symbol, timeframe, limit=limit
            )
            return PO3Detector.candles_to_df(raw)
        except Exception as e:
            logger.warning(f"fetch_ohlcv({timeframe}) 失败: {e}")
            return None

    async def _print_position_status(self) -> None:
        """打印当前持仓状态"""
        pos = self.executor.position
        if pos is None:
            return
        try:
            ticker = await self.exchange.fetch_ticker(self.cfg.symbol)
            price = float(ticker["last"])
            if pos.direction == "long":
                unreal_pnl = (price - pos.entry_price) * pos.contracts_total
            else:
                unreal_pnl = (pos.entry_price - price) * pos.contracts_total
            holding_min = (datetime.now() - pos.opened_at).total_seconds() / 60
            logger.info(
                f"[POS] {pos.direction.upper()} {pos.contracts_total:.4f} @ "
                f"{pos.entry_price:.2f} | 现价:{price:.2f} | "
                f"未实现PnL:{unreal_pnl:+.4f} USDT | "
                f"SL:{pos.stop_loss:.2f} | "
                f"持仓:{holding_min:.1f}min | "
                f"状态:{self.executor.state.value}"
            )
        except Exception:
            pass


# ─────────────────────────────── Scan 模式 ──────────────────────────────────


async def run_scan(cfg: PO3Config) -> None:
    """一次性扫描当前 PO3 阶段，不交易"""
    exchange = build_exchange(cfg)
    detector = PO3Detector(cfg)

    try:
        await exchange.load_markets()
        raw_15m = await exchange.fetch_ohlcv(cfg.symbol, "15m", limit=80)
        df_15m = PO3Detector.candles_to_df(raw_15m)
        atr = PO3Detector.get_current_atr(df_15m)

        print(f"\n{'='*60}")
        print(f"  PO3 阶段扫描  |  {cfg.symbol}  |  {datetime.now().strftime('%H:%M:%S')}")
        print(f"{'='*60}")
        print(f"  当前 ATR(14): {atr:.2f}")

        acc = detector.detect_accumulation(df_15m)
        if acc is None:
            print("  阶段: 无累积区间（价格处于趋势或高波动状态）")
        else:
            print(f"  阶段: ACCUMULATION")
            print(f"  区间: H={acc.high:.2f}  L={acc.low:.2f}  高度={acc.height:.2f}")

            manip = detector.detect_manipulation(df_15m, acc)
            if manip is None:
                print("  等待 Manipulation 假突破...")
            else:
                print(f"  阶段: MANIPULATION → {manip.bias.upper()}")
                print(f"  假突破方向: {manip.direction}  极值: {manip.extreme:.2f}")
                if manip.has_fvg:
                    print(f"  FVG: {manip.fvg_low:.2f} ~ {manip.fvg_high:.2f}")

                raw_1m = await exchange.fetch_ohlcv(cfg.symbol, "1m", limit=30)
                df_1m = PO3Detector.candles_to_df(raw_1m)
                signal = detector.detect_entry_signal(df_1m, manip)
                if signal:
                    print(f"  阶段: DISTRIBUTION → {signal}")
                else:
                    print("  等待 1m 入场信号...")

        print(f"{'='*60}\n")
    finally:
        await exchange.close()


# ─────────────────────────────── 入口 ────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="PO3/AMD 剥头皮机器人")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="不真实下单，仅跑策略逻辑和日志",
    )
    parser.add_argument(
        "--scan", action="store_true",
        help="一次性打印当前 PO3 阶段，不运行机器人",
    )
    args = parser.parse_args()

    cfg = load_config()
    setup_logger(cfg.log_level)

    if args.scan:
        asyncio.run(run_scan(cfg))
        return

    dry_run = args.dry_run
    bot = PO3Bot(cfg, dry_run=dry_run)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _handle_signal():
        logger.info("收到退出信号...")
        for task in asyncio.all_tasks(loop):
            task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    try:
        loop.run_until_complete(bot.start())
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        loop.run_until_complete(bot.shutdown())
        loop.close()


if __name__ == "__main__":
    main()
