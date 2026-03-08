"""
资金费率套利机器人 - 主入口
支持：币安 / 欧意 OKX / Gate.io / Bitget
"""
import asyncio
import signal
import sys
from datetime import datetime
from typing import Optional

from loguru import logger
from tabulate import tabulate

from config import load_config, AppConfig
from core import (
    ExchangeManager,
    FundingMonitor,
    ArbitrageDetector,
    OrderExecutor,
    PositionManager,
)
from utils import setup_logger


class ArbitrageBot:
    """
    套利机器人主控制器

    运行循环:
    1. 费率监控（后台持续）
    2. 持仓 PnL 刷新（后台持续）
    3. 主循环：扫描机会 → 开仓 / 检查平仓信号
    """

    def __init__(self, config: AppConfig):
        self.config = config
        self.em = ExchangeManager(config)
        self.monitor = FundingMonitor(config, self.em)
        self.detector = ArbitrageDetector(config, self.monitor)
        self.executor = OrderExecutor(config, self.em)
        self.pos_manager = PositionManager(config, self.em)
        self._running = False

    async def start(self) -> None:
        """启动机器人"""
        logger.info("=" * 60)
        logger.info("  资金费率套利机器人 启动")
        logger.info(f"  运行模式: {'实盘交易' if self.config.live_trading else '仅监控 (DRY RUN)'}")
        logger.info(f"  最低年化差阈值: {self.config.arbitrage.min_annual_rate_diff*100:.1f}%")
        logger.info(f"  单次最大仓位: {self.config.arbitrage.max_position_usdt} USDT")
        logger.info(f"  最大并发套利对: {self.config.arbitrage.max_concurrent_positions}")
        logger.info("=" * 60)

        # 初始化交易所连接
        await self.em.initialize()

        if not self.em.list_exchanges():
            logger.warning("未配置交易所 API，将使用公开端点获取费率（不可交易）")

        # 首次全量拉取费率
        logger.info("正在获取初始费率数据...")
        await self.monitor.fetch_once()

        self._running = True

        # 启动后台任务
        tasks = [
            asyncio.create_task(self.monitor.start(), name="monitor"),
            asyncio.create_task(self.pos_manager.start_refresh_loop(), name="pnl_refresh"),
            asyncio.create_task(self._main_loop(), name="main_loop"),
            asyncio.create_task(self._display_loop(), name="display"),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        self._running = False
        self.monitor.stop()
        self.pos_manager.stop()
        await self.em.close()
        logger.info("机器人已安全退出")

    # ──────────────── 主循环 ────────────────

    async def _main_loop(self) -> None:
        """核心决策循环"""
        # 等待首次费率数据就绪
        await asyncio.sleep(5)

        while self._running:
            try:
                await self._run_strategy_once()
            except Exception as e:
                logger.error(f"策略循环异常: {e}", exc_info=True)
            await asyncio.sleep(self.config.monitor.rate_refresh_interval)

    async def _run_strategy_once(self) -> None:
        """执行一次套利决策"""

        # ── 1. 检查是否需要平仓 ──
        active_pairs = self.pos_manager.get_active_pairs()
        if active_pairs:
            close_signals = self.detector.check_close_signals(active_pairs)
            for signal in close_signals:
                # 找到对应持仓
                pos = next(
                    (p for p in self.pos_manager.get_open_positions()
                     if p.symbol == signal.symbol
                     and p.short_exchange == signal.short_exchange
                     and p.long_exchange == signal.long_exchange),
                    None,
                )
                if pos:
                    logger.warning(f"触发平仓信号: {signal.reason}")
                    result = await self.executor.close_position(
                        signal,
                        short_amount=pos.short_amount,
                        long_amount=pos.long_amount,
                    )
                    if result.success:
                        await self.pos_manager.close_position(
                            pos.position_id, signal, result
                        )

        # ── 2. 扫描新开仓机会 ──
        if not self.pos_manager.can_open_new():
            logger.debug("已达最大持仓数，跳过开仓扫描")
            return

        open_symbols = set(self.pos_manager.get_open_symbols())
        opportunities = self.detector.scan_opportunities()

        for opp in opportunities:
            # 跳过已有同品种持仓
            if opp.symbol in open_symbols:
                continue
            if not self.pos_manager.can_open_new():
                break

            logger.info(opp.summary())

            result = await self.executor.open_position(opp)
            if result.success:
                await self.pos_manager.add_position(opp, result)
                open_symbols.add(opp.symbol)

    # ──────────────── 展示循环 ────────────────

    async def _display_loop(self) -> None:
        """定期打印费率表和持仓状态"""
        await asyncio.sleep(15)
        while self._running:
            try:
                self._print_rate_table()
                self._print_position_summary()
            except Exception as e:
                logger.debug(f"展示异常: {e}")
            await asyncio.sleep(60)  # 每分钟刷新一次显示

    def _print_rate_table(self) -> None:
        """打印费率对比表"""
        matrix = self.monitor.get_rate_matrix()
        if not matrix:
            return

        exchanges = self.em.list_exchanges()
        headers = ["合约"] + [ex.upper() for ex in exchanges] + ["最大年化差"]
        rows = []

        for symbol, rate_row in matrix[:15]:  # 只显示前15个
            row = [symbol]
            rates = []
            for ex in exchanges:
                r = rate_row.get(ex)
                if r is not None:
                    row.append(f"{r*100:+.2f}%")
                    rates.append(r)
                else:
                    row.append("-")
            spread = max(rates) - min(rates) if len(rates) >= 2 else 0
            row.append(f"{spread*100:.2f}%")
            rows.append(row)

        print(f"\n{'='*80}")
        print(f"  资金费率实时对比  [{datetime.now().strftime('%H:%M:%S')}]")
        print(tabulate(rows, headers=headers, tablefmt="rounded_outline"))

    def _print_position_summary(self) -> None:
        """打印持仓汇总"""
        summary = self.pos_manager.total_pnl_summary()
        open_pos = self.pos_manager.get_open_positions()

        print(f"\n{'─'*80}")
        print(
            f"  持仓汇总 | 活跃: {summary['open_count']} | "
            f"总名义仓位: {summary['total_position_usdt']:.0f} USDT | "
            f"未实现PnL: {summary['unrealized_pnl']:+.4f} USDT | "
            f"累计资金费: {summary['unrealized_funding']:+.4f} USDT | "
            f"已实现: {summary['realized_pnl']:+.4f} USDT"
        )

        if open_pos:
            print()
            for pos in open_pos:
                print(f"  {pos.summary_line()}")


# ──────────────── 命令行工具 ────────────────

async def run_monitor_only(config: AppConfig) -> None:
    """仅监控模式：只打印费率表，不交易"""
    em = ExchangeManager(config)
    await em.initialize()

    monitor = FundingMonitor(config, em)
    logger.info("获取费率数据中...")
    await monitor.fetch_once()

    matrix = monitor.get_rate_matrix()
    exchanges = em.list_exchanges()
    headers = ["合约"] + [ex.upper() for ex in exchanges] + ["年化差"]
    rows = []

    for symbol, rate_row in matrix:
        row = [symbol]
        rates = []
        for ex in exchanges:
            r = rate_row.get(ex)
            if r is not None:
                row.append(f"{r*100:+.3f}%")
                rates.append(r)
            else:
                row.append("-")
        spread = max(rates) - min(rates) if len(rates) >= 2 else 0
        row.append(f"{spread*100:.2f}%")
        rows.append(row)

    print("\n" + tabulate(rows, headers=headers, tablefmt="rounded_outline"))
    await em.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="资金费率套利机器人")
    parser.add_argument(
        "--mode",
        choices=["bot", "scan"],
        default="bot",
        help="bot=运行套利机器人, scan=一次性扫描费率",
    )
    args = parser.parse_args()

    config = load_config()
    setup_logger(config.log_level)

    if args.mode == "scan":
        asyncio.run(run_monitor_only(config))
    else:
        bot = ArbitrageBot(config)

        # 注册优雅退出信号
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        def _handle_signal():
            logger.info("收到退出信号，正在安全停止...")
            for task in asyncio.all_tasks(loop):
                task.cancel()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _handle_signal)

        try:
            loop.run_until_complete(bot.start())
        except KeyboardInterrupt:
            pass
        finally:
            loop.close()


if __name__ == "__main__":
    main()
