"""
套利机会识别器
分析费率差，找出最优套利组合
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from loguru import logger
from config.settings import AppConfig, ArbitrageConfig
from core.funding_monitor import FundingMonitor, FundingRate


@dataclass
class ArbitrageOpportunity:
    """一个套利机会"""
    symbol: str
    # 做空方（费率高，收取资金费）
    short_exchange: str
    short_rate: FundingRate
    # 做多方（费率低，支付资金费或收取）
    long_exchange: str
    long_rate: FundingRate

    # 年化费率差（净收益估计）
    annual_rate_spread: float
    # 扣除手续费后的净年化
    net_annual_rate: float
    # 建议仓位金额 USDT
    suggested_position_usdt: float
    # 预期每小时收益 USDT
    estimated_hourly_pnl: float

    detected_at: datetime = field(default_factory=datetime.now)

    @property
    def is_profitable(self) -> bool:
        return self.net_annual_rate > 0

    @property
    def direction_str(self) -> str:
        """方向描述"""
        short_sign = "+" if self.short_rate.funding_rate >= 0 else ""
        long_sign = "+" if self.long_rate.funding_rate >= 0 else ""
        return (
            f"SHORT {self.short_exchange}({short_sign}"
            f"{self.short_rate.funding_rate*100:.4f}%) "
            f"<> LONG {self.long_exchange}({long_sign}"
            f"{self.long_rate.funding_rate*100:.4f}%)"
        )

    def summary(self) -> str:
        return (
            f"[套利机会] {self.symbol}\n"
            f"  方向: {self.direction_str}\n"
            f"  年化费率差: {self.annual_rate_spread*100:.2f}%  "
            f"净年化(扣费): {self.net_annual_rate*100:.2f}%\n"
            f"  建议仓位: {self.suggested_position_usdt:.0f} USDT  "
            f"预期时收益: {self.estimated_hourly_pnl:.4f} USDT"
        )


@dataclass
class CloseSignal:
    """平仓信号"""
    symbol: str
    short_exchange: str
    long_exchange: str
    reason: str
    current_spread: float


class ArbitrageDetector:
    """
    套利机会检测器

    策略逻辑:
    - 扫描所有共有品种，找到两个交易所间年化费率差 > 阈值的对
    - 在费率高的一边做空（收费率），费率低的一边做多（支付费率或也收）
    - 净收益 = (short_rate - long_rate) * 持仓价值 - 手续费
    - 当费率差收窄至平仓阈值时，发出平仓信号
    """

    def __init__(self, config: AppConfig, monitor: FundingMonitor):
        self.config = config
        self.arb_cfg: ArbitrageConfig = config.arbitrage
        self.monitor = monitor

    def scan_opportunities(self) -> List[ArbitrageOpportunity]:
        """扫描当前所有套利机会，按净年化排序"""
        opportunities = []

        common_symbols = self.monitor.get_common_symbols()

        for symbol in common_symbols:
            cross_rates = self.monitor.get_cross_exchange_rates(symbol)
            if len(cross_rates) < 2:
                continue

            # 遍历所有交易所对组合
            exchange_ids = list(cross_rates.keys())
            for i in range(len(exchange_ids)):
                for j in range(i + 1, len(exchange_ids)):
                    ex_a = exchange_ids[i]
                    ex_b = exchange_ids[j]
                    rate_a = cross_rates[ex_a]
                    rate_b = cross_rates[ex_b]

                    opp = self._evaluate_pair(symbol, ex_a, rate_a, ex_b, rate_b)
                    if opp and opp.net_annual_rate >= self.arb_cfg.min_annual_rate_diff:
                        opportunities.append(opp)

        # 按净年化降序
        opportunities.sort(key=lambda x: x.net_annual_rate, reverse=True)
        return opportunities

    def _evaluate_pair(
        self,
        symbol: str,
        ex_a: str,
        rate_a: FundingRate,
        ex_b: str,
        rate_b: FundingRate,
    ) -> Optional[ArbitrageOpportunity]:
        """
        评估两个交易所同一品种的套利价值
        做空费率高的，做多费率低的
        """
        if rate_a.funding_rate > rate_b.funding_rate:
            short_ex, short_rate = ex_a, rate_a
            long_ex, long_rate = ex_b, rate_b
        else:
            short_ex, short_rate = ex_b, rate_b
            long_ex, long_rate = ex_a, rate_a

        # 每8小时收益率
        per_period_spread = short_rate.funding_rate - long_rate.funding_rate
        # 年化（3次/天，365天）
        annual_spread = per_period_spread * 3 * 365

        if annual_spread <= 0:
            return None

        # 扣除双边手续费（开仓 + 平仓各一次 = 4次）
        # 年化手续费成本 = 4 * taker_fee / holding_days * 365
        # 假设平均持仓约 1 天
        fee_cost_annual = 4 * self.arb_cfg.taker_fee_rate * 365
        net_annual = annual_spread - fee_cost_annual

        position = min(
            self.arb_cfg.max_position_usdt,
            max(self.arb_cfg.min_position_usdt, self.arb_cfg.max_position_usdt),
        )

        # 预期每小时收益（annual / 365 / 24 * position）
        hourly_pnl = net_annual / 365 / 24 * position

        return ArbitrageOpportunity(
            symbol=symbol,
            short_exchange=short_ex,
            short_rate=short_rate,
            long_exchange=long_ex,
            long_rate=long_rate,
            annual_rate_spread=annual_spread,
            net_annual_rate=net_annual,
            suggested_position_usdt=position,
            estimated_hourly_pnl=hourly_pnl,
        )

    def check_close_signals(
        self,
        active_positions: List[Dict],
    ) -> List[CloseSignal]:
        """
        检查现有持仓是否需要平仓

        active_positions: [
            {
                "symbol": ...,
                "short_exchange": ...,
                "long_exchange": ...,
                "open_spread": ...,   # 开仓时的年化差
            }
        ]
        """
        signals = []
        for pos in active_positions:
            symbol = pos["symbol"]
            short_ex = pos["short_exchange"]
            long_ex = pos["long_exchange"]

            short_rate = self.monitor.get_rate(short_ex, symbol)
            long_rate = self.monitor.get_rate(long_ex, symbol)

            if not short_rate or not long_rate:
                signals.append(CloseSignal(
                    symbol=symbol,
                    short_exchange=short_ex,
                    long_exchange=long_ex,
                    reason="费率数据缺失，风险平仓",
                    current_spread=0.0,
                ))
                continue

            current_spread = (
                short_rate.funding_rate - long_rate.funding_rate
            ) * 3 * 365

            # 费率方向反转 或 费率差低于平仓阈值
            if current_spread <= self.arb_cfg.close_rate_threshold:
                signals.append(CloseSignal(
                    symbol=symbol,
                    short_exchange=short_ex,
                    long_exchange=long_ex,
                    reason=f"费率差收窄至 {current_spread*100:.2f}% (年化)，触发平仓",
                    current_spread=current_spread,
                ))

        return signals

    def get_best_opportunity(self) -> Optional[ArbitrageOpportunity]:
        """获取当前最优套利机会"""
        opps = self.scan_opportunities()
        return opps[0] if opps else None

    def filter_by_exchanges(
        self, opps: List[ArbitrageOpportunity], include_exchanges: List[str]
    ) -> List[ArbitrageOpportunity]:
        """过滤：仅保留指定交易所的机会"""
        ex_set = set(include_exchanges)
        return [
            o for o in opps
            if o.short_exchange in ex_set and o.long_exchange in ex_set
        ]
