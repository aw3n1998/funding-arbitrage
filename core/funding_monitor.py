"""
资金费率监控器
实时采集各平台永续合约资金费率
"""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import ccxt.async_support as ccxt
from loguru import logger
from config.settings import AppConfig
from core.exchange_manager import ExchangeManager


@dataclass
class FundingRate:
    """单个交易对的资金费率数据"""
    exchange_id: str
    symbol: str           # 标准化 symbol，如 BTC/USDT:USDT
    funding_rate: float   # 当期费率（每8小时）
    annual_rate: float    # 折算年化（funding_rate * 3 * 365）
    next_funding_time: Optional[datetime]
    mark_price: float
    index_price: float
    timestamp: datetime = field(default_factory=datetime.now)

    @classmethod
    def from_ccxt(cls, exchange_id: str, raw: dict) -> "FundingRate":
        """从 CCXT 原始数据构建"""
        fr = float(raw.get("fundingRate", 0) or 0)
        mark_price = float(raw.get("markPrice", 0) or 0)
        index_price = float(raw.get("indexPrice", 0) or 0)

        next_ts = raw.get("nextFundingDatetime") or raw.get("nextFundingTime")
        if isinstance(next_ts, (int, float)):
            next_dt = datetime.fromtimestamp(next_ts / 1000)
        elif isinstance(next_ts, str):
            try:
                next_dt = datetime.fromisoformat(next_ts.replace("Z", "+00:00"))
            except Exception:
                next_dt = None
        else:
            next_dt = None

        return cls(
            exchange_id=exchange_id,
            symbol=raw.get("symbol", ""),
            funding_rate=fr,
            annual_rate=fr * 3 * 365,  # 三次/天 × 365天
            next_funding_time=next_dt,
            mark_price=mark_price,
            index_price=index_price,
        )

    def __repr__(self) -> str:
        direction = "+" if self.funding_rate >= 0 else ""
        return (
            f"[{self.exchange_id:8s}] {self.symbol:20s} "
            f"费率: {direction}{self.funding_rate*100:.4f}%  "
            f"年化: {direction}{self.annual_rate*100:.2f}%  "
            f"标记价: {self.mark_price:.4f}"
        )


# 按交易所、合约记录的费率表
FundingRateTable = Dict[str, Dict[str, FundingRate]]


class FundingMonitor:
    """
    多交易所资金费率监控器

    工作流程：
    1. 异步并发拉取各交易所费率
    2. 统一存储到内存表
    3. 对外提供查询接口
    """

    def __init__(self, config: AppConfig, exchange_manager: ExchangeManager):
        self.config = config
        self.em = exchange_manager
        self._rates: FundingRateTable = {}  # {exchange_id: {symbol: FundingRate}}
        self._lock = asyncio.Lock()
        self._running = False

    async def start(self) -> None:
        """启动持续监控"""
        self._running = True
        logger.info("资金费率监控器已启动")
        while self._running:
            try:
                await self._refresh_all()
            except Exception as e:
                logger.error(f"费率刷新异常: {e}")
            await asyncio.sleep(self.config.monitor.rate_refresh_interval)

    def stop(self) -> None:
        self._running = False

    async def fetch_once(self) -> FundingRateTable:
        """手动触发一次全量刷新"""
        await self._refresh_all()
        return self._rates

    async def _refresh_all(self) -> None:
        """并发刷新所有交易所费率"""
        tasks = []
        for exchange_id in self.em.list_exchanges():
            tasks.append(self._fetch_exchange_rates(exchange_id))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                logger.warning(f"部分交易所费率获取失败: {r}")

    async def _fetch_exchange_rates(self, exchange_id: str) -> None:
        """拉取单个交易所的所有资金费率"""
        exchange = self.em.get_exchange(exchange_id)
        if not exchange:
            return

        try:
            raw_list = await exchange.fetch_funding_rates()
            # CCXT 返回格式：dict(symbol -> funding_rate_dict)
            if isinstance(raw_list, dict):
                items = raw_list.values()
            else:
                items = raw_list

            rates: Dict[str, FundingRate] = {}
            whitelist = set(self.config.arbitrage.symbol_whitelist)

            for raw in items:
                symbol = raw.get("symbol", "")
                # 仅处理白名单品种（若有）
                if whitelist and symbol not in whitelist:
                    continue
                if not raw.get("fundingRate"):
                    continue
                try:
                    rate = FundingRate.from_ccxt(exchange_id, raw)
                    rates[symbol] = rate
                except Exception as e:
                    logger.debug(f"{exchange_id} 解析 {symbol} 失败: {e}")

            async with self._lock:
                self._rates[exchange_id] = rates

            logger.debug(f"{exchange_id} 费率更新: {len(rates)} 个品种")

        except Exception as e:
            logger.warning(f"{exchange_id} fetch_funding_rates 失败: {e}")

    def get_rate(self, exchange_id: str, symbol: str) -> Optional[FundingRate]:
        """查询指定交易所某合约的费率"""
        return self._rates.get(exchange_id, {}).get(symbol)

    def get_all_rates(self) -> FundingRateTable:
        return self._rates

    def get_symbols_on_exchange(self, exchange_id: str) -> List[str]:
        """获取某交易所已加载费率的所有品种"""
        return list(self._rates.get(exchange_id, {}).keys())

    def get_cross_exchange_rates(self, symbol: str) -> Dict[str, FundingRate]:
        """获取某品种在所有交易所的费率"""
        result = {}
        for exchange_id, rates in self._rates.items():
            if symbol in rates:
                result[exchange_id] = rates[symbol]
        return result

    def get_common_symbols(self) -> List[str]:
        """获取在至少两个交易所都有费率的品种"""
        symbol_count: Dict[str, int] = {}
        for rates in self._rates.values():
            for symbol in rates:
                symbol_count[symbol] = symbol_count.get(symbol, 0) + 1
        return [s for s, c in symbol_count.items() if c >= 2]

    def get_rate_matrix(self) -> List[Tuple[str, Dict[str, Optional[float]]]]:
        """
        返回费率矩阵，按最大年化费率差排序
        [(symbol, {exchange_id: annual_rate, ...}), ...]
        """
        common = self.get_common_symbols()
        result = []

        for symbol in common:
            row = {}
            for exchange_id in self.em.list_exchanges():
                rate = self.get_rate(exchange_id, symbol)
                row[exchange_id] = rate.annual_rate if rate else None

            valid_rates = [r for r in row.values() if r is not None]
            if len(valid_rates) >= 2:
                spread = max(valid_rates) - min(valid_rates)
                result.append((symbol, row, spread))

        result.sort(key=lambda x: x[2], reverse=True)
        return [(s, r) for s, r, _ in result]
