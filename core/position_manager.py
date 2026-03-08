"""
持仓与盈亏管理
追踪所有套利持仓，计算实时 PnL
"""
import json
import asyncio
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from loguru import logger
from config.settings import AppConfig
from core.exchange_manager import ExchangeManager
from core.arbitrage_detector import ArbitrageOpportunity, CloseSignal
from core.order_executor import ExecutionResult


@dataclass
class ArbitragePosition:
    """一个活跃的套利持仓"""
    position_id: str
    symbol: str
    short_exchange: str
    long_exchange: str

    # 开仓信息
    open_time: datetime
    open_short_price: float
    open_long_price: float
    short_amount: float     # 合约数量
    long_amount: float
    position_usdt: float    # 名义仓位

    # 开仓时费率差（年化）
    open_annual_spread: float

    # 实时 PnL（由 refresh 更新）
    current_short_price: float = 0.0
    current_long_price: float = 0.0
    unrealized_pnl: float = 0.0     # 价格波动 PnL（对冲后接近0）
    funding_received: float = 0.0   # 累计收取的资金费
    total_pnl: float = 0.0
    last_refresh: Optional[datetime] = None

    # 状态
    is_open: bool = True
    close_time: Optional[datetime] = None
    close_reason: Optional[str] = None
    realized_pnl: float = 0.0

    def to_dict(self) -> dict:
        d = asdict(self)
        # datetime 序列化
        for k, v in d.items():
            if isinstance(v, datetime):
                d[k] = v.isoformat()
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "ArbitragePosition":
        for k in ("open_time", "last_refresh", "close_time"):
            if d.get(k):
                d[k] = datetime.fromisoformat(d[k])
        return cls(**d)

    def holding_hours(self) -> float:
        end = self.close_time or datetime.now()
        return (end - self.open_time).total_seconds() / 3600

    def summary_line(self) -> str:
        status = "持仓中" if self.is_open else "已平仓"
        return (
            f"{self.position_id:25s} | {self.symbol:20s} | "
            f"SHORT {self.short_exchange:8s} LONG {self.long_exchange:8s} | "
            f"{status:5s} | 持仓: {self.holding_hours():.1f}h | "
            f"总PnL: {self.total_pnl:+.4f} USDT"
        )


class PositionManager:
    """
    持仓管理器

    职责:
    1. 记录并持久化所有套利持仓
    2. 定期从交易所同步实际价格，计算 PnL
    3. 管理平仓操作
    """

    SAVE_PATH = "data/positions.json"

    def __init__(self, config: AppConfig, exchange_manager: ExchangeManager):
        self.config = config
        self.em = exchange_manager
        self.positions: Dict[str, ArbitragePosition] = {}
        self._lock = asyncio.Lock()
        self._running = False
        Path("data").mkdir(exist_ok=True)
        self._load_from_disk()

    # ──────────────── 持仓增删 ────────────────

    async def add_position(
        self,
        opportunity: ArbitrageOpportunity,
        result: ExecutionResult,
    ) -> Optional[ArbitragePosition]:
        """套利开仓成功后登记持仓"""
        if not result.success or not result.short_order or not result.long_order:
            return None

        pos = ArbitragePosition(
            position_id=result.opportunity_id,
            symbol=opportunity.symbol,
            short_exchange=opportunity.short_exchange,
            long_exchange=opportunity.long_exchange,
            open_time=datetime.now(),
            open_short_price=result.short_order.filled_price or opportunity.short_rate.mark_price,
            open_long_price=result.long_order.filled_price or opportunity.long_rate.mark_price,
            short_amount=result.short_order.amount,
            long_amount=result.long_order.amount,
            position_usdt=result.position_usdt,
            open_annual_spread=opportunity.annual_rate_spread,
        )

        async with self._lock:
            self.positions[pos.position_id] = pos

        self._save_to_disk()
        logger.info(f"持仓已登记: {pos.position_id} | {pos.symbol}")
        return pos

    async def close_position(
        self,
        position_id: str,
        signal: CloseSignal,
        result: ExecutionResult,
    ) -> None:
        """平仓后更新持仓状态"""
        async with self._lock:
            pos = self.positions.get(position_id)
            if not pos:
                return

            pos.is_open = False
            pos.close_time = datetime.now()
            pos.close_reason = signal.reason
            pos.realized_pnl = pos.total_pnl

        self._save_to_disk()
        logger.info(
            f"持仓已关闭: {position_id} | 原因: {signal.reason} | "
            f"实现PnL: {pos.realized_pnl:+.4f} USDT"
        )

    # ──────────────── PnL 刷新 ────────────────

    async def start_refresh_loop(self) -> None:
        """后台持续刷新持仓 PnL"""
        self._running = True
        while self._running:
            try:
                await self._refresh_all_pnl()
            except Exception as e:
                logger.error(f"PnL 刷新异常: {e}")
            await asyncio.sleep(self.config.monitor.position_refresh_interval)

    def stop(self) -> None:
        self._running = False

    async def _refresh_all_pnl(self) -> None:
        open_positions = [p for p in self.positions.values() if p.is_open]
        if not open_positions:
            return

        tasks = [self._refresh_position_pnl(p) for p in open_positions]
        await asyncio.gather(*tasks, return_exceptions=True)
        self._save_to_disk()

    async def _refresh_position_pnl(self, pos: ArbitragePosition) -> None:
        """刷新单个持仓的价格和 PnL"""
        short_ticker, long_ticker = await asyncio.gather(
            self.em.fetch_ticker(pos.short_exchange, pos.symbol),
            self.em.fetch_ticker(pos.long_exchange, pos.symbol),
            return_exceptions=True,
        )

        if isinstance(short_ticker, Exception) or isinstance(long_ticker, Exception):
            return
        if not short_ticker or not long_ticker:
            return

        short_price = float(short_ticker.get("last", 0) or 0)
        long_price = float(long_ticker.get("last", 0) or 0)

        if short_price <= 0 or long_price <= 0:
            return

        # 价格 PnL（多空对冲，理论上接近 0）
        short_pnl = (pos.open_short_price - short_price) * pos.short_amount
        long_pnl = (long_price - pos.open_long_price) * pos.long_amount
        unrealized = short_pnl + long_pnl

        # 资金费收入估算（基于持仓时长和开仓时费率）
        holding_hours = pos.holding_hours()
        funding_periods = holding_hours / 8  # 每8小时结算一次
        estimated_funding = (
            pos.open_annual_spread / 365 / 3 * funding_periods * pos.position_usdt
        )

        async with self._lock:
            pos.current_short_price = short_price
            pos.current_long_price = long_price
            pos.unrealized_pnl = unrealized
            pos.funding_received = estimated_funding
            pos.total_pnl = unrealized + estimated_funding
            pos.last_refresh = datetime.now()

    # ──────────────── 查询 ────────────────

    def get_open_positions(self) -> List[ArbitragePosition]:
        return [p for p in self.positions.values() if p.is_open]

    def get_closed_positions(self) -> List[ArbitragePosition]:
        return [p for p in self.positions.values() if not p.is_open]

    def get_position_count(self) -> int:
        return len(self.get_open_positions())

    def can_open_new(self) -> bool:
        return self.get_position_count() < self.config.arbitrage.max_concurrent_positions

    def get_open_symbols(self) -> List[str]:
        return [p.symbol for p in self.get_open_positions()]

    def get_active_pairs(self) -> List[Dict]:
        """返回用于检测平仓信号的持仓摘要"""
        result = []
        for p in self.get_open_positions():
            result.append({
                "position_id": p.position_id,
                "symbol": p.symbol,
                "short_exchange": p.short_exchange,
                "long_exchange": p.long_exchange,
                "open_spread": p.open_annual_spread,
            })
        return result

    def total_pnl_summary(self) -> Dict:
        """汇总 PnL"""
        open_pos = self.get_open_positions()
        closed_pos = self.get_closed_positions()

        return {
            "open_count": len(open_pos),
            "closed_count": len(closed_pos),
            "unrealized_pnl": sum(p.unrealized_pnl for p in open_pos),
            "unrealized_funding": sum(p.funding_received for p in open_pos),
            "realized_pnl": sum(p.realized_pnl for p in closed_pos),
            "total_position_usdt": sum(p.position_usdt for p in open_pos),
        }

    # ──────────────── 持久化 ────────────────

    def _save_to_disk(self) -> None:
        try:
            data = {pid: pos.to_dict() for pid, pos in self.positions.items()}
            with open(self.SAVE_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"持仓保存失败: {e}")

    def _load_from_disk(self) -> None:
        path = Path(self.SAVE_PATH)
        if not path.exists():
            return
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            for pid, d in data.items():
                self.positions[pid] = ArbitragePosition.from_dict(d)
            logger.info(f"从磁盘恢复 {len(self.positions)} 条持仓记录")
        except Exception as e:
            logger.warning(f"持仓记录加载失败: {e}")
