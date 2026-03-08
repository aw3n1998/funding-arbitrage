"""
自动下单执行器
负责在两个交易所同时开仓/平仓，处理失败回滚
"""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from loguru import logger
from config.settings import AppConfig
from core.exchange_manager import ExchangeManager
from core.arbitrage_detector import ArbitrageOpportunity, CloseSignal


@dataclass
class OrderResult:
    """单笔订单结果"""
    exchange_id: str
    symbol: str
    side: str           # "buy" | "sell"
    amount: float
    price: Optional[float]
    order_id: Optional[str]
    success: bool
    error: Optional[str] = None
    filled_price: Optional[float] = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class ExecutionResult:
    """一次套利开仓/平仓结果"""
    opportunity_id: str   # 唯一标识
    symbol: str
    short_exchange: str
    long_exchange: str
    short_order: Optional[OrderResult]
    long_order: Optional[OrderResult]
    success: bool         # 两腿均成功
    partial: bool = False # 一腿成功一腿失败（需要回滚）
    position_usdt: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)

    def summary(self) -> str:
        status = "✓ 成功" if self.success else ("△ 部分成功" if self.partial else "✗ 失败")
        lines = [f"[执行结果] {self.symbol} {status}"]
        if self.short_order:
            s = self.short_order
            lines.append(
                f"  做空 [{s.exchange_id}]: {'成功' if s.success else '失败'} "
                f"| 数量: {s.amount} | 成交价: {s.filled_price or '-'}"
            )
        if self.long_order:
            l = self.long_order
            lines.append(
                f"  做多 [{l.exchange_id}]: {'成功' if l.success else '失败'} "
                f"| 数量: {l.amount} | 成交价: {l.filled_price or '-'}"
            )
        return "\n".join(lines)


class OrderExecutor:
    """
    套利订单执行器

    执行策略:
    1. 同时向两个交易所提交市价单
    2. 任一腿失败，立即对成功腿进行反向平仓（回滚）
    3. 支持 dry_run 模式（仅记录，不实际下单）
    """

    def __init__(self, config: AppConfig, exchange_manager: ExchangeManager):
        self.config = config
        self.em = exchange_manager
        self.dry_run = not config.live_trading
        self._order_counter = 0

    def _next_id(self) -> str:
        self._order_counter += 1
        return f"arb_{datetime.now().strftime('%Y%m%d%H%M%S')}_{self._order_counter:04d}"

    async def open_position(
        self, opportunity: ArbitrageOpportunity
    ) -> ExecutionResult:
        """
        开仓：
        - 在 short_exchange 做空
        - 在 long_exchange 做多
        """
        opp_id = self._next_id()
        symbol = opportunity.symbol
        position_usdt = opportunity.suggested_position_usdt

        # 获取标记价格计算数量
        short_price = opportunity.short_rate.mark_price
        long_price = opportunity.long_rate.mark_price

        if short_price <= 0 or long_price <= 0:
            logger.error(f"价格无效: short={short_price}, long={long_price}")
            return ExecutionResult(
                opportunity_id=opp_id, symbol=symbol,
                short_exchange=opportunity.short_exchange,
                long_exchange=opportunity.long_exchange,
                short_order=None, long_order=None, success=False,
            )

        short_amount = round(position_usdt / short_price, 4)
        long_amount = round(position_usdt / long_price, 4)

        logger.info(
            f"[{opp_id}] 准备开仓: {symbol}\n"
            f"  做空 [{opportunity.short_exchange}]: {short_amount} 合约 @ ~{short_price:.4f}\n"
            f"  做多 [{opportunity.long_exchange}]: {long_amount} 合约 @ ~{long_price:.4f}"
        )

        if self.dry_run:
            logger.warning(f"[DRY RUN] 跳过实际下单，开仓已模拟")
            short_result = OrderResult(
                exchange_id=opportunity.short_exchange, symbol=symbol,
                side="sell", amount=short_amount, price=short_price,
                order_id=f"dry_short_{opp_id}", success=True,
                filled_price=short_price,
            )
            long_result = OrderResult(
                exchange_id=opportunity.long_exchange, symbol=symbol,
                side="buy", amount=long_amount, price=long_price,
                order_id=f"dry_long_{opp_id}", success=True,
                filled_price=long_price,
            )
            result = ExecutionResult(
                opportunity_id=opp_id, symbol=symbol,
                short_exchange=opportunity.short_exchange,
                long_exchange=opportunity.long_exchange,
                short_order=short_result, long_order=long_result,
                success=True, position_usdt=position_usdt,
            )
            logger.info(result.summary())
            return result

        # 真实下单：设置杠杆后并发下单
        await asyncio.gather(
            self.em.set_leverage(
                opportunity.short_exchange, symbol, self.config.arbitrage.leverage),
            self.em.set_leverage(
                opportunity.long_exchange, symbol, self.config.arbitrage.leverage),
        )

        short_task = self._place_order(
            opportunity.short_exchange, symbol, "sell", short_amount
        )
        long_task = self._place_order(
            opportunity.long_exchange, symbol, "buy", long_amount
        )

        short_result, long_result = await asyncio.gather(short_task, long_task)

        success = short_result.success and long_result.success
        partial = short_result.success != long_result.success

        if partial:
            logger.error(f"[{opp_id}] 一腿失败，开始回滚...")
            await self._rollback(short_result, long_result, symbol)

        result = ExecutionResult(
            opportunity_id=opp_id, symbol=symbol,
            short_exchange=opportunity.short_exchange,
            long_exchange=opportunity.long_exchange,
            short_order=short_result, long_order=long_result,
            success=success, partial=partial, position_usdt=position_usdt,
        )
        logger.info(result.summary())
        return result

    async def close_position(
        self,
        signal: CloseSignal,
        short_amount: float,
        long_amount: float,
    ) -> ExecutionResult:
        """
        平仓：
        - 在 short_exchange 买回（平空）
        - 在 long_exchange 卖出（平多）
        """
        opp_id = self._next_id()
        symbol = signal.symbol

        logger.info(
            f"[{opp_id}] 准备平仓: {symbol}\n"
            f"  原因: {signal.reason}\n"
            f"  平空 [{signal.short_exchange}]: {short_amount}\n"
            f"  平多 [{signal.long_exchange}]: {long_amount}"
        )

        if self.dry_run:
            logger.warning(f"[DRY RUN] 跳过实际平仓")
            short_result = OrderResult(
                exchange_id=signal.short_exchange, symbol=symbol,
                side="buy", amount=short_amount, price=None,
                order_id=f"dry_close_short_{opp_id}", success=True,
            )
            long_result = OrderResult(
                exchange_id=signal.long_exchange, symbol=symbol,
                side="sell", amount=long_amount, price=None,
                order_id=f"dry_close_long_{opp_id}", success=True,
            )
            return ExecutionResult(
                opportunity_id=opp_id, symbol=symbol,
                short_exchange=signal.short_exchange,
                long_exchange=signal.long_exchange,
                short_order=short_result, long_order=long_result,
                success=True,
            )

        # 获取平仓特定参数
        short_params = self._close_params(signal.short_exchange)
        long_params = self._close_params(signal.long_exchange)

        short_task = self._place_order(
            signal.short_exchange, symbol, "buy", short_amount, params=short_params
        )
        long_task = self._place_order(
            signal.long_exchange, symbol, "sell", long_amount, params=long_params
        )

        short_result, long_result = await asyncio.gather(short_task, long_task)
        success = short_result.success and long_result.success

        result = ExecutionResult(
            opportunity_id=opp_id, symbol=symbol,
            short_exchange=signal.short_exchange,
            long_exchange=signal.long_exchange,
            short_order=short_result, long_order=long_result,
            success=success,
        )
        logger.info(result.summary())
        return result

    async def _place_order(
        self,
        exchange_id: str,
        symbol: str,
        side: str,
        amount: float,
        price: Optional[float] = None,
        params: Optional[Dict] = None,
    ) -> OrderResult:
        """执行单腿下单"""
        params = params or {}
        order = await self.em.create_order(
            exchange_id, symbol, "market", side, amount, price, params
        )

        if order:
            return OrderResult(
                exchange_id=exchange_id, symbol=symbol,
                side=side, amount=amount, price=price,
                order_id=order.get("id"),
                success=True,
                filled_price=order.get("average") or order.get("price"),
            )
        else:
            return OrderResult(
                exchange_id=exchange_id, symbol=symbol,
                side=side, amount=amount, price=price,
                order_id=None, success=False,
                error="下单返回空结果",
            )

    async def _rollback(
        self,
        short_result: OrderResult,
        long_result: OrderResult,
        symbol: str,
    ) -> None:
        """回滚成功的那一腿"""
        if short_result.success:
            logger.warning(
                f"回滚做空腿: {short_result.exchange_id} {symbol} "
                f"数量: {short_result.amount}"
            )
            await self._place_order(
                short_result.exchange_id, symbol, "buy", short_result.amount,
                params=self._close_params(short_result.exchange_id),
            )

        if long_result.success:
            logger.warning(
                f"回滚做多腿: {long_result.exchange_id} {symbol} "
                f"数量: {long_result.amount}"
            )
            await self._place_order(
                long_result.exchange_id, symbol, "sell", long_result.amount,
                params=self._close_params(long_result.exchange_id),
            )

    @staticmethod
    def _close_params(exchange_id: str) -> Dict:
        """各交易所平仓特定参数"""
        params_map = {
            "binance": {"reduceOnly": True},
            "okx": {"reduceOnly": True},
            "gateio": {"reduceOnly": True},
            "bitget": {"reduceOnly": True},
        }
        return params_map.get(exchange_id, {})
