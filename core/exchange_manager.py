"""
交易所连接管理器
统一管理四个交易所的 CCXT 实例
"""
import asyncio
from typing import Dict, Optional
import ccxt.async_support as ccxt
from loguru import logger
from config.settings import AppConfig, ExchangeConfig


# 各交易所的 CCXT 类名映射
EXCHANGE_CLASS_MAP = {
    "binance": ccxt.binance,
    "okx": ccxt.okx,
    "gateio": ccxt.gateio,
    "bitget": ccxt.bitget,
}

# 交易所显示名称
EXCHANGE_DISPLAY_NAMES = {
    "binance": "币安 Binance",
    "okx": "欧意 OKX",
    "gateio": "Gate.io",
    "bitget": "Bitget",
}


class ExchangeManager:
    """管理多个交易所的连接与基础操作"""

    def __init__(self, config: AppConfig):
        self.config = config
        self.exchanges: Dict[str, ccxt.Exchange] = {}
        self._initialized = False

    async def initialize(self) -> None:
        """初始化所有交易所连接"""
        if not self.config.exchanges:
            logger.warning("未配置任何交易所 API，运行在公开数据模式")

        init_tasks = []
        for exchange_id, exchange_config in self.config.exchanges.items():
            init_tasks.append(self._init_exchange(exchange_id, exchange_config))

        await asyncio.gather(*init_tasks, return_exceptions=True)
        self._initialized = True
        logger.info(f"已连接交易所: {list(self.exchanges.keys())}")

    async def _init_exchange(self, exchange_id: str, cfg: ExchangeConfig) -> None:
        """初始化单个交易所"""
        ccxt_id = cfg.name  # binance / okx / gateio / bitget
        exchange_class = EXCHANGE_CLASS_MAP.get(ccxt_id)

        if exchange_class is None:
            logger.error(f"不支持的交易所: {ccxt_id}")
            return

        params: Dict = {
            "apiKey": cfg.api_key,
            "secret": cfg.api_secret,
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},  # 默认永续合约
        }
        if cfg.passphrase:
            params["password"] = cfg.passphrase

        if cfg.sandbox:
            params["sandbox"] = True

        try:
            exchange = exchange_class(params)
            await exchange.load_markets()
            self.exchanges[exchange_id] = exchange
            display = EXCHANGE_DISPLAY_NAMES.get(ccxt_id, ccxt_id)
            logger.info(f"✓ {display} 连接成功，共加载 {len(exchange.markets)} 个市场")
        except Exception as e:
            logger.error(f"✗ {ccxt_id} 连接失败: {e}")

    async def close(self) -> None:
        """关闭所有连接"""
        for exchange_id, exchange in self.exchanges.items():
            try:
                await exchange.close()
                logger.debug(f"{exchange_id} 连接已关闭")
            except Exception as e:
                logger.warning(f"关闭 {exchange_id} 时出错: {e}")
        self.exchanges.clear()

    def get_exchange(self, exchange_id: str) -> Optional[ccxt.Exchange]:
        return self.exchanges.get(exchange_id)

    def list_exchanges(self) -> list:
        return list(self.exchanges.keys())

    async def fetch_ticker(self, exchange_id: str, symbol: str) -> Optional[dict]:
        """获取实时报价"""
        exchange = self.get_exchange(exchange_id)
        if not exchange:
            return None
        try:
            return await exchange.fetch_ticker(symbol)
        except Exception as e:
            logger.debug(f"{exchange_id} fetch_ticker({symbol}) 失败: {e}")
            return None

    async def fetch_balance(self, exchange_id: str) -> Optional[dict]:
        """获取账户余额"""
        exchange = self.get_exchange(exchange_id)
        if not exchange:
            return None
        try:
            return await exchange.fetch_balance({"type": "swap"})
        except Exception as e:
            logger.error(f"{exchange_id} fetch_balance 失败: {e}")
            return None

    async def set_leverage(self, exchange_id: str, symbol: str, leverage: int) -> bool:
        """设置杠杆"""
        exchange = self.get_exchange(exchange_id)
        if not exchange:
            return False
        try:
            await exchange.set_leverage(leverage, symbol)
            return True
        except Exception as e:
            logger.warning(f"{exchange_id} set_leverage({symbol}, {leverage}) 失败: {e}")
            return False

    async def create_order(
        self,
        exchange_id: str,
        symbol: str,
        order_type: str,
        side: str,
        amount: float,
        price: Optional[float] = None,
        params: Optional[dict] = None,
    ) -> Optional[dict]:
        """统一下单接口"""
        exchange = self.get_exchange(exchange_id)
        if not exchange:
            logger.error(f"交易所 {exchange_id} 未初始化")
            return None

        params = params or {}
        # Bitget/Gate 永续合约需要指定 marginMode
        if exchange_id == "bitget":
            params.setdefault("marginMode", "isolated")
        if exchange_id == "gateio":
            params.setdefault("settle", "usdt")

        try:
            order = await exchange.create_order(
                symbol, order_type, side, amount, price, params
            )
            logger.info(
                f"[{exchange_id}] 下单成功: {side} {amount} {symbol} "
                f"@ {price or 'market'} | 订单ID: {order.get('id')}"
            )
            return order
        except Exception as e:
            logger.error(f"[{exchange_id}] 下单失败 {side} {symbol}: {e}")
            return None

    async def fetch_positions(self, exchange_id: str) -> list:
        """获取当前持仓"""
        exchange = self.get_exchange(exchange_id)
        if not exchange:
            return []
        try:
            positions = await exchange.fetch_positions()
            return [p for p in positions if abs(float(p.get("contracts", 0) or 0)) > 0]
        except Exception as e:
            logger.error(f"{exchange_id} fetch_positions 失败: {e}")
            return []

    async def cancel_order(self, exchange_id: str, order_id: str, symbol: str) -> bool:
        """取消订单"""
        exchange = self.get_exchange(exchange_id)
        if not exchange:
            return False
        try:
            await exchange.cancel_order(order_id, symbol)
            return True
        except Exception as e:
            logger.warning(f"{exchange_id} cancel_order({order_id}) 失败: {e}")
            return False
