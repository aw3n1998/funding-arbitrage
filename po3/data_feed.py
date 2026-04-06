"""
WebSocket 实时数据源（仅支持 Bitget）

使用 ccxt.pro 订阅：
  - BTC/USDT:USDT  15m OHLCV  → 驱动 Accumulation/Manipulation 检测
  - BTC/USDT:USDT   1m OHLCV  → 驱动入场信号检测
  - BTC/USDT:USDT  Ticker     → 提供实时价格（trailing stop 使用）

架构：
  - 所有流在独立 asyncio.Task 中持续运行，自动重连
  - 通过 asyncio.Event 通知调用方 K 线收盘事件
  - get_df_15m() / get_df_1m() 只返回已收盘的 K 线（排除当前未完成蜡烛）
"""
import asyncio
from datetime import datetime
from typing import Optional

import pandas as pd
import ccxt.pro as ccxtpro
from loguru import logger

from po3.detector import PO3Detector


class DataFeed:
    """
    Bitget WebSocket 数据源

    外部使用模式：
        feed = DataFeed(exchange, symbol)
        asyncio.create_task(feed.start())

        # 等待 15m K 线收盘
        await feed.candle_closed_15m.wait()
        feed.candle_closed_15m.clear()
        df = feed.get_df_15m()

        # 实时价格（无需等待事件）
        price = feed.last_price
    """

    # 本地保留最大 K 线数量（防止内存无限增长）
    _MAX_BARS = 300

    def __init__(self, exchange: ccxtpro.Exchange, symbol: str):
        self.exchange = exchange
        self.symbol = symbol

        # 滚动 DataFrame（已收盘 + 当前未收盘的最后一根）
        self._df_15m: pd.DataFrame = pd.DataFrame()
        self._df_1m: pd.DataFrame = pd.DataFrame()

        # 实时价格（Ticker 流更新）
        self.last_price: float = 0.0
        self.last_price_ts: Optional[datetime] = None

        # K 线收盘通知事件
        self.candle_closed_15m: asyncio.Event = asyncio.Event()
        self.candle_closed_1m: asyncio.Event = asyncio.Event()

        # 内部：追踪最新 K 线时间戳（用于检测收盘）
        self._last_ts_15m: Optional[int] = None
        self._last_ts_1m: Optional[int] = None

        self._running: bool = False
        self._tasks: list = []

    # ──────────────────── 生命周期 ────────────────────

    async def start(self) -> None:
        """启动全部 WebSocket 流（先初始化历史数据）"""
        self._running = True
        logger.info(f"[WS] DataFeed 启动 | symbol={self.symbol}")

        # 用 REST 接口预热历史 K 线（保证检测器有足够历史）
        await self._init_history()

        # 启动三个并发 WS 流
        self._tasks = [
            asyncio.create_task(self._stream_ohlcv("15m"), name="ws_15m"),
            asyncio.create_task(self._stream_ohlcv("1m"),  name="ws_1m"),
            asyncio.create_task(self._stream_ticker(),      name="ws_ticker"),
        ]

        # 等待所有流（任一异常不会中断其他流，各自内部重连）
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def stop(self) -> None:
        """停止所有流"""
        self._running = False
        for task in self._tasks:
            if not task.done():
                task.cancel()
        logger.info("[WS] DataFeed 已停止")

    # ──────────────────── 对外接口 ────────────────────

    def get_df_15m(self) -> pd.DataFrame:
        """返回已收盘的 15m K 线（排除最后一根正在形成的蜡烛）"""
        if len(self._df_15m) < 2:
            return self._df_15m
        return self._df_15m.iloc[:-1].copy()

    def get_df_1m(self) -> pd.DataFrame:
        """返回已收盘的 1m K 线（排除最后一根正在形成的蜡烛）"""
        if len(self._df_1m) < 2:
            return self._df_1m
        return self._df_1m.iloc[:-1].copy()

    @property
    def is_ready(self) -> bool:
        """数据是否已初始化（至少有基本历史 K 线）"""
        return len(self._df_15m) >= 20 and len(self._df_1m) >= 20

    # ──────────────────── 内部：历史初始化 ────────────────────

    async def _init_history(self) -> None:
        """用 REST 初始化历史 K 线（WS 连接建立前的冷启动数据）"""
        for timeframe, target in [("15m", "_df_15m"), ("1m", "_df_1m")]:
            for attempt in range(3):
                try:
                    raw = await self.exchange.fetch_ohlcv(
                        self.symbol, timeframe, limit=200
                    )
                    df = PO3Detector.candles_to_df(raw)
                    setattr(self, target, df)
                    # 初始化时间戳（WS 流以此为基准判断收盘）
                    if timeframe == "15m" and len(raw) > 0:
                        self._last_ts_15m = raw[-1][0]
                    elif timeframe == "1m" and len(raw) > 0:
                        self._last_ts_1m = raw[-1][0]
                    logger.info(
                        f"[WS] {timeframe} 历史初始化完成: {len(df)} 根"
                    )
                    break
                except Exception as e:
                    logger.warning(f"[WS] {timeframe} 历史初始化失败 (尝试{attempt+1}/3): {e}")
                    await asyncio.sleep(2 ** attempt)

    # ──────────────────── 内部：WebSocket 流 ────────────────────

    async def _stream_ohlcv(self, timeframe: str) -> None:
        """持续订阅指定时间框架的 OHLCV WebSocket 流，自动重连"""
        reconnect_delay = 1
        while self._running:
            try:
                # watch_ohlcv 每次收到新数据就返回当前 K 线列表
                ohlcv_list = await self.exchange.watch_ohlcv(
                    self.symbol, timeframe, limit=self._MAX_BARS
                )
                if not ohlcv_list:
                    continue

                reconnect_delay = 1  # 重置退避
                new_df = PO3Detector.candles_to_df(ohlcv_list)

                if timeframe == "15m":
                    new_ts = ohlcv_list[-1][0]
                    candle_closed = (
                        self._last_ts_15m is not None
                        and new_ts != self._last_ts_15m
                    )
                    self._df_15m = new_df
                    self._last_ts_15m = new_ts
                    if candle_closed:
                        logger.debug("[WS] 15m K线收盘")
                        self.candle_closed_15m.set()

                elif timeframe == "1m":
                    new_ts = ohlcv_list[-1][0]
                    candle_closed = (
                        self._last_ts_1m is not None
                        and new_ts != self._last_ts_1m
                    )
                    self._df_1m = new_df
                    self._last_ts_1m = new_ts
                    if candle_closed:
                        logger.debug("[WS] 1m K线收盘")
                        self.candle_closed_1m.set()

            except asyncio.CancelledError:
                break
            except ccxtpro.NetworkError as e:
                logger.warning(f"[WS] {timeframe} 网络断开: {e}，{reconnect_delay}s 后重连")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)
            except Exception as e:
                logger.error(f"[WS] {timeframe} 流异常: {e}，{reconnect_delay}s 后重连")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)

    async def _stream_ticker(self) -> None:
        """持续订阅实时价格（Ticker），自动重连"""
        reconnect_delay = 1
        while self._running:
            try:
                ticker = await self.exchange.watch_ticker(self.symbol)
                price = float(ticker.get("last") or ticker.get("close") or 0)
                if price > 0:
                    self.last_price = price
                    self.last_price_ts = datetime.now()
                reconnect_delay = 1
            except asyncio.CancelledError:
                break
            except ccxtpro.NetworkError as e:
                logger.warning(f"[WS] Ticker 网络断开: {e}，{reconnect_delay}s 后重连")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)
            except Exception as e:
                logger.error(f"[WS] Ticker 流异常: {e}，{reconnect_delay}s 后重连")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)
