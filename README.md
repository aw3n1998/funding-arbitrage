# 加密资金费率套利机器人

支持平台：**币安 Binance** | **欧意 OKX** | **Gate.io** | **Bitget**

## 策略原理

资金费率套利（Funding Rate Arbitrage）是一种市场中性策略：

```
当交易所 A 的资金费率 >> 交易所 B 的资金费率时：

  做空 A（收取高额资金费） + 做多 B（支付低额资金费）
                         ↓
  净收益 = (rate_A - rate_B) × 持仓价值 × 结算次数 - 手续费
```

价格风险被完全对冲，收益来源仅为费率差。

---

## 项目结构

```
funding_arbitrage/
├── main.py                    # 主入口
├── config/
│   ├── settings.py            # 全部配置项
│   └── __init__.py
├── core/
│   ├── exchange_manager.py    # 交易所连接（CCXT）
│   ├── funding_monitor.py     # 实时费率采集
│   ├── arbitrage_detector.py  # 套利机会识别
│   ├── order_executor.py      # 自动下单（含回滚）
│   └── position_manager.py   # 持仓 & PnL 管理
├── utils/
│   └── logger.py              # 日志系统
├── data/                      # 持仓持久化
├── logs/                      # 运行日志
├── .env.example               # 配置模板
└── requirements.txt
```

---

## 快速开始

### 1. 安装依赖

```bash
cd funding_arbitrage
pip install -r requirements.txt
```

### 2. 配置 API

```bash
cp .env.example .env
# 编辑 .env 填入各交易所 API Key
```

各交易所 API 权限要求：
- 读取权限（查询余额、持仓）
- 合约交易权限
- **禁止提币权限**（安全起见）

### 3. 运行

```bash
# 安全模式：仅监控费率，不下单
python main.py --mode scan

# 机器人模式（默认 DRY RUN，不实际下单）
python main.py

# 开启真实交易（.env 中 LIVE_TRADING=true）
LIVE_TRADING=true python main.py
```

---

## 关键配置说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `MIN_ANNUAL_RATE_DIFF` | 5% | 最低年化费率差触发阈值 |
| `MAX_POSITION_USDT` | 500 | 单对最大仓位（USDT） |
| `LEVERAGE` | 1 | 杠杆（建议不加杠杆） |
| `MAX_CONCURRENT_POSITIONS` | 5 | 最大同时持仓对数 |
| `LIVE_TRADING` | false | 真实下单开关 |

---

## 风险提示

1. **执行风险**：两腿下单存在时间差，价格可能不利变动；系统已实现失败自动回滚
2. **流动性风险**：小币种滑点大，建议只用主流币种白名单
3. **资金费率预测风险**：费率可能在结算前反转
4. **交易所风险**：API 限速、维护、爆仓清算均可能影响策略
5. **建议从小仓位开始测试**，熟悉系统后再逐步增加

---

## 运行日志示例

```
2024-01-15 10:00:05 | INFO     | 资金费率套利机器人 启动
2024-01-15 10:00:06 | INFO     | ✓ 币安 Binance 连接成功，共加载 312 个市场
2024-01-15 10:00:07 | INFO     | ✓ 欧意 OKX 连接成功，共加载 485 个市场
2024-01-15 10:00:07 | INFO     | ✓ Gate.io 连接成功，共加载 891 个市场
2024-01-15 10:00:08 | INFO     | ✓ Bitget 连接成功，共加载 267 个市场

╭──────────────────────────────────────────────────────╮
│ 资金费率实时对比  [10:01:00]                          │
├────────────────┬──────────┬──────────┬──────────┬───┤
│ 合约           │ BINANCE  │ OKX      │ GATEIO   │ ... │
├────────────────┼──────────┼──────────┼──────────┼───┤
│ BTC/USDT:USDT  │ +0.12%   │ +0.08%   │ -0.02%   │   │
│ ETH/USDT:USDT  │ +0.15%   │ +0.03%   │ +0.01%   │   │
╰──────────────────────────────────────────────────────╯
```
