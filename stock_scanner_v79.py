#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
stock_scanner_v791_final.py
A股强势股扫描器 - 最终修复版
整合所有审查意见（含 DeepSeek 反馈）
"""

import os
import re
import pickle
import random
import time
import warnings
import traceback
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

warnings.filterwarnings('ignore')

# ================== 配置常量 ==================
EPSILON = 1e-9
MIN_HIST_DAYS = 30
MAX_RETRY = 3
CACHE_DIR = Path("./cache_v791")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_TTL_HOURS = 4  # 缓存有效期（小时）

CONFIG = {
    'max_drop': 0.08,
    'atr_mult_high': 2.5,
    'atr_mult_mid': 2.0,
    'atr_mult_low': 1.5,
    'rs_thresholds': [1.2, 1.1, 1.0, 0.9],
    'rs_scores': [15, 10, 5, 0, -5],
    'min_turnover': 0.8,
    'obv_threshold': 1.01,
    'vol_mult_breakout': 1.5,
    'rsi_period': 14,
    'kdj_n': 9,
    'kdj_m1': 3,
    'kdj_m2': 3,
    'adx_period': 14,
}


# ================== 工具函数 ==================

def normalize_code(code) -> str:
    """提取纯数字代码"""
    return re.sub(r'\D', '', str(code).strip().upper()).zfill(6)


def get_baostock_symbol(code: str) -> str:
    """转为 baostock 格式"""
    code = str(code).zfill(6)
    if code.startswith(('6', '9')):
        return f"sh.{code}"
    elif code.startswith(('0', '2', '3')):
        return f"sz.{code}"
    elif code.startswith(('4', '8')):
        return f"bj.{code}"
    return f"sz.{code}"


def clean_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """统一列名 + 安全数值转换"""
    if df is None or df.empty:
        return df

    df = df.copy()

    price_map = {
        '日期': 'date', '开盘': 'open', '开盘价': 'open',
        '最高': 'high', '最高价': 'high',
        '最低': 'low', '最低价': 'low',
        '收盘': 'close', '收盘价': 'close',
        '成交量': 'volume', '成交额': 'amount',
        '换手率': 'turn', '换手': 'turn',
    }
    for cn, en in price_map.items():
        if cn in df.columns and en not in df.columns:
            df[en] = df[cn]

    required_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
    for col in required_cols:
        if col not in df.columns:
            df[col] = 0.0

    # 安全转换（修复：用 pd.to_numeric 替代 astype）
    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'turn']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        nat_count = df['date'].isna().sum()
        if nat_count > 0:
            print(f"    ⚠️ {nat_count} 行日期解析失败，已删除")
        df = df.dropna(subset=['date'])
        df = df.sort_values('date').reset_index(drop=True)

    df = df.dropna(subset=['open', 'high', 'low', 'close'], how='all')
    return df


# ================== 数据获取 ==================

def fetch_hist_with_akshare(code: str, end_date_str: str) -> pd.DataFrame:
    """通过 akshare 获取历史数据"""
    import akshare as ak
    try:
        start_date = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d")
        end_str = end_date_str.replace('-', '')
        df = ak.stock_zh_a_hist(
            symbol=code, period="daily",
            start_date=start_date, end_date=end_str, adjust="qfq"
        )
        if df is not None and not df.empty:
            df = df.rename(columns={
                '日期': 'date', '开盘': 'open', '最高': 'high',
                '最低': 'low', '收盘': 'close', '成交量': 'volume',
                '成交额': 'amount', '换手率': 'turn'
            })
            return clean_numeric(df)
    except Exception as e:
        print(f"    akshare 异常: {type(e).__name__}: {str(e)[:80]}")
    return None


def fetch_hist_with_baostock(code: str, end_date_str: str) -> pd.DataFrame:
    """通过 baostock 获取历史数据"""
    import baostock as bs
    symbol = get_baostock_symbol(code)
    start_date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")

    for attempt in range(MAX_RETRY):
        try:
            rs = bs.query_history_k_data_plus(
                symbol,
                "date,open,high,low,close,volume,amount,turn",
                start_date=start_date,
                end_date=end_date_str,
                frequency="d",
                adjustflag="2"
            )
            data_list = []
            while rs.error_code == '0' and rs.next():
                data_list.append(rs.get_row_data())

            if not data_list:
                continue

            df = pd.DataFrame(data_list, columns=rs.fields)
            for col in ['open', 'high', 'low', 'close', 'volume', 'amount', 'turn']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            return clean_numeric(df)

        except Exception as e:
            print(f"    baostock 第{attempt+1}次失败: {type(e).__name__}: {str(e)[:80]}")
            time.sleep(1.5 + random.uniform(0, 1.0))

    return None


def fetch_hist_with_fallback(code: str, end_date_str: str) -> pd.DataFrame:
    """
    获取历史数据：akshare 优先，baostock 兜底。
    修复：删除了 ETF 兜底逻辑（严重bug）。
    """
    df = fetch_hist_with_akshare(code, end_date_str)
    if df is not None and len(df) >= MIN_HIST_DAYS:
        return df

    print(f"    └─ {code} akshare 失败，切换 baostock...")
    df = fetch_hist_with_baostock(code, end_date_str)
    if df is not None and len(df) >= MIN_HIST_DAYS:
        return df

    return None  # 修复：获取失败直接返回 None，不再用 ETF 兜底


def fetch_hist_with_cache(code: str, end_date_str: str):
    """
    带缓存的历史数据获取。
    修复：增加缓存时效控制（默认4小时）。
    """
    cache_file = CACHE_DIR / f"{code}_{end_date_str.replace('-', '')}.pkl"

    if cache_file.exists():
        # 检查缓存时效
        age_hours = (datetime.now().timestamp() - cache_file.stat().st_mtime) / 3600
        if age_hours < CACHE_TTL_HOURS:
            try:
                with open(cache_file, 'rb') as f:
                    df = pickle.load(f)
                return clean_numeric(df), None
            except Exception:
                pass
        else:
            cache_file.unlink(missing_ok=True)  # 过期删除

    df = fetch_hist_with_fallback(code, end_date_str)

    if df is not None and len(df) >= MIN_HIST_DAYS:
        df = clean_numeric(df)
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(df, f)
        except Exception:
            pass
        return df, None

    return None, f'{code} 数据获取失败（akshare + baostock 均失败）'


def batch_fetch_all_hist(codes: list, end_date_str: str) -> tuple:
    """批量获取历史数据"""
    hist_dict = {}
    errors = []

    try:
        from tqdm import tqdm
        iterator = tqdm(codes, desc="获取历史数据")
    except ImportError:
        print("⚠️ 未安装 tqdm，将不显示进度条。建议: pip install tqdm")
        iterator = codes

    for code in iterator:
        df, err = fetch_hist_with_cache(code, end_date_str)
        if df is not None:
            hist_dict[code] = df
        else:
            errors.append((code, err))
        time.sleep(0.1)  # 限速

    return hist_dict, errors


def fetch_hs300_data(end_date_str: str) -> pd.DataFrame:
    """获取沪深300 ETF 数据（仅用于指数对比，不作为个股兜底）"""
    import akshare as ak
    try:
        start_date = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d")
        end_str = end_date_str.replace('-', '')
        df = ak.stock_zh_a_hist(
            symbol="510300", period="daily",
            start_date=start_date, end_date=end_str, adjust="qfq"
        )
        if df is not None and not df.empty:
            df = df.rename(columns={
                '日期': 'date', '开盘': 'open', '最高': 'high',
                '最低': 'low', '收盘': 'close', '成交量': 'volume'
            })
            return clean_numeric(df)
    except Exception as e:
        print(f"  ⚠️ 沪深300数据获取失败: {e}")
    return None


# ================== 实时行情 ==================

def build_spot_lookup(spot_df: pd.DataFrame) -> dict:
    """
    构建代码→行情行的查找表。
    修复：向量化替代 iterrows，大幅提升性能。
    """
    if spot_df is None or spot_df.empty:
        return {}

    code_col = next((c for c in spot_df.columns if c in ('代码', 'code')), None)
    if code_col is None:
        return {}

    codes = spot_df[code_col].astype(str).str.replace(r'\D', '', regex=True).str.zfill(6)
    records = spot_df.to_dict('records')
    lookup = dict(zip(codes, records))
    return lookup


def fetch_today_quote(code: str, spot_df: pd.DataFrame = None,
                      spot_lookup: dict = None) -> dict:
    """从东财实时行情提取字段；缺失字段填 0。"""
    empty = {
        '今日涨跌幅': 0.0, '今日开盘价': 0.0, '今日收盘价': 0.0,
        '今日成交量': 0, '昨收': 0.0, '今开': 0.0,
        '今日最高': 0.0, '今日最低': 0.0, '今日振幅': 0.0,
        '量比': 0.0, '成交额': 0.0, '换手率': 0.0,
        '今日均价': 0.0, '内盘': 0.0, '外盘': 0.0, '委比': 0.0,
        '流通市值': 0.0,
    }

    symbol = normalize_code(code)
    r = None

    if spot_lookup is not None and symbol in spot_lookup:
        r = spot_lookup[symbol]
    elif spot_df is not None and not spot_df.empty:
        code_col = next((c for c in spot_df.columns if c in ('代码', 'code')), None)
        if code_col is not None:
            s = spot_df[code_col].astype(str).str.replace(r'\D', '', regex=True).str.zfill(6)
            hit = spot_df[s == symbol]
            if not hit.empty:
                r = hit.iloc[0]

    if r is None:
        return empty

    def g(*names, default=0.0):
        for n in names:
            try:
                if n in r:
                    val = r[n]
                    if pd.notna(val):
                        return float(val)
            except (ValueError, TypeError, KeyError):
                continue
        return default

    open_p = g('今开', '开盘')
    high_p = g('最高')
    low_p = g('最低')
    close_p = g('最新价', '收盘')
    pre_close = g('昨收')
    pct = g('涨跌幅', 'pct')
    vol = g('成交量', 'volume', '成交手')
    amount = g('成交额', 'amount')
    amp = g('振幅')
    vol_ratio = g('量比')
    turnover = g('换手率', '换手')
    float_mv = g('流通市值')

    # 修复：振幅重算加 max(0) 防负值
    if (amp is None or amp == 0) and pre_close > EPSILON and high_p > 0:
        amp = max((high_p - low_p) / pre_close * 100, 0)

    # 均价计算（修复：用 amount/close 反推判断单位）
    avg_price = 0.0
    if amount > 0 and vol > 0 and close_p > EPSILON:
        estimated_shares = amount / close_p  # 用成交额/收盘价估算股数
        # 判断 vol 是手还是股
        diff_as_shares = abs(vol - estimated_shares)
        diff_as_lots = abs(vol * 100 - estimated_shares)
        if diff_as_shares < diff_as_lots:
            shares = vol  # vol 本身就是股数
        else:
            shares = vol * 100  # vol 是手数
        avg_price = amount / (shares + EPSILON)

        # 合理性校验
        if not (0.2 < avg_price / close_p < 5.0):
            avg_price = (high_p + low_p + close_p) / 3
    elif close_p > 0:
        avg_price = (high_p + low_p + close_p) / 3

    return {
        '今日涨跌幅': round(pct, 2),
        '今日开盘价': round(open_p, 2),
        '今日收盘价': round(close_p, 2),
        '今日成交量': int(vol) if pd.notna(vol) and vol > 0 else 0,  # 修复：防NaN
        '昨收': round(pre_close, 2),
        '今开': round(open_p, 2),
        '今日最高': round(high_p, 2),
        '今日最低': round(low_p, 2),
        '今日振幅': round(amp, 2),
        '量比': round(vol_ratio, 2),
        '成交额': round(amount, 2),
        '换手率': round(turnover, 2),
        '今日均价': round(avg_price, 2),
        '内盘': round(g('内盘'), 2),
        '外盘': round(g('外盘'), 2),
        '委比': round(g('委比'), 2),
        '流通市值': round(float_mv, 2),
    }


# ================== 技术指标 ==================

def calculate_rsi(hist: pd.DataFrame, period: int = 14) -> float:
    """
    RSI - Wilder EMA 版本（与通达信/同花顺一致）
    修复：原版用 SMA，与主流软件不一致
    """
    if hist is None or len(hist) < period + 5:
        return 50.0

    delta = hist['close'].diff()
    gain = delta.clip(lower=0).ewm(alpha=1.0 / period, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1.0 / period, adjust=False).mean()

    rs = gain / (loss + EPSILON)
    rsi = 100 - (100 / (1 + rs))

    val = rsi.iloc[-1]
    if pd.isna(val):
        return 50.0
    return round(float(val), 2)


def calculate_macd_hist_series(hist: pd.DataFrame) -> pd.Series:
    """
    MACD 柱状图序列
    修复：最小数据量从35提高到50，确保EMA收敛
    """
    if hist is None or len(hist) < 50:
        return None

    ema12 = hist['close'].ewm(span=12, adjust=False).mean()
    ema26 = hist['close'].ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    return dif - dea


def calculate_kdj(hist: pd.DataFrame, n: int = 9, m1: int = 3, m2: int = 3):
    """
    KDJ 计算
    修复：RSV 除零溢出保护（high_n == low_n 时取中性值50）
    注：RSV=50 是业界标准做法（通达信/同花顺均如此）
    """
    if hist is None or len(hist) < n + 5:
        return 50.0, 50.0, 50.0

    low_n = hist['low'].rolling(n).min()
    high_n = hist['high'].rolling(n).max()

    denom = high_n - low_n
    rsv = np.where(
        denom > EPSILON,
        (hist['close'] - low_n) / denom * 100,
        50.0  # 无波动时取中性值（标准做法）
    )
    rsv = pd.Series(rsv, index=hist.index).clip(0, 100)

    k = rsv.ewm(alpha=1.0 / m1, adjust=False).mean()
    d = k.ewm(alpha=1.0 / m2, adjust=False).mean()
    j = 3 * k - 2 * d

    k_val = k.iloc[-1] if not pd.isna(k.iloc[-1]) else 50.0
    d_val = d.iloc[-1] if not pd.isna(d.iloc[-1]) else 50.0
    j_val = j.iloc[-1] if not pd.isna(j.iloc[-1]) else 50.0

    return round(float(k_val), 2), round(float(d_val), 2), round(float(j_val), 2)


def calculate_adx(hist: pd.DataFrame, period: int = 14) -> float:
    """ADX 趋势强度"""
    if hist is None or len(hist) < period * 2 + 1:
        return 0.0

    high = hist['high'].values
    low = hist['low'].values
    close = hist['close'].values

    tr = np.maximum(
        high[1:] - low[1:],
        np.maximum(
            np.abs(high[1:] - close[:-1]),
            np.abs(low[1:] - close[:-1])
        )
    )

    up_move = high[1:] - high[:-1]
    down_move = low[:-1] - low[1:]
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    atr = pd.Series(tr).ewm(alpha=1.0 / period, adjust=False).mean().values
    plus_di = 100 * pd.Series(plus_dm).ewm(alpha=1.0 / period, adjust=False).mean().values / (atr + EPSILON)
    minus_di = 100 * pd.Series(minus_dm).ewm(alpha=1.0 / period, adjust=False).mean().values / (atr + EPSILON)

    dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di + EPSILON)
    adx = pd.Series(dx).ewm(alpha=1.0 / period, adjust=False).mean().values

    if len(adx) == 0 or pd.isna(adx[-1]):
        return 0.0
    return round(float(adx[-1]), 2)


def calculate_obv_trend(hist: pd.DataFrame) -> str:
    """
    OBV 趋势判断
    修复：使用不重叠窗口（原版 obv[-5:] 是 obv[-15:] 的子集）
    """
    if hist is None or len(hist) < 25:
        return '下降'

    close_change = hist['close'].diff().fillna(0)
    obv_change = np.where(
        close_change > 0, hist['volume'],
        np.where(close_change < 0, -hist['volume'], 0)
    )
    obv = np.cumsum(obv_change)

    # 修复：不重叠窗口
    obv_5 = np.mean(obv[-5:])
    obv_prev15 = np.mean(obv[-20:-5])  # 前15天，不与后5天重叠

    threshold = CONFIG['obv_threshold']
    if obv_5 > obv_prev15 * threshold:
        return '上升'
    elif obv_5 < obv_prev15 / threshold:
        return '下降'
    return '平稳'


def calculate_max_drawdown(hist: pd.DataFrame, period: int = 60) -> float:
    """最大回撤"""
    if hist is None or len(hist) < 5:
        return 0.0
    period = min(period, len(hist))
    close = hist['close'].tail(period)
    peak = close.expanding().max()
    dd = (close - peak) / (peak + EPSILON)
    return round(abs(float(dd.min())) * 100, 2)


def calculate_volatility(hist: pd.DataFrame, period: int = 20) -> float:
    """年化波动率"""
    if hist is None or len(hist) < period + 1:
        return 0.0
    returns = hist['close'].tail(period + 1).pct_change().dropna()
    if len(returns) < 2:
        return 0.0
    return round(float(returns.std() * np.sqrt(252) * 100), 2)


def calculate_bias(hist: pd.DataFrame, period: int = 20) -> float:
    """BIAS 乖离率"""
    if hist is None or len(hist) < period:
        return 0.0
    ma = hist['close'].tail(period).mean()
    close = hist['close'].iloc[-1]
    if ma < EPSILON:
        return 0.0
    return round((close - ma) / ma * 100, 2)


# ================== 信号函数 ==================

def signal_breakout_20d_volume(hist: pd.DataFrame, vol_mult: float = 1.5) -> str:
    """20日新高 + 放量 + 阳线防诱多"""
    if hist is None or len(hist) < 25:
        return '否'

    last = hist.iloc[-1]
    prev = hist.iloc[-2]
    high_20d = hist['high'].iloc[-21:-1].max()
    vol_20d_avg = hist['volume'].iloc[-21:-1].mean()

    if vol_20d_avg < EPSILON:
        return '否'

    is_new_high = last['close'] > high_20d
    is_vol_up = last['volume'] > vol_20d_avg * vol_mult
    is_bullish = last['close'] > last['open'] and last['close'] > prev['close']

    if is_new_high and is_vol_up and is_bullish:
        return '是'
    return '否'


def signal_oversold_rebound(hist: pd.DataFrame) -> str:
    """RSI超卖区 + MACD绿柱收缩 + KDJ低位拐头"""
    if hist is None or len(hist) < 50:
        return '否'

    rsi_val = calculate_rsi(hist, CONFIG['rsi_period'])
    macd_hist = calculate_macd_hist_series(hist)
    if macd_hist is None:
        return '否'

    # MACD绿柱收缩（加3日平滑，减少噪声误判）
    bars_smooth = macd_hist.rolling(3).mean().tail(6)
    shrink_days = 0
    for i in range(1, len(bars_smooth)):
        if pd.notna(bars_smooth.iloc[i]) and pd.notna(bars_smooth.iloc[i - 1]):
            if bars_smooth.iloc[i] < 0 and bars_smooth.iloc[i] > bars_smooth.iloc[i - 1]:
                shrink_days += 1

    macd_shrink = shrink_days >= 2

    # KDJ低位拐头
    k_val, d_val, j_val = calculate_kdj(hist, CONFIG['kdj_n'], CONFIG['kdj_m1'], CONFIG['kdj_m2'])
    kdj_low_turn = k_val < 30 and k_val > d_val

    # RSI超卖
    rsi_oversold = rsi_val < 35

    if rsi_oversold and macd_shrink and kdj_low_turn:
        return '是'
    return '否'


def check_macd_golden_cross(hist: pd.DataFrame) -> str:
    """MACD金叉检测"""
    if hist is None or len(hist) < 50:
        return '无'

    ema12 = hist['close'].ewm(span=12, adjust=False).mean()
    ema26 = hist['close'].ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()

    cross = (dif.shift(1) < dea.shift(1)) & (dif > dea)
    if cross.tail(5).any():
        return '是（最近金叉）'
    return '无'


def count_consecutive_limits(hist: pd.DataFrame, limit_pct: float = 0.095) -> int:
    """
    计算近20日最大连续涨停次数。
    修复：只要不是涨停就重置计数器（原版阳线不重置是bug）
    修复：多取1天给shift用，避免首行NaN漏算
    """
    if hist is None or len(hist) < 21:
        return 0

    df = hist.tail(21).copy()
    df['pct_change'] = df['close'].pct_change()
    df = df.iloc[1:]  # 去掉第一行NaN

    df['is_limit'] = df['pct_change'] >= limit_pct

    max_consec = 0
    current = 0
    for is_limit in df['is_limit']:
        if is_limit:
            current += 1
            max_consec = max(max_consec, current)
        else:
            current = 0  # 修复：非涨停即重置

    return max_consec


# ================== 分析函数 ==================

def calc_streak_and_3d(hist: pd.DataFrame) -> dict:
    """连涨天、3日涨%、连续3天振幅%、连续3天均价"""
    out = {'连涨天': 0, '3日涨%': 0.0, '连续3天振幅%': 0.0, '连续3天均价': 0.0}

    if hist is None or len(hist) < 4:
        return out

    # 连涨天数
    streak = 0
    for i in range(len(hist) - 1, 0, -1):
        if hist['close'].iloc[i] > hist['close'].iloc[i - 1]:
            streak += 1
        else:
            break
    out['连涨天'] = streak

    # 3日涨幅
    last3 = hist.tail(3)
    if len(last3) == 3 and last3['close'].iloc[0] > EPSILON:
        out['3日涨%'] = round((last3['close'].iloc[-1] / last3['close'].iloc[0] - 1) * 100, 2)

    # 连续3天振幅
    amps = []
    for i in range(len(last3)):
        row = last3.iloc[i]
        if row['close'] > EPSILON:
            amps.append((row['high'] - row['low']) / row['close'] * 100)
    out['连续3天振幅%'] = round(float(np.mean(amps)), 2) if amps else 0.0

    # 连续3天均价
    out['连续3天均价'] = round(float(last3['close'].mean()), 2)

    return out


def detect_market_regime(hs300_df: pd.DataFrame, ma_period: int = 60) -> tuple:
    """判断市场状态"""
    if hs300_df is None or len(hs300_df) < ma_period + 2:
        return '未知', 0.0, 0.0

    close = hs300_df['close']
    ma = close.rolling(ma_period).mean()

    current = close.iloc[-1]
    ma_val = ma.iloc[-1]
    prev_close = close.iloc[-2]

    if pd.isna(ma_val) or ma_val < EPSILON:
        return '未知', 0.0, 0.0

    change_pct = (current / prev_close - 1) * 100 if prev_close > EPSILON else 0.0
    deviation = (current / ma_val - 1) * 100

    if current > ma_val * 1.02:
        regime = '强势'
    elif current > ma_val:
        regime = '偏强'
    elif current > ma_val * 0.98:
        regime = '偏弱'
    else:
        regime = '弱势'

    return regime, round(change_pct, 2), round(deviation, 2)


def calculate_relative_strength(hist: pd.DataFrame, hs300_df: pd.DataFrame) -> tuple:
    """
    计算相对强度评分。
    修复：使用 pd.merge 进行日期对齐（替代字符串匹配）
    修复：增加除零保护
    说明：(1+stock_ret)/(1+index_ret) 在指数为负时也能正确工作
         （跌得少 → 比值>1 → 表示相对强势）
    """
    if hist is None or hs300_df is None:
        return 0.0, 0
    if len(hist) < 20 or len(hs300_df) < 20:
        return 0.0, 0

    # 修复：用 pd.merge 进行日期对齐
    merged = pd.merge(
        hist[['date', 'close']].rename(columns={'close': 'close_s'}),
        hs300_df[['date', 'close']].rename(columns={'close': 'close_i'}),
        on='date', how='inner'
    )
    merged = merged.sort_values('date').reset_index(drop=True)

    if len(merged) < 10:
        return 0.0, 0

    # 取最近60个交易日
    merged = merged.tail(60)

    s0 = float(merged['close_s'].iloc[0])
    s1 = float(merged['close_s'].iloc[-1])
    i0 = float(merged['close_i'].iloc[0])
    i1 = float(merged['close_i'].iloc[-1])

    if s0 <= EPSILON or i0 <= EPSILON:
        return 0.0, 0

    stock_ret = s1 / s0
    index_ret = i1 / i0

    # 修复：除零保护
    if abs(index_ret) < EPSILON:
        return 0.0, 0

    # (1+stock_ret)/(1+index_ret) 在指数为负时也语义正确
    rs = stock_ret / index_ret

    # 评分
    thresholds = CONFIG['rs_thresholds']
    scores = CONFIG['rs_scores']

    score = scores[-1]
    for i, th in enumerate(thresholds):
        if rs >= th:
            score = scores[i]
            break

    return round(rs, 4), score


def calculate_chip_efficiency(hist: pd.DataFrame) -> float:
    """
    筹码效率（改进版：成交量加权均价）
    修复：原版仅用简单均价，未考虑成交量
    """
    if hist is None or len(hist) < 20:
        return 0.0

    last20 = hist.tail(20)
    total_vol = last20['volume'].sum()

    if total_vol < EPSILON:
        return 0.0

    # 成交量加权均价（近似筹码成本）
    weighted_cost = (last20['close'] * last20['volume']).sum() / total_vol

    if weighted_cost < EPSILON:
        return 0.0

    # 当前价在加权成本之上的天数比例
    above_ratio = (last20['close'] > weighted_cost).sum() / len(last20)
    return round(above_ratio * 100, 2)


def calculate_risk_score(hist: pd.DataFrame, current_price: float) -> dict:
    """
    风险评分（ATR止损）
    修复：除零保护完善
    修复：统一用 max_drop 约束，去掉矛盾的硬编码 0.95
    """
    result = {'止损价': 0.0, '止损距离%': 0.0, '风险等级': '未知'}

    if hist is None or len(hist) < 20 or current_price <= EPSILON:
        return result

    # ATR 计算
    high = hist['high'].values
    low = hist['low'].values
    close = hist['close'].values

    tr = np.maximum(
        high[1:] - low[1:],
        np.maximum(
            np.abs(high[1:] - close[:-1]),
            np.abs(low[1:] - close[:-1])
        )
    )
    atr_series = pd.Series(tr).ewm(alpha=1.0 / 14, adjust=False).mean()
    atr = float(atr_series.iloc[-1]) if not pd.isna(atr_series.iloc[-1]) else 0.0

    if atr < EPSILON:
        return result

    daily_vol = atr / current_price

    # 根据波动率选择ATR倍数
    if daily_vol > 0.05:
        atr_mult = CONFIG['atr_mult_high']
    elif daily_vol > 0.03:
        atr_mult = CONFIG['atr_mult_mid']
    else:
        atr_mult = CONFIG['atr_mult_low']

    # ATR止损
    atr_stop = current_price - atr_mult * atr

    # 修复：统一用 max_drop 约束（去掉矛盾的硬编码 0.95）
    pct_stop = current_price * (1 - CONFIG['max_drop'])
    stop_loss_final = max(atr_stop, pct_stop)

    # 止损距离
    if stop_loss_final > 0:
        stop_distance_pct = round((current_price - stop_loss_final) / current_price * 100, 2)
    else:
        stop_distance_pct = 0.0

    # 风险等级
    if stop_distance_pct <= 3:
        risk_level = '低'
    elif stop_distance_pct <= 6:
        risk_level = '中'
    else:
        risk_level = '高'

    result['止损价'] = round(stop_loss_final, 2)
    result['止损距离%'] = stop_distance_pct
    result['风险等级'] = risk_level

    return result


def calculate_liquidity_score(turnover: float, amount: float, float_mv: float = 0) -> float:
    """
    流动性评分
    修复：增加正向激励分档
    修复：增加流通市值因子
    """
    score = 0.0

    # 换手率评分
    if turnover < CONFIG['min_turnover']:
        score -= 15
    elif turnover >= 3.0:
        score += 10
    elif turnover >= 1.5:
        score += 5

    # 成交额评分
    if amount < 5e7:  # 5000万以下
        score -= 10
    elif amount > 5e8:  # 5亿以上
        score += 5

    # 流通市值评分（新增）
    if float_mv > 0:
        if float_mv < 2e9:  # 20亿以下
            score -= 5
        elif float_mv > 5e10:  # 500亿以上
            score += 5

    return score


def calculate_total_score(row: dict) -> pd.Series:
    """汇总评分"""
    s1 = row.get('启动得分', 0)
    s2 = row.get('筹码得分', 0)
    s3 = row.get('趋势得分', 0)
    s4 = row.get('共振得分', 0)
    s5 = row.get('资金得分', 0)
    s6 = row.get('风控得分', 0)
    s7 = row.get('RS 得分', 0)
    liquidity_penalty = row.get('流动性扣分', 0)

    total = s1 + s2 + s3 + s4 + s5 + s6 + s7 + liquidity_penalty
    total = max(0, min(100, total))
    pct = total / 100.0

    if pct >= 0.85:
        r, a = "S 级 (极强)", "重仓出击 (60-70%)"
    elif pct >= 0.75:
        r, a = "A 级 (强势)", "分批建仓 (40-50%)"
    elif pct >= 0.65:
        r, a = "B 级 (观察)", "轻仓试盘 (20-30%)"
    elif pct >= 0.50:
        r, a = "C 级 (弱势)", "观望 (<10%)"
    else:
        r, a = "D 级 (风险)", "排除/止损"

    return pd.Series(
        [s1, s2, s3, s4, s5, s6, s7, liquidity_penalty, total, r, a],
        index=['启动得分', '筹码得分', '趋势得分', '共振得分',
               '资金得分', '风控得分', 'RS 得分', '流动性扣分',
               '总分', '评级', '操作建议']
    )


# ================== 输出构建 ==================

def build_output_row(code: str, hist: pd.DataFrame, quote: dict,
                     hs300_df: pd.DataFrame = None) -> dict:
    """构建单只股票的输出行"""
    out = {'代码': code}
    out.update(quote)

    # 连涨和3日数据
    streak_data = calc_streak_and_3d(hist)
    out.update(streak_data)

    # 技术指标
    if hist is not None and len(hist) >= 50:
        out['RSI'] = calculate_rsi(hist, CONFIG['rsi_period'])
        k, d, j = calculate_kdj(hist, CONFIG['kdj_n'], CONFIG['kdj_m1'], CONFIG['kdj_m2'])
        out['KDJ_K'] = k
        out['KDJ_D'] = d
        out['KDJ_J'] = j
        out['ADX'] = calculate_adx(hist, CONFIG['adx_period'])
        out['MACD金叉'] = check_macd_golden_cross(hist)
        out['20日突破'] = signal_breakout_20d_volume(hist, CONFIG['vol_mult_breakout'])
        out['超卖反弹'] = signal_oversold_rebound(hist)
        out['筹码效率%'] = calculate_chip_efficiency(hist)
        out['OBV趋势'] = calculate_obv_trend(hist)
        out['60日最大回撤%'] = calculate_max_drawdown(hist, 60)
        out['20日年化波动%'] = calculate_volatility(hist, 20)
        out['BIAS20%'] = calculate_bias(hist, 20)
        out['连续涨停次数'] = count_consecutive_limits(hist)
    else:
        out['RSI'] = 50.0
        out['KDJ_K'] = 50.0
        out['KDJ_D'] = 50.0
        out['KDJ_J'] = 50.0
        out['ADX'] = 0.0
        out['MACD金叉'] = '无'
        out['20日突破'] = '否'
        out['超卖反弹'] = '否'
        out['筹码效率%'] = 0.0
        out['OBV趋势'] = '未知'
        out['60日最大回撤%'] = 0.0
        out['20日年化波动%'] = 0.0
        out['BIAS20%'] = 0.0
        out['连续涨停次数'] = 0

    # 相对强度
    if hs300_df is not None and hist is not None:
        rs_val, rs_score = calculate_relative_strength(hist, hs300_df)
        out['相对强度'] = rs_val
        out['强度评分'] = rs_score
    else:
        out['相对强度'] = 0.0
        out['强度评分'] = 0

    # 流动性评分
    out['流动性评分'] = calculate_liquidity_score(
        quote.get('换手率', 0),
        quote.get('成交额', 0),
        quote.get('流通市值', 0)
    )

    # 风险
    current_price = quote.get('今日收盘价', 0)
    risk = calculate_risk_score(hist, current_price)
    out.update(risk)

    # 市场状态
    if hs300_df is not None:
        regime, chg, dev = detect_market_regime(hs300_df)
        out['市场状态'] = regime
        out['市场涨跌幅%'] = chg
        out['市场偏离度%'] = dev
    else:
        out['市场状态'] = '未知'
        out['市场涨跌幅%'] = 0.0
        out['市场偏离度%'] = 0.0

    # 信号标签
    signal_tags = []
    if out.get('20日突破') == '是':
        signal_tags.append('20日突破放量')
    if out.get('超卖反弹') == '是':
        signal_tags.append('超跌蓄势')
    if out.get('MACD金叉') == '是（最近金叉）':
        signal_tags.append('MACD金叉')
    if out.get('连续涨停次数', 0) >= 2:
        signal_tags.append(f"连板{out['连续涨停次数']}")
    out['信号标签'] = '|'.join(signal_tags) if signal_tags else ''

    return out


# ================== 主流程 ==================

def run_scanner(code_list: list, end_date_str: str = None):
    """主扫描流程"""
    # 修复：历史数据截止日改为昨日，避免盘中日期错配
    if end_date_str is None:
        end_date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    print("=" * 60)
    print(f"  A股强势股扫描器 v791-final | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  历史数据截止日: {end_date_str}")
    print("=" * 60)

    # 1. 获取沪深300基准数据
    print("\n[1/5] 获取沪深300基准数据...")
    hs300_df = fetch_hs300_data(end_date_str)
    if hs300_df is None:
        print("  ⚠️ 沪深300数据获取失败，相对强度将为默认值")

    # 市场状态
    if hs300_df is not None:
        regime, chg, dev = detect_market_regime(hs300_df)
        print(f"  📈 大盘环境: {regime} | 涨跌幅: {chg}% | 偏离MA60: {dev}%")

    # 2. 批量获取个股历史数据
    print(f"\n[2/5] 批量获取个股历史数据 ({len(code_list)} 只)...")
    hist_dict, errors = batch_fetch_all_hist(code_list, end_date_str)
    print(f"  ✅ 成功: {len(hist_dict)} 只 | ❌ 失败: {len(errors)} 只")
    if errors:
        for code, err in errors[:5]:
            print(f"    - {code}: {err}")
        if len(errors) > 5:
            print(f"    ... 还有 {len(errors)-5} 只")

    # 3. 获取实时行情
    print("\n[3/5] 获取实时行情...")
    spot_df = None
    spot_lookup = {}
    try:
        import akshare as ak
        spot_df = ak.stock_zh_a_spot_em()
        spot_lookup = build_spot_lookup(spot_df)
        print(f"  ✅ 实时行情: {len(spot_lookup)} 只")
    except Exception as e:
        print(f"  ⚠️ 实时行情获取失败: {type(e).__name__}: {str(e)[:100]}")

    # 4. 逐股分析
    print(f"\n[4/5] 逐股分析...")
    results = []

    try:
        from tqdm import tqdm
        iterator = tqdm(code_list, desc="分析进度")
    except ImportError:
        iterator = code_list

    for code in iterator:
        try:
            hist = hist_dict.get(code)
            quote = fetch_today_quote(code, spot_df, spot_lookup)
            row = build_output_row(code, hist, quote, hs300_df)
            results.append(row)
        except Exception as e:
            print(f"  ⚠️ {code} 分析异常: {type(e).__name__}: {str(e)[:80]}")
            continue

    # 5. 输出结果
    print(f"\n[5/5] 输出结果...")
    if not results:
        print("  ⚠️ 无有效结果")
        return pd.DataFrame()

    result_df = pd.DataFrame(results)
    result_df = result_df.sort_values('今日涨跌幅', ascending=False).reset_index(drop=True)

    # 保存CSV
    output_csv = f"scan_result_{end_date_str.replace('-', '')}.csv"
    result_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"  ✅ CSV 已保存: {output_csv}")

    # 保存Excel（含信号预警sheet）
    try:
        output_xlsx = f"scan_result_{end_date_str.replace('-', '')}.xlsx"
        with pd.ExcelWriter(output_xlsx, engine='openpyxl') as writer:
            result_df.to_excel(writer, sheet_name='全部结果', index=False)

            # 信号预警
            sig_mask = result_df['信号标签'] != ''
            df_signal = result_df.loc[sig_mask]
            if not df_signal.empty:
                df_signal.to_excel(writer, sheet_name='信号预警', index=False)
            else:
                pd.DataFrame(columns=result_df.columns).to_excel(
                    writer, sheet_name='信号预警', index=False
                )
        print(f"  ✅ Excel 已保存: {output_xlsx}")
    except Exception as e:
        print(f"  ⚠️ Excel保存失败: {e}")

    print(f"\n🎯 扫描完成！共 {len(results)} 只有效结果")
    return result_df


# ================== 入口 ==================

if __name__ == '__main__':
    import baostock as bs

    # 登录 baostock
    lg = bs.login()
    if lg.error_code != '0':
        print(f"⚠️ Baostock 登录失败: {lg.error_msg}")

    # 获取全市场股票列表
    print("📋 获取A股列表...")
    try:
        import akshare as ak
        stock_list_df = ak.stock_zh_a_spot_em()[['代码', '名称']]
        code_list = stock_list_df['代码'].astype(str).str.zfill(6).tolist()
        print(f"  共 {len(code_list)} 只股票")
    except Exception as e:
        print(f"❌ 获取股票列表失败: {e}")
        # 测试用
        code_list = ['000001', '600519', '300750', '002594', '601318']
        print(f"  使用测试列表: {code_list}")

    # 运行扫描
    result = run_scanner(code_list)

    # 登出 baostock
    bs.logout()

    # 打印前10
    if not result.empty:
        print("\n📊 涨幅前10:")
        display_cols = ['代码', '今日涨跌幅', 'RSI', '20日突破', '超卖反弹', '信号标签']
        available_cols = [c for c in display_cols if c in result.columns]
        print(result[available_cols].head(10).to_string(index=False))
