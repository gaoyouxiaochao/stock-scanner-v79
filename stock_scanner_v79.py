#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
stock_scanner_v791_fixed.py
A股强势股扫描器 - 修复版
"""

import os
import re
import pickle
import warnings
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

warnings.filterwarnings('ignore')

# ================== 配置常量 ==================
EPSILON = 1e-9
MIN_HIST_DAYS = 30
CACHE_DIR = Path("./cache")
CACHE_DIR.mkdir(exist_ok=True)

CONFIG = {
    'max_drop': 0.08,           # 最大回撤止损比例
    'atr_mult': 2.0,            # ATR止损倍数
    'rs_thresholds': [1.2, 1.1, 1.0, 0.9],
    'rs_scores': [15, 10, 5, 0, -5],
    'min_turnover': 0.8,        # 最低换手率
    'obv_threshold': 1.01,
    'vol_mult_breakout': 1.5,   # 突破放量倍数
    'rsi_period': 14,
    'kdj_n': 9,
    'kdj_m1': 3,
    'kdj_m2': 3,
    'adx_period': 14,
}


# ================== 工具函数 ==================

def get_akshare_symbol(code: str) -> str:
    """将6位代码转为akshare格式"""
    code = str(code).strip().zfill(6)
    if code.startswith(('6', '9')):
        return f"sh{code}"
    elif code.startswith(('0', '2', '3')):
        return f"sz{code}"
    elif code.startswith(('4', '8')):
        return f"bj{code}"
    return code


def clean_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """将DataFrame中的数值列统一转为float，处理脏数据"""
    if df is None or df.empty:
        return df

    df = df.copy()

    # 列名映射：中文 → 英文
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

    # 确保关键列存在
    required_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
    for col in required_cols:
        if col not in df.columns:
            df[col] = 0.0

    # 安全转换数值列（修复：用 pd.to_numeric 替代 astype，避免空字符串崩溃）
    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'turn']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 处理日期列
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date'])
        df = df.sort_values('date').reset_index(drop=True)

    # 去掉全为NaN的行
    df = df.dropna(subset=['open', 'high', 'low', 'close'], how='all')

    return df


# ================== 数据获取 ==================

def fetch_hist_with_fallback(code: str, end_date_str: str) -> pd.DataFrame:
    """优先akshare，失败则用baostock"""
    import akshare as ak

    # 尝试 akshare
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
                '最低': 'low', '收盘': 'close', '成交量': 'volume'
            })
            return clean_numeric(df)
    except Exception:
        pass

    # 尝试 baostock
    try:
        import baostock as bs
        bs.login()
        symbol = get_akshare_symbol(code)
        start_date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
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
        bs.logout()

        if data_list:
            df = pd.DataFrame(data_list, columns=rs.fields)
            # 修复：用 pd.to_numeric 替代 astype，避免空字符串崩溃
            for col in ['open', 'high', 'low', 'close', 'volume', 'amount', 'turn']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            return clean_numeric(df)
    except Exception:
        pass

    # 最后尝试：用沪深300 ETF 数据作为兜底（仅用于指数对比）
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
    except Exception:
        pass

    return None


def fetch_hist_with_cache(code: str, end_date_str: str):
    """带缓存的历史数据获取"""
    cache_file = CACHE_DIR / f"{code}_{end_date_str.replace('-', '')}.pkl"

    if cache_file.exists():
        try:
            with open(cache_file, 'rb') as f:
                df = pickle.load(f)
            return clean_numeric(df), None
        except Exception:
            pass

    df = fetch_hist_with_fallback(code, end_date_str)

    if df is not None and len(df) >= MIN_HIST_DAYS:
        df = clean_numeric(df)
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(df, f)
        except Exception:
            pass
        return df, None

    return None, '数据获取失败（AkShare+Baostock均失败）'


def batch_fetch_all_hist(codes: list, end_date_str: str) -> dict:
    """批量获取历史数据"""
    hist_dict = {}
    errors = []
    for i, code in enumerate(codes):
        df, err = fetch_hist_with_cache(code, end_date_str)
        if df is not None:
            hist_dict[code] = df
        else:
            errors.append((code, err))
        if (i + 1) % 50 == 0:
            print(f"  进度: {i+1}/{len(codes)}")
    return hist_dict, errors


# ================== 实时行情 ==================

def build_spot_lookup(spot_df: pd.DataFrame) -> dict:
    """构建代码→行情行的查找表（向量化，替代iterrows）"""
    if spot_df is None or spot_df.empty:
        return {}

    code_col = next((c for c in spot_df.columns if c in ('代码', 'code')), None)
    if code_col is None:
        return {}

    # 修复：向量化替代 iterrows，大幅提升性能
    codes = spot_df[code_col].astype(str).str.replace(r'\D', '', regex=True).str.zfill(6)
    lookup = dict(zip(codes, spot_df.to_dict('records')))
    return lookup


def fetch_today_quote(code: str, spot_df: pd.DataFrame, spot_lookup: dict = None) -> dict:
    """从东财实时行情提取字段；缺失字段填 0。"""
    empty = {
        '今日涨跌幅': 0.0, '今日开盘价': 0.0, '今日收盘价': 0.0,
        '今日成交量': 0, '昨收': 0.0, '今开': 0.0,
        '今日最高': 0.0, '今日最低': 0.0, '今日振幅': 0.0,
        '量比': 0.0, '成交额': 0.0, '换手率': 0.0,
        '今日均价': 0.0, '内盘': 0.0, '外盘': 0.0, '委比': 0.0,
    }

    symbol = code.zfill(6)
    r = None

    if spot_lookup is not None:
        r = spot_lookup.get(symbol)
    elif spot_df is not None and not getattr(spot_df, 'empty', True):
        colmap = {str(c).strip(): c for c in spot_df.columns}
        code_col = colmap.get('代码') or colmap.get('code')
        if code_col is not None:
            s = spot_df[code_col].astype(str).str.replace(r'\D', '', regex=True).str.zfill(6)
            hit = spot_df[s == symbol]
            if not hit.empty:
                r = hit.iloc[0]

    if r is None:
        return empty

    # 辅助取值函数
    def g(*names, default=0.0):
        for n in names:
            if n in r.index if hasattr(r, 'index') else n in r:
                val = r[n] if hasattr(r, '__getitem__') else r.get(n)
                if pd.notna(val):
                    try:
                        return float(val)
                    except (ValueError, TypeError):
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

    # 修复：振幅重算加 max(0) 防负值
    if amp == 0 and pre_close > EPSILON:
        amp = max((high_p - low_p) / pre_close * 100, 0)

    # 均价计算
    avg_price = 0.0
    if amount > 0 and vol > 0:
        # 修复：判断 vol 单位（手 vs 股）
        if vol > 1e8:
            shares = vol  # 已经是股数
        else:
            shares = vol * 100  # 手 → 股
        avg_price = amount / (shares + EPSILON)

        # 合理性校验
        if close_p > 0 and (avg_price > close_p * 5 or avg_price < close_p * 0.2):
            avg_price = (high_p + low_p + close_p) / 3 if close_p > 0 else close_p
    elif close_p > 0:
        avg_price = (high_p + low_p + close_p) / 3

    return {
        '今日涨跌幅': round(pct, 2),
        '今日开盘价': round(open_p, 2),
        '今日收盘价': round(close_p, 2),
        '今日成交量': int(vol) if pd.notna(vol) and vol > 0 else 0,  # 修复：防NaN崩溃
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
    }


# ================== 技术指标 ==================

def precompute_indicators(hist: pd.DataFrame) -> pd.DataFrame:
    """预计算常用均线"""
    df = hist.copy()
    df['ma5'] = df['close'].rolling(5).mean()
    df['ma10'] = df['close'].rolling(10).mean()
    df['ma20'] = df['close'].rolling(20).mean()
    df['ma60'] = df['close'].rolling(60).mean()
    return df


def calculate_rsi(hist: pd.DataFrame, period: int = 14) -> float:
    """
    RSI 计算
    修复：使用 Wilder EMA 替代 SMA，与主流软件一致
    """
    if hist is None or len(hist) < period + 5:
        return 50.0

    delta = hist['close'].diff()
    # 修复：使用 ewm 替代 rolling.mean()
    gain = delta.clip(lower=0).ewm(alpha=1.0 / period, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1.0 / period, adjust=False).mean()

    rs = gain / (loss + EPSILON)
    rsi = 100 - (100 / (1 + rs))

    val = rsi.iloc[-1]
    if pd.isna(val):
        return 50.0
    return round(float(val), 2)


def calculate_macd_hist_series(hist: pd.DataFrame) -> pd.Series:
    """计算MACD柱状图序列"""
    # 修复：最小数据量从35提高到50，确保EMA收敛
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
    修复：RSV 除零溢出保护
    """
    if hist is None or len(hist) < n + 5:
        return 50.0, 50.0, 50.0

    low_n = hist['low'].rolling(n).min()
    high_n = hist['high'].rolling(n).max()

    # 修复：防止 high_n == low_n 时 RSV 溢出
    denom = high_n - low_n
    rsv = np.where(
        denom > EPSILON,
        (hist['close'] - low_n) / denom * 100,
        50.0  # 无波动时取中性值
    )
    rsv = pd.Series(rsv, index=hist.index).clip(0, 100)

    k = rsv.ewm(alpha=1.0 / m1, adjust=False).mean()
    d = k.ewm(alpha=1.0 / m2, adjust=False).mean()
    j = 3 * k - 2 * d

    k_val = k.iloc[-1]
    d_val = d.iloc[-1]
    j_val = j.iloc[-1]

    if pd.isna(k_val) or pd.isna(d_val) or pd.isna(j_val):
        return 50.0, 50.0, 50.0

    return round(float(k_val), 2), round(float(d_val), 2), round(float(j_val), 2)


def calculate_adx(hist: pd.DataFrame, period: int = 14) -> float:
    """ADX 计算"""
    if hist is None or len(hist) < period * 2 + 1:
        return 0.0

    high = hist['high'].values
    low = hist['low'].values
    close = hist['close'].values

    # True Range
    tr = np.maximum(
        high[1:] - low[1:],
        np.maximum(
            np.abs(high[1:] - close[:-1]),
            np.abs(low[1:] - close[:-1])
        )
    )

    # +DM / -DM
    up_move = high[1:] - high[:-1]
    down_move = low[:-1] - low[1:]
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)

    # Wilder 平滑
    atr = pd.Series(tr).ewm(alpha=1.0 / period, adjust=False).mean().values
    plus_di = 100 * pd.Series(plus_dm).ewm(alpha=1.0 / period, adjust=False).mean().values / (atr + EPSILON)
    minus_di = 100 * pd.Series(minus_dm).ewm(alpha=1.0 / period, adjust=False).mean().values / (atr + EPSILON)

    dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di + EPSILON)
    adx = pd.Series(dx).ewm(alpha=1.0 / period, adjust=False).mean().values

    if len(adx) == 0 or pd.isna(adx[-1]):
        return 0.0
    return round(float(adx[-1]), 2)


def calculate_obv(hist: pd.DataFrame) -> np.ndarray:
    """OBV 能量潮"""
    if hist is None or len(hist) < 5:
        return np.array([0])
    direction = np.sign(hist['close'].diff().fillna(0))
    obv = (direction * hist['volume']).cumsum().values
    return obv


# ================== 信号函数 ==================

def signal_breakout_20d_volume(hist: pd.DataFrame, vol_mult: float = 1.5) -> str:
    """20日新高 + 放量 + 阳线防诱多"""
    if hist is None or len(hist) < 25:
        return '否'

    last = hist.iloc[-1]
    prev = hist.iloc[-2]
    high_20d = hist['high'].iloc[-21:-1].max()
    vol_20d_avg = hist['volume'].iloc[-21:-1].mean()

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

    # 修复：显式传入 period
    rsi_val = calculate_rsi(hist, CONFIG['rsi_period'])
    macd_hist = calculate_macd_hist_series(hist)

    if macd_hist is None:
        return '否'

    # MACD绿柱收缩判断
    bars = macd_hist.tail(6)
    shrink_days = 0
    for i in range(1, len(bars)):
        if bars.iloc[i] < 0 and bars.iloc[i] > bars.iloc[i - 1]:
            shrink_days += 1

    macd_shrink = shrink_days >= 2

    # KDJ低位拐头
    k, d, j = calculate_kdj(hist, CONFIG['kdj_n'], CONFIG['kdj_m1'], CONFIG['kdj_m2'])
    kdj_low_turn = k < 30 and k > d

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


# ================== 分析函数 ==================

def calc_streak_and_3d(hist: pd.DataFrame) -> dict:
    """连涨天、3日涨%、连续3天振幅%、连续3天均价"""
    out = {
        '连涨天': 0,
        '3日涨%': 0.0,
        '连续3天振幅%': 0.0,
        '连续3天均价': 0.0,
    }

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
        out['3日涨%'] = round(
            (last3['close'].iloc[-1] / last3['close'].iloc[0] - 1) * 100, 2
        )

    # 连续3天振幅
    amps = []
    for i in range(len(last3)):
        row = last3.iloc[i]
        if row['close'] > EPSILON:
            amp_val = (row['high'] - row['low']) / row['close'] * 100
            amps.append(amp_val)
    out['连续3天振幅%'] = round(float(np.mean(amps)), 2) if amps else 0.0

    # 连续3天均价
    out['连续3天均价'] = round(float(last3['close'].mean()), 2)

    return out


def detect_market_regime(hs300_df: pd.DataFrame, ma_period: int = 60):
    """
    判断市场状态
    返回: (状态, 涨跌幅%, 距均线偏离度%)
    """
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
    计算相对强度评分
    修复：增加 index_ret 除零保护
    """
    if hist is None or hs300_df is None:
        return 0.0, 0

    if len(hist) < 20 or len(hs300_df) < 20:
        return 0.0, 0

    # 对齐日期
    hist_dates = set(hist['date'].dt.strftime('%Y-%m-%d'))
    hs300_aligned = hs300_df[hs300_df['date'].dt.strftime('%Y-%m-%d').isin(hist_dates)]

    if len(hs300_aligned) < 10:
        return 0.0, 0

    # 个股收益率
    stock_ret = hist['close'].iloc[-1] / hist['close'].iloc[0]

    # 指数收益率
    index_ret = hs300_aligned['close'].iloc[-1] / hs300_aligned['close'].iloc[0]

    # 修复：除零保护
    if abs(index_ret) < EPSILON:
        return 0.0, 0

    rs = stock_ret / index_ret

    # 评分
    thresholds = CONFIG['rs_thresholds']
    scores = CONFIG['rs_scores']

    score = scores[-1]  # 默认最低分
    for i, th in enumerate(thresholds):
        if rs >= th:
            score = scores[i]
            break

    return round(rs, 4), score


def calculate_chip_efficiency(hist: pd.DataFrame) -> float:
    """筹码效率（简化版：近20日收盘价在均价之上的比例）"""
    if hist is None or len(hist) < 20:
        return 0.0

    last20 = hist.tail(20)
    avg_cost = last20['close'].mean()

    if avg_cost < EPSILON:
        return 0.0

    above_ratio = (last20['close'] > avg_cost).sum() / len(last20)
    return round(above_ratio * 100, 2)


def calculate_risk_score(hist: pd.DataFrame, current_price: float, stop_loss: float) -> dict:
    """
    风险评分
    修复：除零保护完善
    """
    result = {
        '止损价': 0.0,
        '止损距离%': 0.0,
        '风险等级': '未知',
    }

    if hist is None or len(hist) < 20 or current_price <= 0:
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
    atr = pd.Series(tr).ewm(alpha=1.0 / CONFIG['atr_mult'], adjust=False).mean().iloc[-1]

    # 止损价
    atr_stop = current_price - atr * CONFIG['atr_mult']
    pct_stop = current_price * (1 - CONFIG['max_drop'])
    stop_loss_final = max(atr_stop, pct_stop)

    # 修复：完善除零保护
    if stop_loss_final > 0 and current_price > EPSILON:
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


def calculate_liquidity_score(turnover: float, amount: float) -> float:
    """
    流动性评分
    修复：增加正向激励分档
    """
    score = 0.0

    # 换手率评分
    if turnover < CONFIG['min_turnover']:
        score -= 15
    elif turnover >= 3.0:
        score += 10
    elif turnover >= 1.5:
        score += 5

    # 成交额评分（单位：元）
    if amount < 5e7:  # 5000万以下
        score -= 10
    elif amount > 5e8:  # 5亿以上
        score += 5

    return score


# ================== 输出构建 ==================

def build_output_row(code: str, hist: pd.DataFrame, quote: dict,
                     hs300_df: pd.DataFrame = None) -> dict:
    """构建单只股票的输出行"""
    out = {'代码': code}

    # 实时行情
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

    # 相对强度
    if hs300_df is not None:
        rs_val, rs_score = calculate_relative_strength(hist, hs300_df)
        out['相对强度'] = rs_val
        out['强度评分'] = rs_score
    else:
        out['相对强度'] = 0.0
        out['强度评分'] = 0

    # 流动性评分
    out['流动性评分'] = calculate_liquidity_score(
        quote.get('换手率', 0), quote.get('成交额', 0)
    )

    # 风险
    current_price = quote.get('今日收盘价', 0)
    risk = calculate_risk_score(hist, current_price, 0)
    out.update(risk)

    # 市场状态
    if hs300_df is not None:
        regime, chg, dev = detect_market_regime(hs300_df)
        out['市场状态'] = regime
    else:
        out['市场状态'] = '未知'

    return out


# ================== 主流程 ==================

def run_scanner(code_list: list, end_date_str: str = None):
    """主扫描流程"""
    if end_date_str is None:
        end_date_str = datetime.now().strftime('%Y-%m-%d')

    print(f"[1/5] 获取沪深300基准数据...")
    hs300_df = fetch_hist_with_fallback("510300", end_date_str)

    print(f"[2/5] 批量获取个股历史数据 ({len(code_list)} 只)...")
    hist_dict, errors = batch_fetch_all_hist(code_list, end_date_str)
    if errors:
        print(f"  ⚠️ {len(errors)} 只股票数据获取失败")

    print(f"[3/5] 获取实时行情...")
    try:
        import akshare as ak
        spot_df = ak.stock_zh_a_spot_em()
        spot_lookup = build_spot_lookup(spot_df)
    except Exception as e:
        print(f"  ⚠️ 实时行情获取失败: {e}")
        spot_df = None
        spot_lookup = {}

    print(f"[4/5] 逐股分析...")
    results = []
    for i, code in enumerate(code_list):
        hist = hist_dict.get(code)
        quote = fetch_today_quote(code, spot_df, spot_lookup)
        row = build_output_row(code, hist, quote, hs300_df)
        results.append(row)

        if (i + 1) % 100 == 0:
            print(f"  进度: {i+1}/{len(code_list)}")

    print(f"[5/5] 输出结果...")
    result_df = pd.DataFrame(results)

    # 保存
    output_file = f"scan_result_{end_date_str.replace('-', '')}.csv"
    result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"  ✅ 结果已保存至 {output_file}")

    return result_df


# ================== 入口 ==================

if __name__ == '__main__':
    # 示例：扫描部分股票
    # 实际使用时替换为完整股票列表
    test_codes = ['000001', '600519', '300750', '002594', '601318']
    result = run_scanner(test_codes)
    print(result.head())
