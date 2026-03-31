# -*- coding: utf-8 -*-
""" 【A股强势股扫描器 v7.9_MultiSource_Final_Stable】
- 最终干净版：无任何append调用 + Lock保护 + 强重试
- 32只/424只均可稳定运行 """

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys
import re
import time
import warnings
import traceback
import random
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

warnings.filterwarnings('ignore')

try:
    from tqdm import tqdm
    USE_TQDM = True
except ImportError:
    USE_TQDM = False
    print("⚠️ 未安装 tqdm，将不显示进度条。建议运行: pip install tqdm")

import akshare as ak
import baostock as bs

# ================== 配置区域 ==================
POSSIBLE_INPUTS = [
    Path("输入股票代码及名称清单v1.xlsx"),      # ← 必须是这个名字！不能改
]

OUTPUT_DIR = Path("results")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CACHE_DIR = Path("stock_cache_v79")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

MAX_HIST_DAYS = 150
MAX_WORKERS = 1
MIN_HIST_DAYS = 30
BAOSTOCK_RETRY = 5

MARKET_CONFIG = {
    '主板': {'limit_pct': 0.095, 'breakout_pct': 6.0, 'atr_mult_high': 3.0, 'atr_mult_mid': 2.5, 'atr_mult_low': 2.0, 'max_drop': 0.20},
    '科创/创业板': {'limit_pct': 0.195, 'breakout_pct': 8.0, 'atr_mult_high': 3.5, 'atr_mult_mid': 3.0, 'atr_mult_low': 2.5, 'max_drop': 0.30},
    '北交所': {'limit_pct': 0.295, 'breakout_pct': 10.0, 'atr_mult_high': 4.0, 'atr_mult_mid': 3.5, 'atr_mult_low': 3.0, 'max_drop': 0.40}
}
EPSILON = 1e-9

BS_LOCK = Lock()

# ================== 多数据源回退 ==================
BS_LOGGED_IN = False

def bs_login_once():
    global BS_LOGGED_IN
    with BS_LOCK:
        if not BS_LOGGED_IN:
            try:
                bs.login()
                BS_LOGGED_IN = True
                print("✅ Baostock 登录成功")
                return True
            except Exception as e:
                print(f"❌ Baostock 登录失败: {e}")
                return False
        return True

def get_baostock_symbol(code: str) -> str:
    code = re.sub(r'\D', '', str(code).strip())
    if len(code) != 6:
        return ""
    if code.startswith(('6', '5', '9')):
        return f"sh.{code}"
    return f"sz.{code}"

def fetch_with_baostock(code: str, start_date: str, end_date: str):
    if not bs_login_once():
        return None
    symbol = get_baostock_symbol(code)
    if not symbol:
        return None

    print(f"🔍 Baostock 查询 {symbol} | 开始: {start_date} | 结束: {end_date}")

    for attempt in range(BAOSTOCK_RETRY):
        for adjust in ["2", "1"]:
            try:
                with BS_LOCK:
                    rs = bs.query_history_k_data_plus(
                        symbol,
                        "date,open,high,low,close,volume,amount,turn,peTTM,pbMRQ",
                        start_date=start_date,
                        end_date=end_date,
                        frequency="d",
                        adjustflag=adjust
                    )
                if rs.error_code != '0':
                    continue

                df = rs.get_data()
                if df.empty or len(df) < MIN_HIST_DAYS:
                    continue

                print(f"   ✅ 第{attempt+1}次成功 获取 {len(df)} 条数据")
                df = df.astype({'open':'float','high':'float','low':'float','close':'float',
                                'volume':'float','amount':'float','turn':'float'})
                df.rename(columns={'turn': 'turnover_rate'}, inplace=True)
                df['date'] = pd.to_datetime(df['date'])
                return df
            except Exception as e:
                print(f"   ❌ 第{attempt+1}次失败: {str(e)[:120]}")
                time.sleep(2.0 + random.uniform(0, 2.0))
    print(f"❌ {code} Baostock 所有尝试均失败")
    return None

def fetch_hist_with_fallback(code: str, end_date_str: str):
    symbol = re.sub(r'\D', '', str(code).strip().upper())
    start_date_ak = (datetime.now() - timedelta(days=MAX_HIST_DAYS)).strftime("%Y%m%d")
    end_str_ak = end_date_str.replace('-', '')
    start_date_bs = (datetime.now() - timedelta(days=MAX_HIST_DAYS)).strftime("%Y-%m-%d")
    end_date_bs = end_date_str

    for attempt in range(2):
        try:
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily",
                                    start_date=start_date_ak, end_date=end_str_ak, adjust="qfq")
            if not df.empty and len(df) >= MIN_HIST_DAYS:
                df = df.rename(columns={'日期':'date','开盘':'open','最高':'high',
                                        '最低':'low','收盘':'close','成交量':'volume'})
                return df
            time.sleep(0.5)
        except:
            time.sleep(0.8 + random.uniform(0, 0.3))

    print(f"  └─ {code} akshare 失败，切换 Baostock...")
    df = fetch_with_baostock(code, start_date_bs, end_date_bs)
    if df is not None and len(df) >= MIN_HIST_DAYS:
        return df
    return None

# ================== 基础工具 ==================
def get_last_trade_day():
    return datetime.now().strftime("%Y-%m-%d")

def is_beijing_stock(code):
    code = re.sub(r'\D', '', str(code).strip())
    if len(code) != 6:
        return False
    return code[:2] in ['83', '87', '88', '89'] or code[:3] == '920'

def detect_market_type(code):
    code = re.sub(r'\D', '', str(code).strip())
    if not code or len(code) != 6:
        return '主板'
    if is_beijing_stock(code):
        return '北交所'
    if code.startswith('3') or code.startswith('688') or code.startswith('689'):
        return '科创/创业板'
    return '主板'

def get_akshare_symbol(code):
    return re.sub(r'\D', '', str(code).strip().upper())

def clean_numeric(df):
    df.columns = [str(c).lower().strip() for c in df.columns]
    price_map = {'开盘':'open','最高':'high','最低':'low','收盘':'close',
                 '开盘价':'open','最高价':'high','最低价':'low','收盘价':'close','日期':'date'}
    for cn, en in price_map.items():
        if cn in df.columns and en not in df.columns:
            df[en] = df[cn]
    for col in ['open', 'high', 'low', 'close']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    vol_cols = ['成交量', 'volume', 'vol', 'vol_amt']
    for col in vol_cols:
        if col in df.columns:
            df['volume'] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            break
    if 'volume' not in df.columns:
        df['volume'] = 0.0
    turn_cols = ['换手率', 'turnover_rate', 'turnover', '周转率', 'hsl']
    for col in turn_cols:
        if col in df.columns:
            df['turnover_rate'] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            break
    if 'turnover_rate' not in df.columns:
        df['turnover_rate'] = 0.0
    if 'date' not in df.columns:
        df['date'] = pd.to_datetime(df.index)
    else:
        df['date'] = pd.to_datetime(df['date'])
    df = df.dropna(subset=['close'])
    df = df[df['close'] > EPSILON]
    return df

def fetch_hs300_data(end_date_str):
    try:
        df = ak.index_zh_a_hist(symbol="000300", period="daily")
        if not df.empty:
            df = df.rename(columns={'日期': 'date', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close', '成交量': 'volume'})
            return clean_numeric(df)
    except:
        pass
    try:
        start_date = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d")
        end_str = end_date_str.replace('-', '')
        df = ak.stock_zh_a_hist(symbol="510300", period="daily", start_date=start_date, end_date=end_str, adjust="qfq")
        if not df.empty:
            df = df.rename(columns={'日期': 'date', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close', '成交量': 'volume'})
            return clean_numeric(df)
    except:
        pass
    return None

def fetch_hist_with_cache(code, end_date_str):
    cache_file = CACHE_DIR / f"{code}_{end_date_str.replace('-', '')}.pkl"
    if cache_file.exists():
        try:
            with open(cache_file, 'rb') as f:
                df = pickle.load(f)
            return clean_numeric(df), None
        except:
            pass
    df = fetch_hist_with_fallback(code, end_date_str)
    if df is not None and len(df) >= MIN_HIST_DAYS:
        df = clean_numeric(df)
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(df, f)
        except:
            pass
        return df, None
    return None, '数据获取失败（AkShare+Baostock均失败）'

def batch_fetch_all_hist(codes, end_date_str):
    hist_dict = {}
    errors = []
    print(f"🚀 并行拉取 {len(codes)} 只股票历史数据（{MAX_WORKERS} 线程）...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_code = {executor.submit(fetch_hist_with_cache, code, end_date_str): code for code in codes}
        for future in (tqdm(as_completed(future_to_code), total=len(codes), desc="📥 下载K线") if USE_TQDM else as_completed(future_to_code)):
            code = future_to_code[future]
            try:
                hist, err = future.result()
                if hist is not None:
                    hist_dict[code] = hist
                else:
                    errors.append(f"{code}: {err}")
            except Exception as e:
                errors.append(f"{code}: {str(e)[:100]}")
    print(f"✅ 数据拉取完成！成功 {len(hist_dict)} 只 | 失败 {len(errors)} 只")
    return hist_dict, errors

spot_cache = None
def get_all_spot_data():
    global spot_cache
    if spot_cache is not None:
        return spot_cache
    try:
        spot_cache = ak.stock_zh_a_spot_em()
        if not spot_cache.empty:
            spot_cache.columns = [str(c).lower().strip() for c in spot_cache.columns]
        return spot_cache
    except:
        return pd.DataFrame()

def fetch_today_quote(code, spot_df):
    symbol = get_akshare_symbol(code)
    if spot_df.empty:
        return {'今日涨跌幅': 0, '今日开盘价': 0, '今日收盘价': 0, '今日成交量': 0}
    code_col = '代码' if '代码' in spot_df.columns else 'code'
    row = spot_df[spot_df[code_col].astype(str).str.strip() == symbol]
    if not row.empty:
        open_col = next((c for c in ['今开', '开盘'] if c in row.columns), None)
        close_col = next((c for c in ['最新价', '收盘'] if c in row.columns), None)
        pct_col = next((c for c in ['涨跌幅', 'pct'] if c in row.columns), None)
        vol_col = next((c for c in ['成交量', 'volume', '成交手'] if c in row.columns), None)
        open_price = float(row[open_col].iloc[0]) if open_col else 0
        close_price = float(row[close_col].iloc[0]) if close_col else 0
        pct = float(row[pct_col].iloc[0]) if pct_col else 0
        volume = float(row[vol_col].iloc[0]) if vol_col else 0
        return {'今日涨跌幅': round(pct, 2), '今日开盘价': round(open_price, 2),
                '今日收盘价': round(close_price, 2), '今日成交量': int(volume)}
    return {'今日涨跌幅': 0, '今日开盘价': 0, '今日收盘价': 0, '今日成交量': 0}

# ================== 技术指标（干净无append）==================
def precompute_indicators(hist):
    df = hist.copy()
    df['ma5'] = df['close'].rolling(5).mean()
    df['ma10'] = df['close'].rolling(10).mean()
    df['ma20'] = df['close'].rolling(20).mean()
    return df

def calculate_adx(hist, period=14):
    if len(hist) < period * 2 + 1:
        return 0.0
    high, low, close = hist['high'], hist['low'], hist['close']
    tr = pd.concat([high - low, abs(high - close.shift()), abs(low - close.shift())], axis=1).max(axis=1)
    plus_dm = high.diff().where((high.diff() > -low.diff()) & (high.diff() > 0), 0)
    minus_dm = (-low.diff()).where((-low.diff() > high.diff()) & (-low.diff() > 0), 0)
    atr = tr.ewm(alpha=1 / period, adjust=False).mean()
    atr_safe = atr.where(atr > EPSILON, EPSILON)
    plus_di = 100 * plus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr_safe
    minus_di = 100 * minus_dm.ewm(alpha=1 / period, adjust=False).mean() / atr_safe
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di + EPSILON)
    adx = dx.ewm(alpha=1 / period, adjust=False).mean()
    return round(adx.iloc[-1], 2)

def detect_technical_patterns(hist):
    if len(hist) < 20:
        return '无明显形态'
    patterns = []
    if hist['ma5'].iloc[-1] > hist['ma10'].iloc[-1] and hist['ma5'].iloc[-2] <= hist['ma10'].iloc[-2]:
        patterns.append("MA5 金叉 MA10")
    if hist['close'].iloc[-1] > hist['high'].tail(20).max() * 1.02:
        patterns.append("突破 20 日高点")
    vol_ma5 = hist['volume'].rolling(5).mean()
    if hist['volume'].iloc[-1] > vol_ma5.iloc[-1] * 2 and hist['close'].iloc[-1] > hist['close'].iloc[-2] * 1.015:
        patterns.append("放量突破")
    if (hist['close'].tail(3) > hist['close'].tail(3).shift()).sum() >= 2:
        patterns.append("近期连阳")
    if hist['close'].iloc[-1] > hist['ma20'].iloc[-1]:
        patterns.append("站上 MA20")
    return " | ".join(patterns) if patterns else "无明显形态"

def detect_kline_patterns(hist):
    if len(hist) < 3:
        return '数据不足'
    prev2 = hist.iloc[-3]
    prev1 = hist.iloc[-2]
    curr = hist.iloc[-1]
    def body_info(row):
        body = abs(row['close'] - row['open'])
        is_yang = row['close'] > row['open']
        is_yin = row['close'] < row['open']
        return body, is_yang, is_yin
    b2, yang2, yin2 = body_info(prev2)
    b1, yang1, yin1 = body_info(prev1)
    bc, yangc, yinc = body_info(curr)
    if len(hist) >= 20:
        avg_body = (hist['close'] - hist['open']).abs().tail(20).mean()
    else:
        avg_body = (b2 + b1 + bc) / 3
    if avg_body < EPSILON:
        avg_body = EPSILON
    def is_large(body): return body > avg_body * 1.5
    def is_small(body): return body < avg_body * 0.5
    def is_doji(body): return body < avg_body * 0.15
    gap_up = curr['low'] > prev1['high']
    prev1_mid = (prev1['open'] + prev1['close']) / 2
    prev2_mid = (prev2['open'] + prev2['close']) / 2
    strong_signals = []
    normal_signals = []
    lower_shadow_c = min(curr['open'], curr['close']) - curr['low']
    upper_shadow_c = curr['high'] - max(curr['open'], curr['close'])
    if is_small(bc) and lower_shadow_c > bc * 2 and upper_shadow_c < bc * 0.5:
        strong_signals.append("锤头线 (看涨)" if yangc else "上吊线 (警惕)")
    if is_small(bc) and upper_shadow_c > bc * 2 and lower_shadow_c < bc * 0.5:
        normal_signals.append("流星线 (看跌)" if yinc else "倒锤头")
    if is_doji(bc):
        normal_signals.append("十字星")
    if is_large(bc) and yangc and lower_shadow_c < bc * 0.2 and upper_shadow_c < bc * 0.2:
        strong_signals.append("光脚大阳")
    elif is_large(bc) and yinc and lower_shadow_c < bc * 0.2 and upper_shadow_c < bc * 0.2:
        strong_signals.append("光头大阴")
    if yin1 and yangc and bc > b1 * 1.2 and curr['close'] > prev1['open'] and curr['open'] < prev1['close']:
        strong_signals.append("看涨吞没")
    if yang1 and yinc and bc > b1 * 1.2 and curr['close'] < prev1['open'] and curr['open'] > prev1['close']:
        strong_signals.append("看跌吞没")
    if (curr['high'] <= max(prev1['open'], prev1['close']) and curr['low'] >= min(prev1['open'], prev1['close'])):
        if is_doji(bc):
            strong_signals.append("十字孕线")
        elif yang1 and yinc:
            normal_signals.append("看跌孕线")
        elif yin1 and yangc:
            normal_signals.append("看涨孕线")
    if yang1 and yinc and gap_up and curr['close'] < prev1_mid and curr['open'] > prev1['close']:
        strong_signals.append("乌云盖顶")
    if yin1 and yangc and curr['open'] < prev1['low'] and curr['close'] > prev1_mid:
        strong_signals.append("刺透形态")
    if (yin2 and is_large(b2) and is_small(b1) and yangc and is_large(bc) and curr['close'] > prev2_mid):
        strong_signals.append("早晨之星 (见底)")
    if (yang2 and is_large(b2) and is_small(b1) and yinc and is_large(bc) and curr['close'] < prev2_mid):
        strong_signals.append("黄昏之星 (见顶)")
    final_list = []
    seen = set()
    for sig in strong_signals + normal_signals:
        if sig not in seen:
            final_list.append(sig)
            seen.add(sig)
            if len(final_list) >= 3:
                break
    if not final_list:
        if yangc:
            final_list.append("小阳线" if bc <= avg_body * 1.2 else "中阳线")
        elif yinc:
            final_list.append("小阴线" if bc <= avg_body * 1.2 else "中阴线")
        else:
            final_list.append("震荡")
    return " | ".join(final_list)

def check_macd_golden_cross(hist):
    if len(hist) < 35:
        return '无'
    ema12 = hist['close'].ewm(span=12, adjust=False).mean()
    ema26 = hist['close'].ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    cross = (dif.shift(1) < dea.shift(1)) & (dif > dea)
    if cross.tail(5).any():
        return '是（最近金叉）'
    return '否'

def calculate_vwap_cost(hist):
    if len(hist) < 20:
        return 0.0
    recent = hist.tail(20)
    vol_sum = recent['volume'].sum()
    if vol_sum < EPSILON:
        return round(recent['close'].iloc[-1], 2)
    vwap = (recent['close'] * recent['volume']).sum() / vol_sum
    return round(vwap, 2)

def calculate_profit_pct(close, avg_cost):
    if avg_cost <= EPSILON:
        return 0.0
    return round((close - avg_cost) / avg_cost * 100, 2)

def calculate_support_resistance(hist):
    if len(hist) < 10:
        return 0, 0, 0, 0
    return (round(hist['low'].tail(10).min(), 2), round(hist['high'].tail(10).max(), 2),
            round(hist['low'].tail(5).min(), 2), round(hist['high'].tail(5).max(), 2))

def check_valid_breakout(hist, code=""):
    if len(hist) < 10:
        return 0
    market_type = detect_market_type(code)
    min_pct = MARKET_CONFIG[market_type]['breakout_pct']
    df = hist.tail(60).copy()
    df['range'] = df['high'] - df['low']
    df['range'] = df['range'].replace(0, np.nan).fillna(EPSILON)
    df['close_pos'] = (df['close'] - df['low']) / df['range']
    df['vol_ma5'] = df['volume'].rolling(5).mean()
    df['pct'] = (df['close'] / df['open'] - 1) * 100
    valid = (df['pct'] >= min_pct) & (df['close_pos'] >= 0.85) & (df['volume'] > df['vol_ma5'] * 1.5)
    return int(valid.sum())

def calculate_chip_efficiency(hist, code=""):
    if len(hist) < 20:
        return 0.0
    market_type = detect_market_type(code)
    limit_pct = MARKET_CONFIG[market_type]['limit_pct']
    df = hist.tail(20).copy()
    df['is_up'] = df['close'] >= df['open']
    df['is_limit'] = (df['close'] / df['open'] - 1) >= limit_pct
    max_consec = 0
    current = 0
    for is_limit, is_up in zip(df['is_limit'], df['is_up']):
        if is_limit:
            current += 1
            max_consec = max(max_consec, current)
        elif not is_up:
            current = 0
    if max_consec >= 3:
        return 15.0
    up_vol = df[df['is_up']]['volume'].sum()
    down_vol = df[~df['is_up']]['volume'].sum()
    if down_vol < EPSILON:
        return 15.0 if up_vol > EPSILON else 0.0
    ratio = up_vol / down_vol
    score = 15 * (1 - np.exp(-ratio / 3))
    return round(min(score, 15.0), 2)

def calculate_obv_trend(hist):
    if len(hist) < 20:
        return '下降'
    close_change = hist['close'].diff().fillna(0)
    obv_change = np.where(close_change > 0, hist['volume'], np.where(close_change < 0, -hist['volume'], 0))
    obv = np.cumsum(obv_change)
    obv_5 = np.mean(obv[-5:])
    obv_15 = np.mean(obv[-15:])
    if obv_5 > obv_15 * 1.01:
        return '上升'
    elif obv_5 < obv_15 * 0.99:
        return '下降'
    return '震荡'

def check_ma_structure(hist):
    if len(hist) < 30:
        return False
    ma5 = hist['close'].rolling(5).mean()
    ma10 = hist['close'].rolling(10).mean()
    ma20 = hist['close'].rolling(20).mean()
    curr_ma5, curr_ma10, curr_ma20 = ma5.iloc[-1], ma10.iloc[-1], ma20.iloc[-1]
    close = hist['close'].iloc[-1]
    trend_up = curr_ma5 > ma5.iloc[-6] * 1.005 if len(ma5) >= 6 else False
    aligned = (curr_ma5 > curr_ma10 * 1.005) and (curr_ma10 > curr_ma20 * 1.005)
    price_ok = close > curr_ma5 * 0.99
    return aligned and price_ok and trend_up

def get_risk_control(hist, code=""):
    if len(hist) < 20:
        return 0.0
    market_type = detect_market_type(code)
    config = MARKET_CONFIG[market_type]
    h_l = hist['high'] - hist['low']
    h_c = abs(hist['high'] - hist['close'].shift())
    l_c = abs(hist['low'] - hist['close'].shift())
    tr = pd.concat([h_l, h_c, l_c], axis=1).max(axis=1)
    atr = tr.rolling(14).mean().iloc[-1]
    close = hist['close'].iloc[-1]
    if close < EPSILON:
        return 0.0
    low_20 = hist['low'].tail(20).min()
    daily_vol = atr / close
    atr_mult = config['atr_mult_high'] if daily_vol > 0.05 else config['atr_mult_mid'] if daily_vol > 0.03 else config['atr_mult_low']
    raw_stop = close - atr_mult * atr
    low_protection = low_20 * 0.95
    stop_loss = min(raw_stop, low_protection)
    stop_loss = max(stop_loss, close * (1 - config['max_drop']))
    stop_loss = min(stop_loss, close * 0.95)
    return round(max(stop_loss, 0), 2)

def calculate_relative_strength(hist, hs300_df):
    if len(hist) < 60 or hs300_df is None or len(hs300_df) < 60:
        return 0.0, 0
    hist_recent = hist.tail(60)
    hist_dates = set(hist_recent['date'])
    hs300_aligned = hs300_df[hs300_df['date'].isin(hist_dates)]
    if len(hs300_aligned) < 48:
        return 0.0, 0
    stock_ret = hist_recent['close'].iloc[-1] / hist_recent['close'].iloc[0]
    index_ret = hs300_aligned['close'].iloc[-1] / hs300_aligned['close'].iloc[0]
    rs = stock_ret / index_ret
    score = 15 if rs > 1.2 else 10 if rs > 1.1 else 5 if rs > 1.0 else 0 if rs > 0.9 else -5
    return round(rs, 3), score

def calculate_liquidity_score(hist):
    if 'turnover_rate' not in hist.columns or len(hist) < 20:
        return 0.0, 0
    avg_turn = hist['turnover_rate'].tail(20).mean()
    score = -15 if avg_turn < 0.8 else 0
    return round(avg_turn, 2), score

def calculate_risk_score(row):
    price = row['最新价']
    stop = row['ATR 止损位']
    if price <= EPSILON or stop <= EPSILON:
        return 0
    risk_ratio = (price - stop) / price
    if 0.05 <= risk_ratio <= 0.10:
        return 15
    elif 0.03 <= risk_ratio < 0.05:
        return 12
    elif 0.10 < risk_ratio <= 0.15:
        return 10
    elif risk_ratio > 0.15:
        return 5
    return 0

def calculate_smart_scores(row):
    n = row['大阳次数']
    s1 = 15 if n >= 3 else 10 if n >= 2 else 5 if n >= 1 else 0
    c = row['筹码效率分']
    s2 = 15 if c >= 15 else 10 if c >= 10 else 5 if c >= 5 else 0
    adx = row['ADX 趋势强度']
    s3 = 15 if adx > 25 else 10 if adx > 20 else 5 if adx > 15 else 0
    s4 = 15 if row['均线多头'] == '是' else 5
    obv_trend = row['OBV 趋势']
    s5 = 15 if obv_trend == '上升' else 10 if obv_trend == '震荡' else 5
    s6 = calculate_risk_score(row)
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
    return pd.Series([s1, s2, s3, s4, s5, s6, s7, liquidity_penalty, total, r, a])

def get_definition_sheet():
    data = {
        '指标名称': ['1. 大阳次数', '2. 筹码效率分', '3. ADX 趋势强度', '4. 均线多头', '5. ATR 止损位', '6. OBV 趋势', '7. 相对强度 RS', '8. 20 日均换手率%', '9. 启动得分', '10. 筹码得分', '11. 趋势得分', '12. 共振得分', '13. 资金得分', '14. 风控得分', '15. RS 得分', '16. 流动性扣分', '17. 总分', '18. 评级', '19. 操作建议'],
        '核心定义': ['60 天内有效大涨（市场自适应）', '涨时放量/跌时缩量（含连板奖励，最高15分）', '趋势强度（Wilder ADX严格SMMA）', 'MA5>MA10>MA20且向上', '动态ATR止损位（最严格保护）', '资金流向趋势（含震荡判断）', '相对沪深300的60日强度（日期对齐）', '流动性门槛（<0.8%扣15分）', '爆发力维度', '筹码锁定维度', '趋势纯度维度', '周期共振维度', '资金流向维度', '风险控制维度', '相对大盘强度（RS>1.2得15分）', '流动性惩罚', '综合评分（0-100分硬保护）', '等级划分', '仓位指导'],
        '计算公式': ['主板≥6%、科创≥8%、北交所≥10% + 收盘位置≥85% + 放量1.5倍', '涨跌量比 + 连续涨停≥3次得15分（上限15）', 'Wilder SMMA（初始简单平均，后续(N-1)/N平滑）', 'MA5>MA10*1.005 & MA10>MA20*1.005 & MA5向上', 'MIN(ATR止损, 20日低点*0.95) + 最大回撤下限', '5日OBV vs 15日OBV，±1%为震荡', '个股60日涨幅 / HS300 60日涨幅（实际交易日对齐）', '20日平均换手率', '≥3次=15, ≥2次=10, ≥1次=5', '≥15分=15, ≥10分=10, ≥5分=5', '>25=15, >20=10, >15=5', '是=15, 否=5', '上升=15, 震荡=10, 下降=5', '止损比例5-10%=15, 3-5%=12, 10-15%=10, >15%=5', 'RS>1.2=15, >1.1=10, >1.0=5, >0.9=0, ≤0.9=-5', '<0.8%扣15分', 'Sum(7项得分+扣分)，硬限制0-100', 'S≥85%, A≥75%, B≥65%, C≥50%, D<50%', '按评级执行仓位，D级严格止损'],
        '得分区间': ['0~15分', '0~15分（修复上限）', '0~15分', '5/15分', '自动计算', '5/10/15分', '-5~15分（修复对齐）', '-15~0分', '0~15分', '0~15分', '0~15分', '5/15分', '5~15分', '0~15分', '-5~15分', '-15~0分', '0~100分（硬保护）', 'S/A/B/C/D', '重仓/分批/轻仓/观望/排除'],
        '实盘意义': ['捕捉爆发力，过滤假突破', '判断主力筹码锁定度（上限15分防溢出）', '趋势纯度>25为强趋势股', '均线多头排列是中线持仓基础', '给出最严格止损价，保护本金', '资金是否持续流入', '避免弱市跟风股（日期对齐确保准确）', '剔除地量僵尸股', '爆发力越高越值得重仓', '筹码越集中越容易拉升', '趋势越纯越不容易被砸', '多因子共振胜率更高', '资金是股价的先行指标', '止损距离越小越安全', '相对大盘强势是核心选股逻辑（RS>1.2极强）', '流动性不足的股票风险极高', '综合得分越高越值得加仓（0-100硬保护）', 'S/A级立即行动，D级直接排除', '结合当前市场情绪执行仓位']
    }
    return pd.DataFrame(data)

# ================== 主程序 ==================
if __name__ == "__main__":
    print(f"🚀 启动 A股强势股扫描器 v7.9_MultiSource_Final_Stable（Lock保护版）...")
    END_DATE_STR = get_last_trade_day()
    print(f"📅 扫描基准日期：{END_DATE_STR}")
    hs300 = fetch_hs300_data(END_DATE_STR)
    spot_df = get_all_spot_data()
    input_path = None
    for p in POSSIBLE_INPUTS:
        if p.exists():
            input_path = p
            break
    if not input_path:
        print("❌ 错误：未找到输入文件！")
        sys.exit(1)
    print(f"📂 已找到输入文件：{input_path.name}")
    try:
        input_df = pd.read_excel(input_path, sheet_name=0)
    except Exception as e:
        print(f"❌ 错误：读取Excel失败 - {e}")
        sys.exit(1)
    code_col = next((c for c in input_df.columns if '代码' in str(c).lower() or 'code' in str(c).lower()), None)
    name_col = next((c for c in input_df.columns if '名称' in str(c).lower() or 'name' in str(c).lower()), None)
    if code_col is None:
        print("❌ 错误：输入文件中未找到“代码”列！")
        sys.exit(1)
    input_df.rename(columns={code_col: '股票代码', name_col or '股票名称': '股票名称'}, inplace=True)
    input_df['股票代码'] = input_df['股票代码'].astype(str).str.strip().str.upper()
    input_df = input_df.drop_duplicates(subset=['股票代码'])
    MY_STOCKS = input_df['股票代码'].tolist()
    name_dict = dict(zip(input_df['股票代码'], input_df['股票名称']))
    print(f"📊 共加载 {len(MY_STOCKS)} 只股票，开始扫描...\n")
    hist_dict, fetch_errors = batch_fetch_all_hist(MY_STOCKS, END_DATE_STR)
    errors = fetch_errors
    if not hist_dict:
        print("❌ 无有效数据")
        sys.exit(0)
    print(f"\n⚡ 开始计算技术指标（{len(hist_dict)} 只）...")
    results = []
    volume_records = []
    iterator = tqdm(hist_dict.items(), desc="计算指标", unit="只") if USE_TQDM else hist_dict.items()
    for code, hist in iterator:
        name = name_dict.get(code, "未知")
        if len(hist) < MIN_HIST_DAYS:
            errors.append(f"{code} ({name}): 数据不足{MIN_HIST_DAYS}天")
            continue
        try:
            df_pre = precompute_indicators(hist)
            today = fetch_today_quote(code, spot_df)
            avg_cost = calculate_vwap_cost(hist)
            profit_pct = calculate_profit_pct(today['今日收盘价'], avg_cost)
            short_sup, short_res, ultra_sup, ultra_res = calculate_support_resistance(hist)
            macd_cross = check_macd_golden_cross(hist)
            tech_patterns = detect_technical_patterns(df_pre)
            kline_patterns = detect_kline_patterns(hist)
            rs_value, rs_score = calculate_relative_strength(hist, hs300)
            avg_turnover, liquidity_penalty = calculate_liquidity_score(hist)
            stop_loss = get_risk_control(hist, code)
            last_close = hist['close'].iloc[-1]
            stop_distance_pct = round((last_close - stop_loss) / (last_close + EPSILON) * 100, 2) if stop_loss > 0 else 0
            row = {
                '股票代码': code, '股票名称': name, '最新价': round(last_close, 2),
                '今日涨跌幅%': today['今日涨跌幅'], '今日开盘价': today['今日开盘价'],
                '今日收盘价': today['今日收盘价'], '平均成本': avg_cost, '收盘获利%': profit_pct,
                '短线支撑位': short_sup, '短线压力位': short_res,
                '超短线支撑位': ultra_sup, '超短线压力位': ultra_res,
                'MACD 金叉': macd_cross, '技术形态': tech_patterns, 'K 线形态': kline_patterns,
                '大阳次数': check_valid_breakout(hist, code),
                '筹码效率分': calculate_chip_efficiency(hist, code),
                'ADX 趋势强度': calculate_adx(hist),
                '均线多头': '是' if check_ma_structure(hist) else '否',
                'ATR 止损位': stop_loss, '止损距离%': stop_distance_pct,
                'OBV 趋势': calculate_obv_trend(hist), '相对强度 RS': rs_value,
                'RS 得分': rs_score, '20 日均换手率%': avg_turnover,
                '流动性扣分': liquidity_penalty, '市场类型': detect_market_type(code)
            }
            results.append(row)
            if len(hist) >= 20:
                vol_ma20 = hist['volume'].tail(20).mean()
                today_vol = today.get('今日成交量', 0)
                volume_ratio = round(today_vol / (vol_ma20 + EPSILON), 2) if vol_ma20 > 0 else 0.0
                if volume_ratio >= 1.6 and today['今日涨跌幅'] > -2.0:
                    volume_records.append({
                        '股票代码': code, '股票名称': name, '最新价': row['最新价'],
                        '今日涨跌幅%': today['今日涨跌幅'], '今日成交量(手)': today_vol,
                        '20日均量(手)': round(vol_ma20), '放量倍数': volume_ratio,
                        'K 线形态': row['K 线形态'], 'ADX 趋势强度': row['ADX 趋势强度'],
                        'OBV 趋势': row['OBV 趋势']
                    })
        except Exception as e:
            errors.append(f"{code} ({name}): 计算异常 {str(e)}")
    print("\n" + "=" * 60)
    print(f"✅ 扫描完成！成功：{len(results)} 只 | 失败：{len(errors)} 只")
    if not results:
        print("⚠️ 警告：没有成功处理任何股票。")
        sys.exit(0)
    df = pd.DataFrame(results)
    scores = df.apply(calculate_smart_scores, axis=1)
    scores.columns = ['启动得分', '筹码得分', '趋势得分', '共振得分', '资金得分', '风控得分', 'RS 得分', '流动性扣分', '总分', '评级', '操作建议']
    df_final = pd.concat([df, scores], axis=1)
    rating_order = {'S 级 (极强)': 0, 'A 级 (强势)': 1, 'B 级 (观察)': 2, 'C 级 (弱势)': 3, 'D 级 (风险)': 4}
    df_final['评级排序'] = df_final['评级'].map(rating_order)
    df_final = df_final.sort_values(['评级排序', '总分'], ascending=[True, False]).drop('评级排序', axis=1)
    print("📈 生成今日放量Top20预筛选...")
    if volume_records:
        df_volume = pd.DataFrame(volume_records)
        df_volume = df_volume.merge(df_final[['股票代码', '总分', '评级']], on='股票代码', how='left')
        df_volume = df_volume.sort_values('放量倍数', ascending=False).head(20)
        df_volume.insert(0, '放量排名', range(1, len(df_volume) + 1))
        df_volume = df_volume[['放量排名', '股票代码', '股票名称', '最新价', '今日涨跌幅%', '今日成交量(手)', '20日均量(手)', '放量倍数', '总分', '评级', 'K 线形态', 'ADX 趋势强度']]
    else:
        df_volume = pd.DataFrame(columns=['放量排名', '股票代码', '股票名称', '最新价', '今日涨跌幅%', '今日成交量(手)', '20日均量(手)', '放量倍数', '总分', '评级', 'K 线形态', 'ADX 趋势强度'])
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    output_path = OUTPUT_DIR / f"自选强势股_v7.9_MultiSource_Final_Stable_{timestamp}.xlsx"
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df_final.to_excel(writer, sheet_name='股票扫描结果', index=False)
            df_volume.to_excel(writer, sheet_name='今日放量Top20', index=False)
            get_definition_sheet().to_excel(writer, sheet_name='指标完全解读手册', index=False)
            df_final['市场类型'].value_counts().to_frame('数量').to_excel(writer, sheet_name='市场统计')
            df_final['评级'].value_counts().to_frame('数量').to_excel(writer, sheet_name='评级分布')
            if errors:
                pd.DataFrame({'错误详情': errors}).to_excel(writer, sheet_name='错误记录', index=False)
        print(f"📊 结果已保存至：{output_path}")
        print(" 📌 新增Sheet：今日放量Top20（Stable版）")
        top_stocks = df_final.head(5)[['股票名称', '股票代码', '评级', '总分', 'K 线形态']]
        print("\n🏆 排名前5的强势股：")
        print(top_stocks.to_string(index=False))

        if not df_volume.empty:
            print("\n🔥 今日放量Top5预筛选：")
            print(df_volume.head(5)[['放量排名', '股票名称', '放量倍数', '今日涨跌幅%', '总分', '评级']].to_string(index=False))
        else:
            print("\n🔥 今日无明显放量股票（放量倍数≥1.6）")

    except Exception as e:
        print(f"❌ 保存文件失败：{e}")
        traceback.print_exc()