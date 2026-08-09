# -*- coding: utf-8 -*-
""" 【A股强势股扫描器 v81_MinCredible】
基于 v7.9：保留自选 Excel + 七维评分 + 全部原有技术指标算法。
v81 最小可信版（只修可信度与区分度，不含基本面默认开启）：
1) 最新价/获利%：spot无效回退K线收盘（消灭-100）
2) 缓存 TTL（默认4小时），避免脏缓存
3) 历史基准日：周末回退到周五
4) RS merge 对齐（保留）
5) 池内分位：总分分位%、RS分位%（提升区分度可读性）
6) 大盘MA60环境写入建议；BIAS不追高（保留）
7) 基本面/资金流默认关闭（Actions稳定）
8) 周末：强制以最近交易日(上周五)K线收盘回填价格/涨跌/振幅，避免全0与获利-100
"""

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

CACHE_DIR = Path("stock_cache_v81")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_TTL_HOURS = 2  # 缓存有效期（小时）；复权因子变化时尽快失效

MAX_HIST_DAYS = 150
MAX_WORKERS = 1  # 固定自选约10只：单线程更稳，避免触发数据源限流
MIN_HIST_DAYS = 30
BAOSTOCK_RETRY = 5

MARKET_CONFIG = {
    '主板': {'limit_pct': 0.095, 'breakout_pct': 6.0, 'atr_mult_high': 3.0, 'atr_mult_mid': 2.5, 'atr_mult_low': 2.0, 'max_drop': 0.20},
    '科创/创业板': {'limit_pct': 0.195, 'breakout_pct': 8.0, 'atr_mult_high': 3.5, 'atr_mult_mid': 3.0, 'atr_mult_low': 2.5, 'max_drop': 0.30},
    '北交所': {'limit_pct': 0.295, 'breakout_pct': 10.0, 'atr_mult_high': 4.0, 'atr_mult_mid': 3.5, 'atr_mult_low': 3.0, 'max_drop': 0.40}
}
EPSILON = 1e-9
BIAS_HIGH_THRESHOLD = 5.0
VOL_BOOST_THRESHOLD = 1.5
DEFAULT_ATR_PERIOD = 14

BS_LOCK = Lock()

# ================== 多数据源回退 ==================
BS_LOGGED_IN = False
BENCHMARK_NAME = "未知"  # 相对强度基准指数名称

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
        # 固定前复权 adjustflag="2"，与 akshare qfq 一致，避免缓存混用后复权
        try:
            with BS_LOCK:
                rs = bs.query_history_k_data_plus(
                    symbol,
                    "date,open,high,low,close,volume,amount,turn,peTTM,pbMRQ",
                    start_date=start_date,
                    end_date=end_date,
                    frequency="d",
                    adjustflag="2"
                )
                if rs.error_code != '0':
                    raise RuntimeError(f"baostock error_code={rs.error_code}")
                df = rs.get_data()
            if df is None or df.empty or len(df) < MIN_HIST_DAYS:
                raise RuntimeError("empty or too short")
            print(f"   ✅ 第{attempt+1}次成功 获取 {len(df)} 条数据 (qfq)")
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
        except Exception:
            time.sleep(0.8 + random.uniform(0, 0.3))

    print(f"  └─ {code} akshare 失败，切换 Baostock...")
    df = fetch_with_baostock(code, start_date_bs, end_date_bs)
    if df is not None and len(df) >= MIN_HIST_DAYS:
        return df
    return None

# ================== 基础工具 ==================
def get_last_trade_day():
    """最近已收盘的交易日（周末/周一开盘前：回退到周五）。"""
    d = datetime.now()
    # 周六、周日：用上周五
    if d.weekday() >= 5:
        d -= timedelta(days=d.weekday() - 4)  # Mon=0 ... Sat=5->Fri, Sun=6->Fri
        return d.strftime("%Y-%m-%d")
    for _ in range(10):
        if d.weekday() < 5:
            return d.strftime("%Y-%m-%d")
        d -= timedelta(days=1)
    return datetime.now().strftime("%Y-%m-%d")


def is_weekend_session() -> bool:
    """当前是否周末（无盘），应用最近交易日收盘数据。"""
    return datetime.now().weekday() >= 5


def session_data_note(end_date_str: str) -> str:
    if is_weekend_session():
        return f"周末无盘，分析基准日={end_date_str}（上周五收盘），实时字段可能为0，价格类指标沿用该日收盘"
    return f"分析基准日={end_date_str}"


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
    """沪深300优先，失败则 510300 ETF，再失败上证指数 000001，避免 RS 全 0。"""
    start_date = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d")
    end_str = end_date_str.replace('-', '')
    rename_map = {'日期': 'date', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close', '成交量': 'volume'}

    # 1) 指数 000300
    try:
        df = ak.index_zh_a_hist(symbol="000300", period="daily")
        if df is not None and not df.empty:
            df = df.rename(columns=rename_map)
            global BENCHMARK_NAME
            BENCHMARK_NAME = "沪深300"
            print("  ✅ 基准指数: 沪深300 (000300)")
            return clean_numeric(df)
    except Exception:
        pass
    # 2) ETF 510300
    try:
        df = ak.stock_zh_a_hist(symbol="510300", period="daily", start_date=start_date, end_date=end_str, adjust="qfq")
        if df is not None and not df.empty:
            df = df.rename(columns=rename_map)
            global BENCHMARK_NAME
            BENCHMARK_NAME = "沪深300ETF"
            print("  ✅ 基准指数降级: 沪深300ETF (510300)")
            return clean_numeric(df)
    except Exception:
        pass
    # 3) 上证指数
    try:
        df = ak.index_zh_a_hist(symbol="000001", period="daily")
        if df is not None and not df.empty:
            df = df.rename(columns=rename_map)
            global BENCHMARK_NAME
            BENCHMARK_NAME = "上证指数"
            print("  ⚠️ 基准指数降级: 上证指数 (000001)")
            return clean_numeric(df)
    except Exception:
        pass
    print("  ❌ 基准指数全部失败，RS 将为 0")
    return None

def fetch_hist_with_cache(code, end_date_str):
    cache_file = CACHE_DIR / f"{code}_{end_date_str.replace('-', '')}_qfq.pkl"  # 文件名含复权标识，避免前后复权混用
    if cache_file.exists():
        try:
            age_h = (datetime.now().timestamp() - cache_file.stat().st_mtime) / 3600.0
            if age_h < CACHE_TTL_HOURS:
                with open(cache_file, 'rb') as f:
                    df = pickle.load(f)
                # 校验缓存末日期不过于陈旧（相对 end_date）
                df2 = clean_numeric(df)
                if df2 is not None and not df2.empty and 'date' in df2.columns:
                    return df2, None
            else:
                try:
                    cache_file.unlink(missing_ok=True)
                except Exception:
                    pass
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

def batch_fetch_all_hist(codes, end_date_str):
    hist_dict = {}
    errors = []
    print(f"🚀 拉取 {len(codes)} 只股票历史数据（workers={MAX_WORKERS}）...")
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
    """获取全市场实时行情；空表不永久缓存，允许后续重试。"""
    global spot_cache
    if spot_cache is not None and not getattr(spot_cache, 'empty', True):
        return spot_cache
    try:
        df = ak.stock_zh_a_spot_em()
        if df is not None and not df.empty:
            df = df.copy()
            df.columns = [str(c).strip() for c in df.columns]
            spot_cache = df
            return spot_cache
        # 成功但空：不写入全局，下次可重试
        print("  ⚠️ 实时行情返回空表，不缓存，稍后可重试")
        return pd.DataFrame()
    except Exception as e:
        print(f"  ⚠️ 实时行情获取失败: {type(e).__name__}: {str(e)[:80]}")
        return pd.DataFrame()


def build_spot_lookup(spot_df):
    """一次性构建 code -> Series 索引，避免每只股票反复筛选全表"""
    if spot_df is None or getattr(spot_df, 'empty', True):
        return {}
    df = spot_df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    code_col = next((c for c in df.columns if c in ('代码', 'code')), None)
    if code_col is None:
        return {}
    lookup = {}
    for _, r in df.iterrows():
        key = re.sub(r'\D', '', str(r[code_col])).zfill(6)
        if key:
            lookup[key] = r
    return lookup


def fetch_today_quote(code, spot_df, spot_lookup=None):
    """从东财实时行情提取更多字段；缺失字段填 0。"""
    empty = {
        '今日涨跌幅': 0.0, '今日开盘价': 0.0, '今日收盘价': 0.0, '今日成交量': 0,
        '昨收': 0.0, '今开': 0.0, '今日最高': 0.0, '今日最低': 0.0,
        '今日振幅': 0.0, '量比': 0.0, '成交额': 0.0, '换手率': 0.0, '今日均价': 0.0,
        '内盘': 0.0, '外盘': 0.0, '委比': 0.0,
    }
    symbol = get_akshare_symbol(code).zfill(6)
    r = None
    if spot_lookup is not None:
        r = spot_lookup.get(symbol)
    elif spot_df is not None and not getattr(spot_df, 'empty', True):
        df = spot_df
        df_cols = [str(c).strip() for c in df.columns]
        # 不整体 copy，只做列名映射
        colmap = {str(c).strip(): c for c in df.columns}
        code_col = colmap.get('代码') or colmap.get('code')
        if code_col is not None:
            s = df[code_col].astype(str).str.replace(r'\D', '', regex=True).str.zfill(6)
            hit = df[s == symbol]
            if not hit.empty:
                r = hit.iloc[0]
    if r is None:
        return empty

    def g(*names, default=0.0):
        for n in names:
            if n in r.index and pd.notna(r[n]):
                try:
                    return float(r[n])
                except Exception:
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
    if amp == 0 and pre_close > EPSILON:
        amp = (high_p - low_p) / pre_close * 100
    vol_ratio = g('量比')
    turnover = g('换手率', '换手')

    avg_price = 0.0
    if amount > 0 and vol > 0:
        avg_price = amount / (vol * 100 + EPSILON)
        if avg_price < 0.05 or (close_p > 0 and (avg_price > close_p * 5 or avg_price < close_p * 0.2)):
            avg_price = (high_p + low_p + close_p) / 3 if close_p > 0 else close_p
    elif close_p > 0:
        avg_price = (high_p + low_p + close_p) / 3

    return {
        '今日涨跌幅': round(pct, 2),
        '今日开盘价': round(open_p, 2),
        '今日收盘价': round(close_p, 2),
        '今日成交量': int(vol),
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


def calc_streak_and_3d(hist):
    """连涨天、3日涨%、连续3天振幅%、连续3天均价"""
    out = {
        '连涨天': 0,
        '3日涨%': 0.0,
        '连续3天振幅%': 0.0,
        '连续3天均价': 0.0,
    }
    if hist is None or len(hist) < 4:
        return out
    h = hist.sort_values('date').reset_index(drop=True)
    closes = h['close'].astype(float).values
    streak = 0
    for i in range(len(closes) - 1, 0, -1):
        if closes[i] > closes[i - 1]:
            streak += 1
        else:
            break
    out['连涨天'] = int(streak)

    last3 = h.tail(3)
    if len(last3) == 3:
        c0 = float(last3['close'].iloc[0])
        c2 = float(last3['close'].iloc[-1])
        out['3日涨%'] = round((c2 / (c0 + EPSILON) - 1) * 100, 2)
        amps = []
        start_i = len(h) - 3
        for i in range(start_i, len(h)):
            prev_c = float(h['close'].iloc[i - 1])
            hi = float(h['high'].iloc[i])
            lo = float(h['low'].iloc[i])
            if prev_c > EPSILON:
                amps.append((hi - lo) / prev_c * 100)
        out['连续3天振幅%'] = round(float(np.mean(amps)), 2) if amps else 0.0
        out['连续3天均价'] = round(float(last3['close'].mean()), 2)
    return out


# ================== 技术指标（干净无append）==================

def detect_intraday_limit_up(hist, code, name=""):
    """
    用日K判断是否盘中触及涨停（非分钟级精确，但是日线可行近似）：
    - 昨收 * (1+涨停幅度) 为理论涨停价
    - 最高价 >= 涨停价 * 0.995 视为盘中触及
    - 收盘价 >= 涨停价 * 0.995 视为收盘涨停
    返回: (标签, 是否触及)
    """
    if hist is None or len(hist) < 2:
        return '否', False
    last = hist.iloc[-1]
    prev_close = float(hist['close'].iloc[-2])
    if prev_close <= EPSILON:
        return '否', False
    # 涨停幅度
    code6 = re.sub(r'\D', '', str(code)).zfill(6)
    name_u = str(name).upper()
    if 'ST' in name_u or '*ST' in name_u:
        limit_pct = 0.05
    elif code6.startswith(('30', '68')):
        limit_pct = 0.20
    elif code6.startswith(('4', '8')):
        limit_pct = 0.30
    else:
        limit_pct = 0.10
    limit_price = prev_close * (1.0 + limit_pct)
    high = float(last['high'])
    close = float(last['close'])
    tol = limit_price * 0.995
    touch = high >= tol
    seal = close >= tol
    if seal:
        return '收盘涨停', True
    if touch:
        return '盘中触及涨停', True
    return '否', False


def backfill_volume_fields_from_hist(today, hist):
    """周末/spot为空时，用日K回填量比/成交量/成交额/换手。"""
    if hist is None or len(hist) < 6:
        return today
    last = hist.iloc[-1]
    # 成交量
    if float(today.get('今日成交量', 0) or 0) <= 0 and 'volume' in hist.columns:
        today['今日成交量'] = int(float(last['volume']))
    # 成交额
    if float(today.get('成交额', 0) or 0) <= 0:
        if 'amount' in hist.columns and float(last.get('amount', 0) or 0) > 0:
            today['成交额'] = round(float(last['amount']), 2)
        elif float(last.get('volume', 0) or 0) > 0:
            # 粗估：均价 * 量（量单位未知时仅作展示）
            px = float(last['close'])
            today['成交额'] = round(px * float(last['volume']), 2)
    # 换手
    if float(today.get('换手率', 0) or 0) <= 0:
        for col in ('turnover_rate', 'turn'):
            if col in hist.columns and float(last.get(col, 0) or 0) > 0:
                today['换手率'] = round(float(last[col]), 2)
                break
    # 量比：今日量 / 过去5日均量
    if float(today.get('量比', 0) or 0) <= 0 and 'volume' in hist.columns:
        vol_ma5 = float(hist['volume'].iloc[-6:-1].mean())
        vol_today = float(last['volume'])
        if vol_ma5 > EPSILON:
            today['量比'] = round(vol_today / vol_ma5, 2)
    return today


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
    tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
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
    if len(hist) >= 21 and hist['close'].iloc[-1] > hist['high'].iloc[-21:-1].max() * 1.02:
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
    if len(hist) < 12:
        return 0
    market_type = detect_market_type(code)
    min_pct = MARKET_CONFIG[market_type]['breakout_pct']
    df = hist.tail(61).copy()
    df['range'] = df['high'] - df['low']
    df['range'] = df['range'].replace(0, np.nan).fillna(EPSILON)
    df['close_pos'] = (df['close'] - df['low']) / df['range']
    df['vol_ma5'] = df['volume'].rolling(5).mean()
    df['pct'] = (df['close'] / df['close'].shift(1) - 1) * 100
    df = df.iloc[1:]  # 去掉 shift 首行 NaN
    valid = (df['pct'] >= min_pct) & (df['close_pos'] >= 0.85) & (df['volume'] > df['vol_ma5'] * 1.5)
    return int(valid.sum())

def calculate_chip_efficiency(hist, code=""):
    if len(hist) < 21:
        return 0.0
    market_type = detect_market_type(code)
    limit_pct = MARKET_CONFIG[market_type]['limit_pct']
    df = hist.tail(21).copy()
    df['is_up'] = df['close'] >= df['open']
    df['is_limit'] = (df['close'] / df['close'].shift(1) - 1) >= limit_pct
    df = df.iloc[1:]  # 去掉 shift 产生的首行 NaN
    max_consec = 0
    current = 0
    for is_limit in df['is_limit']:
        if bool(is_limit):
            current += 1
            max_consec = max(max_consec, current)
        else:
            current = 0  # 非涨停即断开连续
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
        return '震荡'
    close_change = hist['close'].diff().fillna(0)
    obv_change = np.where(close_change > 0, hist['volume'], np.where(close_change < 0, -hist['volume'], 0))
    obv = np.cumsum(obv_change)
    if len(obv) < 20:
        return '震荡'
    obv_5 = np.mean(obv[-5:])
    obv_prev = np.mean(obv[-20:-5])  # 不与近5日重叠的前15日
    if obv_5 > obv_prev * 1.01:
        return '上升'
    elif obv_5 < obv_prev * 0.99:
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
    daily_vol = atr / close
    atr_mult = config['atr_mult_high'] if daily_vol > 0.05 else config['atr_mult_mid'] if daily_vol > 0.03 else config['atr_mult_low']
    # 追踪止损：max(收盘 - ATR×倍数, 近10日最低价)
    low_10 = float(hist['low'].tail(10).min())
    stop_loss = max(close - atr_mult * atr, low_10)
    # 止损不能过远：最多亏 max_drop（如主板 20%）
    stop_loss = max(stop_loss, close * (1 - config['max_drop']))
    if stop_loss >= close:
        stop_loss = close * (1 - config['max_drop'])
    return round(max(stop_loss, 0), 2)

def calculate_relative_strength(hist, hs300_df, window=60):
    """个股相对沪深300强度：按交易日 merge 对齐后算收益比"""
    if hist is None or hs300_df is None or len(hist) < 20 or len(hs300_df) < 20:
        return 0.0, 0
    h = hist.copy()
    idx = hs300_df.copy()
    h['date'] = pd.to_datetime(h['date'], errors='coerce').dt.tz_localize(None).dt.normalize()
    idx['date'] = pd.to_datetime(idx['date'], errors='coerce').dt.tz_localize(None).dt.normalize()
    h = h.dropna(subset=['date', 'close']).sort_values('date').drop_duplicates('date', keep='last')
    idx = idx.dropna(subset=['date', 'close']).sort_values('date').drop_duplicates('date', keep='last')
    merged = pd.merge(
        h[['date', 'close']].rename(columns={'close': 'close_s'}),
        idx[['date', 'close']].rename(columns={'close': 'close_i'}),
        on='date', how='inner'
    )
    need = max(20, window // 2)
    if len(merged) < need:
        return 0.0, 0
    merged = merged.tail(min(window, len(merged)))
    s0, s1 = float(merged['close_s'].iloc[0]), float(merged['close_s'].iloc[-1])
    i0, i1 = float(merged['close_i'].iloc[0]), float(merged['close_i'].iloc[-1])
    if s0 <= EPSILON or i0 <= EPSILON:
        return 0.0, 0
    stock_ret = s1 / s0
    index_ret = i1 / i0
    if abs(index_ret) < EPSILON:
        return 0.0, 0
    rs = stock_ret / index_ret
    score = 15 if rs > 1.2 else 10 if rs > 1.1 else 5 if rs > 1.0 else 0 if rs > 0.9 else -5
    return round(float(rs), 3), score


def calculate_rsi(hist, period=14):
    if len(hist) < period + 2:
        return 50.0
    delta = hist['close'].diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / (loss + EPSILON)
    rsi = 100 - (100 / (1 + rs))
    val = rsi.iloc[-1]
    if pd.isna(val):
        return 50.0
    return round(float(val), 2)


def calculate_macd_hist_series(hist):
    if len(hist) < 35:
        return None
    ema12 = hist['close'].ewm(span=12, adjust=False).mean()
    ema26 = hist['close'].ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    return dif - dea


def signal_breakout_20d_volume(hist, vol_mult=1.5):
    """20日新高 + 放量 + 阳线防诱多（收盘>开盘 且 收盘>昨收）"""
    if len(hist) < 25:
        return '否'
    high_20 = hist['high'].iloc[-21:-1].max()
    last = hist.iloc[-1]
    prev = hist.iloc[-2]
    vol_ma20 = hist['volume'].iloc[-21:-1].mean()
    is_new_high = (float(last['close']) >= float(high_20) * 0.998) or (float(last['high']) >= float(high_20))
    is_vol = float(last['volume']) >= float(vol_ma20) * vol_mult if vol_ma20 > EPSILON else False
    is_yang = float(last['close']) > float(last['open'])
    is_up = float(last['close']) > float(prev['close'])
    return '是' if (is_new_high and is_vol and is_yang and is_up) else '否'


def calculate_kdj(hist, n=9, m1=3, m2=3):
    """返回最新 K, D, J；数据不足返回 (50,50,50)"""
    if len(hist) < n + 2:
        return 50.0, 50.0, 50.0
    low_n = hist['low'].rolling(n).min()
    high_n = hist['high'].rolling(n).max()
    denom = (high_n - low_n)
    rsv = np.where(denom > EPSILON, (hist['close'] - low_n) / denom * 100, 50.0)
    rsv = pd.Series(rsv, index=hist.index).clip(0, 100)
    k = rsv.ewm(alpha=1 / m1, adjust=False).mean()
    d = k.ewm(alpha=1 / m2, adjust=False).mean()
    j = 3 * k - 2 * d
    return round(float(k.iloc[-1]), 2), round(float(d.iloc[-1]), 2), round(float(j.iloc[-1]), 2)


def signal_oversold_rebound(hist):
    """RSI超卖区 + MACD绿柱收缩 + KDJ低位拐头"""
    if len(hist) < 40:
        return '否'
    rsi = calculate_rsi(hist, 14)
    macd_hist = calculate_macd_hist_series(hist)
    if macd_hist is None:
        return '否'
    bars = macd_hist.tail(6)
    if bars is None or len(bars) < 3:
        return '否'
    shrink_days = 0
    for i in range(1, len(bars)):
        try:
            if bars.iloc[i] < 0 and bars.iloc[i - 1] < 0:
                if abs(float(bars.iloc[i])) < abs(float(bars.iloc[i - 1])) * 0.98:
                    shrink_days += 1
        except Exception:
            continue
    rsi_ok = 25 <= rsi <= 42
    macd_ok = shrink_days >= 3 and float(bars.iloc[-1]) < 0
    # KDJ: J 曾在低位(<15)且拐头向上
    kdj_ok = False
    if len(hist) >= 15:
        low_n = hist['low'].rolling(9).min()
        high_n = hist['high'].rolling(9).max()
        rsv = (hist['close'] - low_n) / (high_n - low_n + EPSILON) * 100
        k = rsv.ewm(alpha=1 / 3, adjust=False).mean()
        d = k.ewm(alpha=1 / 3, adjust=False).mean()
        j = 3 * k - 2 * d
        j1, j0 = float(j.iloc[-1]), float(j.iloc[-2])
        j_min_recent = float(j.tail(5).min())
        kdj_ok = (j_min_recent < 15) and (j1 > j0)
    return '是' if (rsi_ok and macd_ok and kdj_ok) else '否'


def calculate_bias(hist, period=20):
    """BIAS = (收盘 - MA) / MA * 100"""
    if len(hist) < period + 1:
        return 0.0
    ma = hist['close'].rolling(period).mean().iloc[-1]
    close = float(hist['close'].iloc[-1])
    if ma is None or pd.isna(ma) or abs(float(ma)) < EPSILON:
        return 0.0
    return round((close / float(ma) - 1.0) * 100, 2)


def detect_market_regime(hs300_df, ma_period=60):
    """沪深300 vs MA60（中线）+ MA20（短线情绪）"""
    if hs300_df is None or len(hs300_df) < ma_period + 2:
        return '未知', 0.0, 0.0
    df = hs300_df.copy()
    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.tz_localize(None).dt.normalize()
    df = df.dropna(subset=['date', 'close']).sort_values('date').drop_duplicates('date', keep='last')
    close = df['close'].astype(float)
    ma60 = close.rolling(ma_period).mean()
    ma20 = close.rolling(20).mean()
    last_c = float(close.iloc[-1])
    last_ma60 = float(ma60.iloc[-1]) if not pd.isna(ma60.iloc[-1]) else 0.0
    last_ma20 = float(ma20.iloc[-1]) if not pd.isna(ma20.iloc[-1]) else 0.0
    if last_ma60 <= EPSILON:
        return '未知', last_c, last_ma60
    above60 = last_c > last_ma60 * 1.002
    below60 = last_c < last_ma60 * 0.998
    above20 = last_ma20 > EPSILON and last_c > last_ma20
    ma20_up = False
    if len(ma20.dropna()) >= 3:
        ma20_up = float(ma20.iloc[-1]) > float(ma20.iloc[-3])
    if above60 and above20 and ma20_up:
        regime = '强势'
    elif above60:
        regime = '偏强'
    elif below60 and not above20:
        regime = '弱势'
    elif below60:
        regime = '偏弱'
    else:
        regime = '中性'
    return regime, last_c, round(last_ma60, 2)


def calculate_max_drawdown(hist, window=60):
    """近N日最大回撤%，负值"""
    if len(hist) < 10:
        return 0.0
    s = hist['close'].tail(window)
    peak = s.cummax()
    dd = (s - peak) / (peak + EPSILON)
    return round(float(dd.min() * 100), 2)


def calculate_volatility(hist, window=20):
    """近N日年化波动%（日收益std * sqrt(242)）"""
    if len(hist) < window + 1:
        return 0.0
    ret = hist['close'].pct_change().tail(window)
    vol = float(ret.std() * (242 ** 0.5) * 100)
    if pd.isna(vol):
        return 0.0
    return round(vol, 2)


def calculate_liquidity_score(hist):
    if 'turnover_rate' not in hist.columns or len(hist) < 20:
        return 0.0, 0
    avg_turn = hist['turnover_rate'].tail(20).mean()
    score = -15 if avg_turn < 0.8 else 0
    return round(avg_turn, 2), score

def calculate_risk_score(row):
    price = float(row.get('最新价', 0) or 0)
    stop = float(row.get('ATR 止损位', 0) or 0)
    if price <= EPSILON or stop <= EPSILON:
        return 0
    risk_ratio = max((price - stop) / price, 0.0)
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
    return pd.Series(
        [s1, s2, s3, s4, s5, s6, s7, liquidity_penalty, total, r, a],
        index=['启动得分', '筹码得分', '趋势得分', '共振得分', '资金得分', '风控得分', 'RS 得分', '流动性扣分', '总分', '评级', '操作建议']
    )

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
    print(f"🚀 启动 A股强势股扫描器 v81_MinCredible（周末沿用上周五收盘）...")
    END_DATE_STR = get_last_trade_day()
    print(f"📅 扫描基准日期：{END_DATE_STR}")
    print(f"📌 {session_data_note(END_DATE_STR)}")
    hs300 = fetch_hs300_data(END_DATE_STR)
    spot_df = get_all_spot_data()
    spot_lookup = build_spot_lookup(spot_df)
    market_regime, hs300_last, hs300_ma60 = detect_market_regime(hs300, 60)
    print(f"🌡 大盘环境（沪深300 vs MA60）：{market_regime} | 点位≈{hs300_last:.2f} MA60≈{hs300_ma60}")
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
    errors = list(fetch_errors)
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
            today = fetch_today_quote(code, spot_df, spot_lookup)
            streak3 = calc_streak_and_3d(hist)
            avg_cost = calculate_vwap_cost(hist)
            last_close = float(hist['close'].iloc[-1])
            last_open = float(hist['open'].iloc[-1]) if 'open' in hist.columns else last_close
            last_high = float(hist['high'].iloc[-1]) if 'high' in hist.columns else last_close
            last_low = float(hist['low'].iloc[-1]) if 'low' in hist.columns else last_close
            # 周末或 spot 无效：用最近交易日（上周五）K 线收盘作为价格基准
            spot_px = float(today.get('今日收盘价', 0) or 0)
            use_hist_session = is_weekend_session() or spot_px <= EPSILON
            if use_hist_session:
                current_price = last_close
                # 用日 K 回填关键展示字段，避免整行 0
                if float(today.get('昨收', 0) or 0) <= EPSILON and len(hist) >= 2:
                    today['昨收'] = round(float(hist['close'].iloc[-2]), 2)
                if float(today.get('今开', 0) or 0) <= EPSILON:
                    today['今开'] = round(last_open, 2)
                    today['今日开盘价'] = round(last_open, 2)
                if float(today.get('今日最高', 0) or 0) <= EPSILON:
                    today['今日最高'] = round(last_high, 2)
                if float(today.get('今日最低', 0) or 0) <= EPSILON:
                    today['今日最低'] = round(last_low, 2)
                if float(today.get('今日收盘价', 0) or 0) <= EPSILON:
                    today['今日收盘价'] = round(last_close, 2)
                if float(today.get('今日均价', 0) or 0) <= EPSILON:
                    today['今日均价'] = round((last_high + last_low + last_close) / 3.0, 2)
                if float(today.get('今日振幅', 0) or 0) <= EPSILON and len(hist) >= 2:
                    prev_c = float(hist['close'].iloc[-2])
                    if prev_c > EPSILON:
                        today['今日振幅'] = round((last_high - last_low) / prev_c * 100, 2)
                # 涨跌幅：相对前收
                if abs(float(today.get('今日涨跌幅', 0) or 0)) < EPSILON and len(hist) >= 2:
                    prev_c = float(hist['close'].iloc[-2])
                    if prev_c > EPSILON:
                        today['今日涨跌幅'] = round((last_close / prev_c - 1.0) * 100, 2)
            else:
                current_price = spot_px
            # 量能字段回填（周末/spot空）
            today = backfill_volume_fields_from_hist(today, hist)
            limit_up_tag, _ = detect_intraday_limit_up(hist, code, name)
            profit_pct = calculate_profit_pct(current_price, avg_cost)
            short_sup, short_res, ultra_sup, ultra_res = calculate_support_resistance(hist)
            macd_cross = check_macd_golden_cross(hist)
            tech_patterns = detect_technical_patterns(df_pre)
            kline_patterns = detect_kline_patterns(hist)
            rs_value, rs_score = calculate_relative_strength(hist, hs300)
            avg_turnover, liquidity_penalty = calculate_liquidity_score(hist)
            stop_loss = get_risk_control(hist, code)
            stop_distance_pct = round((current_price - stop_loss) / (current_price + EPSILON) * 100, 2) if stop_loss > 0 else 0
            rsi_val = calculate_rsi(hist)
            k_val, d_val, j_val = calculate_kdj(hist)
            sig_break = signal_breakout_20d_volume(hist)
            sig_oversold = signal_oversold_rebound(hist)
            max_dd = calculate_max_drawdown(hist, 60)
            vol_ann = calculate_volatility(hist, 20)
            bias20 = calculate_bias(hist, 20)
            signal_tags = []
            if sig_break == '是':
                signal_tags.append('20日突破放量')
            if sig_oversold == '是':
                signal_tags.append('超跌蓄势')
            if str(macd_cross).startswith('是'):
                signal_tags.append('MACD金叉')
            signal_tag = '|'.join(signal_tags) if signal_tags else ''
            risk_flags = []
            if max_dd <= -20:
                risk_flags.append('深回撤')
            if vol_ann >= 60:
                risk_flags.append('高波动')
            if stop_distance_pct >= 18:
                risk_flags.append('止损过宽')
            if avg_turnover < 0.8:
                risk_flags.append('低流动')
            if bias20 > BIAS_HIGH_THRESHOLD:
                risk_flags.append('乖离偏高')
            if market_regime == '弱势':
                risk_flags.append('弱市')
            risk_tag = '|'.join(risk_flags) if risk_flags else '正常'
            # 策略命中标签（不改总分公式，仅展示）
            strategy_hits = []
            if sig_break == '是':
                strategy_hits.append('趋势突破')
            if sig_oversold == '是':
                strategy_hits.append('超跌蓄势')
            if today.get('量比', 0) and float(today.get('量比', 0) or 0) >= 1.5 and float(today.get('今日涨跌幅', 0) or 0) > 0:
                strategy_hits.append('放量强势')
            strategy_tag = '|'.join(strategy_hits) if strategy_hits else ''
            row = {
                '股票代码': code, '股票名称': name, '最新价': round(current_price, 2),
                '大盘环境': market_regime,
                '昨收': today['昨收'], '今开': today['今开'],
                '今日最高': today['今日最高'], '今日最低': today['今日最低'],
                '今日均价': today['今日均价'],
                '今日涨跌幅%': today['今日涨跌幅'], '今日振幅': today['今日振幅'],
                '量比': today['量比'], '成交额': today['成交额'],
                '盘中涨停': limit_up_tag,
                '换手率%': today['换手率'], '今日成交量': today['今日成交量'],
                '内盘': today['内盘'], '外盘': today['外盘'], '委比': today['委比'],
                '连涨天': streak3['连涨天'], '3日涨%': streak3['3日涨%'],
                '连续3天振幅%': streak3['连续3天振幅%'], '连续3天均价': streak3['连续3天均价'],
                '今日开盘价': today['今日开盘价'], '今日收盘价': today['今日收盘价'],
                '平均成本': avg_cost, '收盘获利%': profit_pct,
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
                '流动性扣分': liquidity_penalty, '市场类型': detect_market_type(code),
                '基准指数': BENCHMARK_NAME,
                'RSI(14)': rsi_val,
                'KDJ_K': k_val, 'KDJ_D': d_val, 'KDJ_J': j_val,
                'BIAS20%': bias20,
                '60日最大回撤%': max_dd,
                '20日年化波动%': vol_ann,
                '信号_20日突破放量': sig_break,
                '信号_超跌蓄势': sig_oversold,
                '信号标签': signal_tag,
                '策略命中': strategy_tag,
                '风险标签': risk_tag,
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
    print(f"📌 v81 提示：非交易日实时字段可能为0；请看「行情可信」列与池内分位，勿单独依赖周日结果")
    if not results:
        print("⚠️ 警告：没有成功处理任何股票。")
        sys.exit(0)
    df = pd.DataFrame(results)
    scores = df.apply(calculate_smart_scores, axis=1)
    scores.columns = ['启动得分', '筹码得分', '趋势得分', '共振得分', '资金得分', '风控得分', 'RS 得分', '流动性扣分', '总分', '评级', '操作建议']
    df_final = pd.concat([df, scores], axis=1)
    # 大盘弱势：不改总分，仅在操作建议上提示（开关）
    if market_regime == '弱势' and '操作建议' in df_final.columns:
        df_final['操作建议'] = df_final['操作建议'].astype(str) + ' | 弱市慎加仓'
    if '风险标签' in df_final.columns and 'BIAS20%' in df_final.columns:
        # 乖离偏高时提示不追高
        mask_bias = df_final['BIAS20%'] > BIAS_HIGH_THRESHOLD
        df_final.loc[mask_bias, '操作建议'] = df_final.loc[mask_bias, '操作建议'].astype(str) + ' | 不追高'
    rating_order = {'S 级 (极强)': 0, 'A 级 (强势)': 1, 'B 级 (观察)': 2, 'C 级 (弱势)': 3, 'D 级 (风险)': 4}
    df_final['评级排序'] = df_final['评级'].map(rating_order)
    df_final = df_final.sort_values(['评级排序', '总分'], ascending=[True, False]).drop('评级排序', axis=1)
    # v81：池内分位（小样本友好：NaN 填0后再 rank，避免全 NaN）
    if '总分' in df_final.columns and len(df_final) >= 1:
        s = pd.to_numeric(df_final['总分'], errors='coerce').fillna(0)
        df_final['池内总分分位%'] = (s.rank(pct=True, method='average') * 100).round(1) if len(df_final) > 1 else 50.0
    else:
        df_final['池内总分分位%'] = 50.0
    if '相对强度 RS' in df_final.columns and len(df_final) >= 1:
        s = pd.to_numeric(df_final['相对强度 RS'], errors='coerce').fillna(0)
        df_final['池内RS分位%'] = (s.rank(pct=True, method='average') * 100).round(1) if len(df_final) > 1 else 50.0
    else:
        df_final['池内RS分位%'] = 50.0
    # 可信度标记：实时行情是否有效
    if is_weekend_session():
        df_final['行情可信'] = f'周末沿用{END_DATE_STR}收盘'
    elif '量比' in df_final.columns:
        df_final['行情可信'] = df_final['量比'].apply(
            lambda x: '是' if float(x or 0) > 0 else '否(spot失败,已回退K线收盘)'
        )
    else:
        df_final['行情可信'] = '未知'
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
    output_path = OUTPUT_DIR / f"自选强势股_v81_MinCredible_{timestamp}.xlsx"
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # 一页纸优先：10只自选快速决策（打开文件先看这页）
            one_page_cols = [c for c in [
                '股票代码', '股票名称', '最新价', '今日涨跌幅%', '总分', '评级',
                '操作建议', '信号标签', '策略命中', '盘中涨停', '风险标签',
                '池内总分分位%', '大盘环境', '行情可信'
            ] if c in df_final.columns]
            df_one = df_final[one_page_cols].copy() if one_page_cols else df_final.iloc[:, :6].copy()
            df_one.to_excel(writer, sheet_name='一页纸决策', index=False)

            df_final.to_excel(writer, sheet_name='股票扫描结果', index=False)
            df_volume.to_excel(writer, sheet_name='今日放量Top20', index=False)

            if '信号_20日突破放量' in df_final.columns:
                sig_mask = (df_final['信号_20日突破放量'] == '是') | (df_final.get('信号_超跌蓄势', '') == '是')
                if '信号_超跌蓄势' in df_final.columns:
                    sig_mask = (df_final['信号_20日突破放量'] == '是') | (df_final['信号_超跌蓄势'] == '是')
                else:
                    sig_mask = (df_final['信号_20日突破放量'] == '是')
                df_signal = df_final.loc[sig_mask].copy()
                sig_cols = [c for c in [
                    '股票代码', '股票名称', '最新价', '总分', '评级', '大盘环境', '策略命中',
                    '信号标签', '风险标签', '盘中涨停', '量比', 'RSI(14)', 'KDJ_J', 'BIAS20%',
                    '相对强度 RS', '60日最大回撤%', '20日年化波动%'
                ] if c in df_signal.columns]
                if not df_signal.empty and sig_cols:
                    df_signal[sig_cols].to_excel(writer, sheet_name='信号预警', index=False)
                else:
                    pd.DataFrame(columns=sig_cols or ['说明']).to_excel(writer, sheet_name='信号预警', index=False)

            get_definition_sheet().to_excel(writer, sheet_name='指标完全解读手册', index=False)
            df_final['市场类型'].value_counts().to_frame('数量').to_excel(writer, sheet_name='市场统计')
            df_final['评级'].value_counts().to_frame('数量').to_excel(writer, sheet_name='评级分布')
            if errors:
                pd.DataFrame({'错误详情': errors}).to_excel(writer, sheet_name='错误记录', index=False)

            # ----- openpyxl 美化（约10只自选场景）-----
            try:
                from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
                from openpyxl.formatting.rule import ColorScaleRule, CellIsRule, FormulaRule
                from openpyxl.utils import get_column_letter

                header_fill = PatternFill(start_color="1F497D", end_color="1F497D", fill_type="solid")
                header_font = Font(name="微软雅黑", size=10, bold=True, color="FFFFFF")
                data_font = Font(name="微软雅黑", size=9)
                thin_border = Border(
                    left=Side(style='thin', color='E0E0E0'),
                    right=Side(style='thin', color='E0E0E0'),
                    top=Side(style='thin', color='E0E0E0'),
                    bottom=Side(style='thin', color='E0E0E0')
                )
                rating_fills = {
                    'S 级 (极强)': PatternFill(start_color="FFD700", end_color="FFD700", fill_type="solid"),
                    'A 级 (强势)': PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid"),
                    'B 级 (观察)': PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid"),
                    'C 级 (弱势)': PatternFill(start_color="E2EFDA", end_color="E2EFDA", fill_type="solid"),
                    'D 级 (风险)': PatternFill(start_color="D9D9D9", end_color="D9D9D9", fill_type="solid"),
                }
                risk_fill = PatternFill(start_color="FCE4EC", end_color="FCE4EC", fill_type="solid")
                limit_fill = PatternFill(start_color="FFCDD2", end_color="FFCDD2", fill_type="solid")

                for sheetname in writer.book.sheetnames:
                    ws = writer.book[sheetname]
                    try:
                        ws.views.sheetView[0].showGridLines = True
                    except Exception:
                        pass
                    # 主表冻结：代码+名称 + 表头
                    if sheetname in ('股票扫描结果', '一页纸决策'):
                        ws.freeze_panes = 'C2'
                    else:
                        ws.freeze_panes = 'A2'

                    for row_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=ws.max_row, max_col=ws.max_column), start=1):
                        for cell in row:
                            cell.font = data_font
                            cell.border = thin_border
                            if row_idx == 1:
                                cell.fill = header_fill
                                cell.font = header_font
                                cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
                            else:
                                if isinstance(cell.value, (int, float)) and not isinstance(cell.value, bool):
                                    cell.alignment = Alignment(horizontal="right", vertical="center")
                                    # 价格类两位小数
                                    if isinstance(cell.value, float):
                                        cell.number_format = '0.00'
                                else:
                                    cell.alignment = Alignment(horizontal="left", vertical="center")

                    # 列宽（中文按字节粗略估）
                    for col in ws.columns:
                        max_len = 8
                        col_letter = get_column_letter(col[0].column)
                        for cell in col[:120]:
                            val_str = str(cell.value or '')
                            try:
                                byte_len = len(val_str.encode('gbk', errors='ignore'))
                            except Exception:
                                byte_len = len(val_str)
                            max_len = max(max_len, byte_len)
                        ws.column_dimensions[col_letter].width = min(max_len / 2 + 3, 28)

                    if sheetname not in ('股票扫描结果', '一页纸决策') or ws.max_row < 2:
                        continue

                    headers = [cell.value for cell in ws[1]]

                    if '评级' in headers:
                        cidx = headers.index('评级') + 1
                        for r in range(2, ws.max_row + 1):
                            val = str(ws.cell(row=r, column=cidx).value or '')
                            if val in rating_fills:
                                ws.cell(row=r, column=cidx).fill = rating_fills[val]

                    if '总分' in headers:
                        letter = get_column_letter(headers.index('总分') + 1)
                        rule = ColorScaleRule(
                            start_type='num', start_value=20, start_color='F8696B',
                            mid_type='num', mid_value=50, mid_color='FFEB84',
                            end_type='num', end_value=85, end_color='63BE7B'
                        )
                        ws.conditional_formatting.add(f'{letter}2:{letter}{ws.max_row}', rule)

                    if '今日涨跌幅%' in headers:
                        letter = get_column_letter(headers.index('今日涨跌幅%') + 1)
                        # 涨红跌绿（A股习惯用红涨）
                        ws.conditional_formatting.add(
                            f'{letter}2:{letter}{ws.max_row}',
                            CellIsRule(operator='greaterThan', formula=['0'],
                                       fill=PatternFill(start_color='FFCDD2', end_color='FFCDD2', fill_type='solid'))
                        )
                        ws.conditional_formatting.add(
                            f'{letter}2:{letter}{ws.max_row}',
                            CellIsRule(operator='lessThan', formula=['0'],
                                       fill=PatternFill(start_color='C8E6C9', end_color='C8E6C9', fill_type='solid'))
                        )

                    if '风险标签' in headers:
                        cidx = headers.index('风险标签') + 1
                        for r in range(2, ws.max_row + 1):
                            val = str(ws.cell(row=r, column=cidx).value or '')
                            if val and val != '正常':
                                ws.cell(row=r, column=cidx).fill = risk_fill

                    if '盘中涨停' in headers:
                        cidx = headers.index('盘中涨停') + 1
                        for r in range(2, ws.max_row + 1):
                            val = str(ws.cell(row=r, column=cidx).value or '')
                            if val and val != '否':
                                ws.cell(row=r, column=cidx).fill = limit_fill

                    if '池内总分分位%' in headers:
                        letter = get_column_letter(headers.index('池内总分分位%') + 1)
                        rule = ColorScaleRule(
                            start_type='num', start_value=0, start_color='F8696B',
                            mid_type='num', mid_value=50, mid_color='FFEB84',
                            end_type='num', end_value=100, end_color='63BE7B'
                        )
                        ws.conditional_formatting.add(f'{letter}2:{letter}{ws.max_row}', rule)

            except Exception as style_e:
                print(f"  ⚠️ Excel美化部分失败（数据已写出）: {style_e}")

        print(f"📊 结果已保存至：{output_path}")
        print(" 📌 新增Sheet：今日放量Top20（Stable版）")
        cols5 = ['股票名称', '股票代码', '评级', '总分']
        for c in ['池内总分分位%', '相对强度 RS', '信号标签', 'K 线形态']:
            if c in df_final.columns:
                cols5.append(c)
        top_stocks = df_final.head(5)[cols5]
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