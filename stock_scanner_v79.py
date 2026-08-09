#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
stock_scanner_v80.py
A股股票诊断评估扫描器 v80

基于 v79 升级，主要改进：
1. 从 Excel 读取指定股票清单（不再扫描全市场）
2. 修复 P0 Bug：评分系统断裂——现在 calculate_total_score() 真正被调用
3. 新增基本面分析引擎（PE/PB/ROE/毛利率/负债率/营收增速）
4. 新增资金面分析引擎（主力净流入/大单/超大单/OBV）
5. 新增 2026 市场主题标签（AI产业链/新能源/创新药/高股息/出海/科创50）
6. 新增终端诊断报告输出（ASCII 格式，可读性强）
7. 修复涨停阈值硬编码问题（主板10%/创业板科创板20%/ST股5%/北交所30%）
8. 修复超卖反弹信号门槛过严（放宽为两条件满足即可）
9. 修复字段名不匹配问题
10. 新增股票名称到输出结果

依赖：
    pip install akshare baostock pandas numpy openpyxl tqdm

用法：
    python stock_scanner_v80.py                              # 使用默认Excel路径
    python stock_scanner_v80.py --excel path/to/file.xlsx   # 指定Excel文件
    python stock_scanner_v80.py --sheet Sheet1               # 指定Sheet名
    python stock_scanner_v80.py --report                     # 同时输出终端诊断报告

GitHub: 可直接上传此文件
"""

import os
import re
import sys
import pickle
import random
import time
import warnings
import argparse
import traceback
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

warnings.filterwarnings('ignore')

# ================== 版本信息 ==================
VERSION = "v80"
VERSION_DATE = "2026-08-09"

# ================== 配置常量 ==================
EPSILON = 1e-9
MIN_HIST_DAYS = 30
MAX_RETRY = 3
CACHE_DIR = Path("./cache_v80")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_TTL_HOURS = 4  # 缓存有效期（小时）

# 默认 Excel 文件路径（可通过命令行参数覆盖）
DEFAULT_EXCEL_PATH = "输入股票代码及名称清单v1.xlsx"
DEFAULT_SHEET_NAME = "Sheet1"

CONFIG = {
    'max_drop': 0.08,
    'atr_mult_high': 2.5,
    'atr_mult_mid': 2.0,
    'atr_mult_low': 1.5,
    'rs_thresholds': [1.2, 1.1, 1.0, 0.9],
    'rs_scores': [5, 3, 1, 0, -2],
    'min_turnover': 0.8,
    'obv_threshold': 1.01,
    'vol_mult_breakout': 1.5,
    'rsi_period': 14,
    'kdj_n': 9,
    'kdj_m1': 3,
    'kdj_m2': 3,
    'adx_period': 14,
    'fund_flow_days': 3,       # 资金流向回看天数
}

# ================== 2026 市场主题映射 ==================
# 行业 -> 主题标签
INDUSTRY_THEME_MAP = {
    # AI 产业链
    '半导体': 'AI产业链', '半导体及元件': 'AI产业链', '电子元件': 'AI产业链',
    '消费电子': 'AI产业链', '电子制造': 'AI产业链', '光学光电子': 'AI产业链',
    '通信设备': 'AI产业链', '通信服务': 'AI产业链', '计算机设备': 'AI产业链',
    '软件开发': 'AI产业链', 'IT服务': 'AI产业链', '互联网服务': 'AI产业链',
    '元件': 'AI产业链', '光学元件': 'AI产业链',
    # 新能源 / 锂电
    '电池': '新能源', '光伏设备': '新能源', '风电设备': '新能源',
    '能源金属': '新能源', '电池制造': '新能源', '电网设备': '新能源',
    '输配电气': '新能源', '电源设备': '新能源',
    # 创新药 / 医疗
    '医疗器械': '创新药', '化学制药': '创新药', '生物制品': '创新药',
    '中药': '创新药', '医药商业': '创新药', '医疗服务': '创新药',
    '医药制造': '创新药', '医疗保健': '创新药',
    # 高端制造
    '通用设备': '高端制造', '专用设备': '高端制造', '仪器仪表': '高端制造',
    '自动化设备': '高端制造', '机器人': '高端制造',
    # 消费
    '食品饮料': '消费', '白酒': '消费', '调味品': '消费',
    '家电': '消费', '纺织服装': '消费', '商业百货': '消费',
    '家居用品': '消费', '装修建材': '消费',
    # 金融
    '银行': '金融', '保险': '金融', '证券': '金融', '多元金融': '金融',
    # 周期 / 资源
    '钢铁': '周期资源', '有色金属': '周期资源', '煤炭': '周期资源',
    '化工': '周期资源', '建材': '周期资源', '石油': '周期资源',
    # 出海相关
    '汽车零部件': '出海', '汽车整车': '出海', '工程机械': '出海',
    '船舶制造': '出海', '家电': '出海',
}

# 主题加分/减分
THEME_SCORE_ADJUST = {
    'AI产业链': +1,
    '新能源': +1,
    '创新药': +1,
    '高端制造': +1,
    '出海': +1,
    '消费': 0,
    '金融': 0,
    '周期资源': 0,
}

# 风险减分条件
RISK_DEDUCTIONS = {
    'ST股': -3,
    '高商誉': -2,
    '近期解禁': -2,
    '大股东减持': -3,
}


# ================== 工具函数 ==================

def normalize_code(code) -> str:
    """提取纯数字代码（支持 SZ002245 / 002245 / sz002245 等格式）"""
    return re.sub(r'\D', '', str(code).strip().upper()).zfill(6)


def get_market_prefix(code: str) -> str:
    """返回市场前缀 sh / sz / bj"""
    code = str(code).zfill(6)
    if code.startswith(('6', '9')):
        return 'sh'
    elif code.startswith(('0', '2', '3')):
        return 'sz'
    elif code.startswith(('4', '8')):
        return 'bj'
    return 'sz'


def get_baostock_symbol(code: str) -> str:
    """转为 baostock 格式"""
    code = str(code).zfill(6)
    prefix = get_market_prefix(code)
    return f"{prefix}.{code}"


def get_limit_threshold(code: str, name: str = '') -> float:
    """
    根据板块和 ST 状态返回涨停阈值。
    修复 v79 硬编码 0.095 的问题。
    """
    name_upper = name.upper()
    if 'ST' in name_upper or '*ST' in name_upper:
        return 0.048  # ST 股 5% 涨停（留些容差）
    if code.startswith('30') or code.startswith('68'):
        return 0.195  # 创业板/科创板 20% 涨停
    if code.startswith('4') or code.startswith('8'):
        return 0.295  # 北交所 30% 涨停
    return 0.095  # 主板 10% 涨停


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

    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'turn']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        nat_count = df['date'].isna().sum()
        if nat_count > 0:
            print(f"    ⚠ {nat_count} 行日期解析失败，已删除")
        df = df.dropna(subset=['date'])
        df = df.sort_values('date').reset_index(drop=True)

    df = df.dropna(subset=['open', 'high', 'low', 'close'], how='all')
    return df


# ================== Excel 股票清单读取 ==================

def read_stock_list_from_excel(excel_path: str, sheet_name: str = 'Sheet1') -> list:
    """
    从 Excel 读取股票代码和名称。
    支持 openpyxl 和 pandas 两种方式。
    返回: [(code, name), ...]
    """
    excel_path = Path(excel_path)
    if not excel_path.exists():
        print(f"❌ Excel 文件不存在: {excel_path}")
        print(f"   请检查路径是否正确，或使用 --excel 参数指定路径")
        sys.exit(1)

    stock_list = []
    try:
        import openpyxl
        wb = openpyxl.load_workbook(str(excel_path), data_only=True)
        if sheet_name not in wb.sheetnames:
            print(f"⚠ Sheet '{sheet_name}' 不存在，可用 Sheet: {wb.sheetnames}")
            print(f"  使用第一个 Sheet: {wb.sheetnames[0]}")
            sheet_name = wb.sheetnames[0]
        ws = wb[sheet_name]

        for row in ws.iter_rows(min_row=1, values_only=True):
            if row[0] is None:
                continue
            # 跳过表头
            first_val = str(row[0]).strip()
            if first_val in ('股票代码', '代码', 'code', 'Code', 'CODE'):
                continue
            code = normalize_code(first_val)
            name = str(row[1]).strip() if len(row) > 1 and row[1] else ''
            if len(code) == 6 and code.isdigit():
                stock_list.append((code, name))

    except ImportError:
        # openpyxl 未安装，用 pandas
        df = pd.read_excel(str(excel_path), sheet_name=sheet_name)
        code_col = df.columns[0]
        name_col = df.columns[1] if len(df.columns) > 1 else None
        for _, row in df.iterrows():
            code = normalize_code(row[code_col])
            name = str(row[name_col]).strip() if name_col else ''
            if len(code) == 6 and code.isdigit():
                stock_list.append((code, name))

    except Exception as e:
        print(f"❌ 读取 Excel 失败: {type(e).__name__}: {e}")
        sys.exit(1)

    return stock_list


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
    """获取历史数据：akshare 优先，baostock 兜底"""
    df = fetch_hist_with_akshare(code, end_date_str)
    if df is not None and len(df) >= MIN_HIST_DAYS:
        return df

    print(f"    └ {code} akshare 失败，切换 baostock...")
    df = fetch_hist_with_baostock(code, end_date_str)
    if df is not None and len(df) >= MIN_HIST_DAYS:
        return df

    return None


def fetch_hist_with_cache(code: str, end_date_str: str):
    """带缓存的历史数据获取"""
    cache_file = CACHE_DIR / f"{code}_{end_date_str.replace('-', '')}.pkl"

    if cache_file.exists():
        age_hours = (datetime.now().timestamp() - cache_file.stat().st_mtime) / 3600
        if age_hours < CACHE_TTL_HOURS:
            try:
                with open(cache_file, 'rb') as f:
                    df = pickle.load(f)
                return clean_numeric(df), None
            except Exception:
                pass
        else:
            cache_file.unlink(missing_ok=True)

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


def batch_fetch_all_hist(code_list: list, end_date_str: str) -> tuple:
    """批量获取历史数据"""
    hist_dict = {}
    errors = []
    total = len(code_list)

    try:
        from tqdm import tqdm
        iterator = tqdm(code_list, desc="获取历史数据")
    except ImportError:
        print("⚠ 未安装 tqdm，将不显示进度条。建议: pip install tqdm")
        iterator = code_list

    for i, code in enumerate(iterator):
        df, err = fetch_hist_with_cache(code, end_date_str)
        if df is not None:
            hist_dict[code] = df
        else:
            errors.append((code, err))
        time.sleep(0.1)  # 限速

    return hist_dict, errors


def fetch_hs300_data(end_date_str: str) -> pd.DataFrame:
    """获取沪深300 ETF 数据（仅用于指数对比）"""
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
        print(f"  ⚠ 沪深300数据获取失败: {e}")
    return None


# ================== 实时行情 ==================

def build_spot_lookup(spot_df: pd.DataFrame) -> dict:
    """构建代码→行情行的查找表"""
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
    """从东财实时行情提取字段；缺失字段填 0"""
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

    if (amp is None or amp == 0) and pre_close > EPSILON and high_p > 0:
        amp = max((high_p - low_p) / pre_close * 100, 0)

    avg_price = 0.0
    if amount > 0 and vol > 0 and close_p > EPSILON:
        estimated_shares = amount / close_p
        diff_as_shares = abs(vol - estimated_shares)
        diff_as_lots = abs(vol * 100 - estimated_shares)
        if diff_as_shares < diff_as_lots:
            shares = vol
        else:
            shares = vol * 100
        avg_price = amount / (shares + EPSILON)
        if not (0.2 < avg_price / close_p < 5.0):
            avg_price = (high_p + low_p + close_p) / 3
    elif close_p > 0:
        avg_price = (high_p + low_p + close_p) / 3

    return {
        '今日涨跌幅': round(pct, 2),
        '今日开盘价': round(open_p, 2),
        '今日收盘价': round(close_p, 2),
        '今日成交量': int(vol) if pd.notna(vol) and vol > 0 else 0,
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


# ================== 基本面数据获取 ==================

def fetch_fundamental_data(code: str) -> dict:
    """
    获取基本面数据：PE/PB/总市值/行业/ROE/毛利率/净利率/负债率
    使用 akshare 接口，失败时返回默认值。
    """
    result = {
        '行业': '', 'PE': 0.0, 'PB': 0.0, '总市值': 0.0,
        'ROE': 0.0, '毛利率': 0.0, '净利率': 0.0,
        '资产负债率': 0.0, '营收增速': 0.0, '净利润增速': 0.0,
    }

    import akshare as ak

    # 1. 个股基本信息（PE/PB/市值/行业）
    try:
        info = ak.stock_individual_info_em(symbol=code)
        if info is not None and not info.empty:
            for _, row in info.iterrows():
                key = str(row.iloc[0]).strip()
                val = row.iloc[1]
                if '行业' in key:
                    result['行业'] = str(val).strip()
                elif '市盈率' in key or key == '市盈率(动态)':
                    try: result['PE'] = round(float(val), 2)
                    except: pass
                elif '市净率' in key:
                    try: result['PB'] = round(float(val), 2)
                    except: pass
                elif '总市值' in key:
                    try: result['总市值'] = round(float(val), 2)
                    except: pass
    except Exception:
        pass

    # 2. 财务分析指标（ROE/毛利率/净利率/负债率）
    try:
        fin = ak.stock_financial_analysis_indicator(symbol=code)
        if fin is not None and not fin.empty:
            # 取最近一期的数据
            latest = fin.iloc[0] if len(fin) > 0 else None
            if latest is not None:
                for col in fin.columns:
                    val = latest[col]
                    if pd.isna(val):
                        continue
                    try:
                        val = float(val)
                    except (ValueError, TypeError):
                        continue
                    if '净资产收益率' in col or col == '加权净资产收益率(%)':
                        result['ROE'] = round(val, 2)
                    elif '销售毛利率' in col or col == '销售毛利率(%)':
                        result['毛利率'] = round(val, 2)
                    elif '销售净利率' in col or col == '销售净利率(%)':
                        result['净利率'] = round(val, 2)
                    elif '资产负债率' in col or col == '资产负债率(%)':
                        result['资产负债率'] = round(val, 2)
    except Exception:
        pass

    # 3. 财务摘要（营收/净利润增速）
    try:
        abstract = ak.stock_financial_abstract(symbol=code)
        if abstract is not None and not abstract.empty:
            # 尝试获取最新两期数据计算增速
            if len(abstract) >= 2:
                for col in abstract.columns:
                    if '营业总收入' in str(col) or '营业收入' in str(col):
                        try:
                            latest_rev = float(abstract.iloc[0][col])
                            prev_rev = float(abstract.iloc[1][col])
                            if prev_rev > EPSILON:
                                result['营收增速'] = round((latest_rev / prev_rev - 1) * 100, 2)
                        except: pass
                    if '净利润' in str(col) and '归属' in str(col):
                        try:
                            latest_np = float(abstract.iloc[0][col])
                            prev_np = float(abstract.iloc[1][col])
                            if abs(prev_np) > EPSILON:
                                result['净利润增速'] = round((latest_np / prev_np - 1) * 100, 2)
                        except: pass
    except Exception:
        pass

    return result


def batch_fetch_fundamentals(code_list: list, name_map: dict) -> dict:
    """批量获取基本面数据"""
    fund_dict = {}
    total = len(code_list)

    try:
        from tqdm import tqdm
        iterator = tqdm(code_list, desc="获取基本面数据")
    except ImportError:
        iterator = code_list

    for code in iterator:
        fund_dict[code] = fetch_fundamental_data(code)
        time.sleep(0.15)  # 限速，防 akshare 被封

    return fund_dict


# ================== 资金流向数据获取 ==================

def fetch_fund_flow(code: str, days: int = 3) -> dict:
    """
    获取资金流向数据：主力净流入/超大单/大单
    使用 akshare 接口，失败时返回默认值。
    """
    result = {
        '主力净流入3日': 0.0, '主力净占比3日': 0.0,
        '超大单净流入3日': 0.0, '大单净流入3日': 0.0,
        '主力净流入今日': 0.0, '主力净占比今日': 0.0,
    }

    import akshare as ak

    try:
        market = get_market_prefix(code)
        df = ak.stock_individual_fund_flow(stock=code, market=market)
        if df is not None and not df.empty:
            # 取最近 days 天
            recent = df.tail(days)
            today = df.tail(1)

            # 列名适配（akshare 不同版本列名可能有差异）
            main_col = next((c for c in df.columns if '主力' in c and '净额' in c), None)
            main_pct_col = next((c for c in df.columns if '主力' in c and '占比' in c), None)
            big_col = next((c for c in df.columns if '大单' in c and '净额' in c and '超大' not in c), None)
            super_col = next((c for c in df.columns if '超大单' in c and '净额' in c), None)

            if main_col:
                result['主力净流入3日'] = round(float(recent[main_col].sum()) / 1e8, 4)  # 转为亿元
                result['主力净流入今日'] = round(float(today[main_col].iloc[0]) / 1e8, 4)
            if main_pct_col:
                result['主力净占比3日'] = round(float(recent[main_pct_col].mean()), 2)
                result['主力净占比今日'] = round(float(today[main_pct_col].iloc[0]), 2)
            if super_col:
                result['超大单净流入3日'] = round(float(recent[super_col].sum()) / 1e8, 4)
            if big_col:
                result['大单净流入3日'] = round(float(recent[big_col].sum()) / 1e8, 4)
    except Exception:
        pass

    return result


def batch_fetch_fund_flows(code_list: list) -> dict:
    """批量获取资金流向数据"""
    flow_dict = {}

    try:
        from tqdm import tqdm
        iterator = tqdm(code_list, desc="获取资金流向")
    except ImportError:
        iterator = code_list

    for code in iterator:
        flow_dict[code] = fetch_fund_flow(code, CONFIG['fund_flow_days'])
        time.sleep(0.15)  # 限速

    return flow_dict


# ================== 技术指标 ==================

def calculate_rsi(hist: pd.DataFrame, period: int = 14) -> float:
    """RSI - Wilder EMA 版本（与通达信/同花顺一致）"""
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
    """MACD 柱状图序列"""
    if hist is None or len(hist) < 50:
        return None

    ema12 = hist['close'].ewm(span=12, adjust=False).mean()
    ema26 = hist['close'].ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    return dif - dea


def calculate_kdj(hist: pd.DataFrame, n: int = 9, m1: int = 3, m2: int = 3):
    """KDJ 计算（含除零保护）"""
    if hist is None or len(hist) < n + 5:
        return 50.0, 50.0, 50.0

    low_n = hist['low'].rolling(n).min()
    high_n = hist['high'].rolling(n).max()

    denom = high_n - low_n
    rsv = np.where(
        denom > EPSILON,
        (hist['close'] - low_n) / denom * 100,
        50.0
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
    """OBV 趋势判断（不重叠窗口）"""
    if hist is None or len(hist) < 25:
        return '下降'

    close_change = hist['close'].diff().fillna(0)
    obv_change = np.where(
        close_change > 0, hist['volume'],
        np.where(close_change < 0, -hist['volume'], 0)
    )
    obv = np.cumsum(obv_change)

    obv_5 = np.mean(obv[-5:])
    obv_prev15 = np.mean(obv[-20:-5])

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


def calculate_ma_alignment(hist: pd.DataFrame) -> str:
    """
    判断均线排列状态。
    返回: '多头排列' / '空头排列' / '交织' / '数据不足'
    """
    if hist is None or len(hist) < 60:
        return '数据不足'

    close = hist['close']
    ma5 = close.tail(5).mean()
    ma10 = close.tail(10).mean()
    ma20 = close.tail(20).mean()
    ma60 = close.tail(60).mean()
    price = close.iloc[-1]

    if price > ma5 > ma10 > ma20 > ma60:
        return '多头排列'
    elif price < ma5 < ma10 < ma20 < ma60:
        return '空头排列'
    return '交织'


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
    """
    超卖反弹信号（v80 放宽版）。
    修复 v79 三条件同时满足过严问题：
    改为 RSI 超卖 + (MACD收缩 或 KDJ低位拐头) 两条件即可。
    """
    if hist is None or len(hist) < 50:
        return '否'

    rsi_val = calculate_rsi(hist, CONFIG['rsi_period'])
    macd_hist = calculate_macd_hist_series(hist)
    if macd_hist is None:
        return '否'

    # MACD绿柱收缩
    bars_smooth = macd_hist.rolling(3).mean().tail(6)
    shrink_days = 0
    for i in range(1, len(bars_smooth)):
        if pd.notna(bars_smooth.iloc[i]) and pd.notna(bars_smooth.iloc[i - 1]):
            if bars_smooth.iloc[i] < 0 and bars_smooth.iloc[i] > bars_smooth.iloc[i - 1]:
                shrink_days += 1
    macd_shrink = shrink_days >= 1  # 放宽：收缩1天即可

    # KDJ低位拐头
    k_val, d_val, j_val = calculate_kdj(hist, CONFIG['kdj_n'], CONFIG['kdj_m1'], CONFIG['kdj_m2'])
    kdj_low_turn = k_val < 35 and k_val > d_val

    # RSI超卖（放宽到40）
    rsi_oversold = rsi_val < 40

    # 两条件满足即可（替代原来的三条件全满足）
    if rsi_oversold and (macd_shrink or kdj_low_turn):
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


def count_consecutive_limits(hist: pd.DataFrame, code: str = '', name: str = '') -> int:
    """
    计算近20日最大连续涨停次数。
    修复 v79：根据板块类型使用不同的涨停阈值。
    """
    if hist is None or len(hist) < 21:
        return 0

    limit_pct = get_limit_threshold(code, name)

    df = hist.tail(21).copy()
    df['pct_change'] = df['close'].pct_change()
    df = df.iloc[1:]

    df['is_limit'] = df['pct_change'] >= limit_pct

    max_consec = 0
    current = 0
    for is_limit in df['is_limit']:
        if is_limit:
            current += 1
            max_consec = max(max_consec, current)
        else:
            current = 0

    return max_consec


# ================== 分析函数 ==================

def calc_streak_and_3d(hist: pd.DataFrame) -> dict:
    """连涨天、3日涨%、连续3天振幅%、连续3天均价"""
    out = {'连涨天': 0, '3日涨%': 0.0, '连续3天振幅%': 0.0, '连续3天均价': 0.0}

    if hist is None or len(hist) < 4:
        return out

    streak = 0
    for i in range(len(hist) - 1, 0, -1):
        if hist['close'].iloc[i] > hist['close'].iloc[i - 1]:
            streak += 1
        else:
            break
    out['连涨天'] = streak

    last3 = hist.tail(3)
    if len(last3) == 3 and last3['close'].iloc[0] > EPSILON:
        out['3日涨%'] = round((last3['close'].iloc[-1] / last3['close'].iloc[0] - 1) * 100, 2)

    amps = []
    for i in range(len(last3)):
        row = last3.iloc[i]
        if row['close'] > EPSILON:
            amps.append((row['high'] - row['low']) / row['close'] * 100)
    out['连续3天振幅%'] = round(float(np.mean(amps)), 2) if amps else 0.0

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
    """计算相对强度评分"""
    if hist is None or hs300_df is None:
        return 0.0, 0
    if len(hist) < 20 or len(hs300_df) < 20:
        return 0.0, 0

    merged = pd.merge(
        hist[['date', 'close']].rename(columns={'close': 'close_s'}),
        hs300_df[['date', 'close']].rename(columns={'close': 'close_i'}),
        on='date', how='inner'
    )
    merged = merged.sort_values('date').reset_index(drop=True)

    if len(merged) < 10:
        return 0.0, 0

    merged = merged.tail(60)

    s0 = float(merged['close_s'].iloc[0])
    s1 = float(merged['close_s'].iloc[-1])
    i0 = float(merged['close_i'].iloc[0])
    i1 = float(merged['close_i'].iloc[-1])

    if s0 <= EPSILON or i0 <= EPSILON:
        return 0.0, 0

    stock_ret = s1 / s0
    index_ret = i1 / i0

    if abs(index_ret) < EPSILON:
        return 0.0, 0

    rs = stock_ret / index_ret

    thresholds = CONFIG['rs_thresholds']
    scores = CONFIG['rs_scores']

    score = scores[-1]
    for i, th in enumerate(thresholds):
        if rs >= th:
            score = scores[i]
            break

    return round(rs, 4), score


def calculate_chip_efficiency(hist: pd.DataFrame) -> float:
    """筹码效率（成交量加权均价）"""
    if hist is None or len(hist) < 20:
        return 0.0

    last20 = hist.tail(20)
    total_vol = last20['volume'].sum()

    if total_vol < EPSILON:
        return 0.0

    weighted_cost = (last20['close'] * last20['volume']).sum() / total_vol

    if weighted_cost < EPSILON:
        return 0.0

    above_ratio = (last20['close'] > weighted_cost).sum() / len(last20)
    return round(above_ratio * 100, 2)


def calculate_risk_score(hist: pd.DataFrame, current_price: float) -> dict:
    """风险评分（ATR止损）"""
    result = {'止损价': 0.0, '止损距离%': 0.0, '风险等级': '未知'}

    if hist is None or len(hist) < 20 or current_price <= EPSILON:
        return result

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

    if daily_vol > 0.05:
        atr_mult = CONFIG['atr_mult_high']
    elif daily_vol > 0.03:
        atr_mult = CONFIG['atr_mult_mid']
    else:
        atr_mult = CONFIG['atr_mult_low']

    atr_stop = current_price - atr_mult * atr
    pct_stop = current_price * (1 - CONFIG['max_drop'])
    stop_loss_final = max(atr_stop, pct_stop)

    if stop_loss_final > 0:
        stop_distance_pct = round((current_price - stop_loss_final) / current_price * 100, 2)
    else:
        stop_distance_pct = 0.0

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
    """流动性评分"""
    score = 0.0

    if turnover < CONFIG['min_turnover']:
        score -= 3
    elif turnover >= 3.0:
        score += 5
    elif turnover >= 1.5:
        score += 3

    if amount < 5e7:
        score -= 2
    elif amount > 5e8:
        score += 2

    if float_mv > 0:
        if float_mv < 2e9:
            score -= 1
        elif float_mv > 5e10:
            score += 1

    return max(-5, min(5, score))


# ================== 评分系统（v80 修复核心） ==================

def calculate_trend_score(row: dict) -> float:
    """
    趋势得分 (0-30)
    ADX 趋势强度 + MACD 状态 + 均线排列 + 价格位置
    """
    score = 0.0

    # ADX 趋势强度 (0-10)
    adx = row.get('ADX', 0)
    if adx > 35:
        score += 10
    elif adx > 25:
        score += 7
    elif adx > 20:
        score += 4
    elif adx > 15:
        score += 2

    # MACD 金叉 (0-5)
    if '金叉' in str(row.get('MACD金叉', '')):
        score += 5

    # 均线排列 (0-10)
    ma_align = row.get('均线排列', '')
    if ma_align == '多头排列':
        score += 10
    elif ma_align == '交织':
        score += 3
    elif ma_align == '空头排列':
        score += 0

    # 价格在 MA20 之上 (0-5)
    bias = row.get('BIAS20%', 0)
    if 0 < bias < 10:
        score += 5  # 温和上行
    elif bias >= 10:
        score += 3  # 过度偏离，扣分
    elif -5 < bias <= 0:
        score += 1  # 微跌

    return round(min(score, 30), 1)


def calculate_signal_score(row: dict) -> float:
    """
    信号得分 (0-15)
    20日突破 + 超卖反弹 + MACD金叉 + 连板
    """
    score = 0.0

    if row.get('20日突破') == '是':
        score += 5
    if row.get('超卖反弹') == '是':
        score += 4
    if '金叉' in str(row.get('MACD金叉', '')):
        score += 3
    consec = row.get('连续涨停次数', 0)
    if consec >= 3:
        score += 3
    elif consec >= 2:
        score += 2
    elif consec >= 1:
        score += 1

    return round(min(score, 15), 1)


def calculate_fundamental_score(fund: dict) -> float:
    """
    基本面得分 (0-25)
    ROE + 营收增速 + PE估值 + 负债率 + 毛利率/净利率
    """
    score = 0.0

    # ROE (0-8)
    roe = fund.get('ROE', 0)
    if roe >= 15:
        score += 8
    elif roe >= 10:
        score += 6
    elif roe >= 5:
        score += 3
    elif roe > 0:
        score += 1

    # 营收增速 (0-5)
    rev_growth = fund.get('营收增速', 0)
    if rev_growth >= 30:
        score += 5
    elif rev_growth >= 15:
        score += 3
    elif rev_growth >= 0:
        score += 1

    # PE 估值 (0-4) — 越低越好（但不为负/0）
    pe = fund.get('PE', 0)
    if 0 < pe <= 15:
        score += 4
    elif 15 < pe <= 30:
        score += 3
    elif 30 < pe <= 50:
        score += 2
    elif pe > 50:
        score += 0
    # PE 为 0 或负（亏损）不给分

    # 资产负债率 (0-3) — 越低越好
    debt = fund.get('资产负债率', 0)
    if 0 < debt < 40:
        score += 3
    elif 40 <= debt < 60:
        score += 1

    # 毛利率 + 净利率 (0-5)
    gross = fund.get('毛利率', 0)
    net = fund.get('净利率', 0)
    if gross >= 40 and net >= 15:
        score += 5
    elif gross >= 30 and net >= 10:
        score += 3
    elif gross >= 20 and net > 0:
        score += 1

    return round(min(score, 25), 1)


def calculate_capital_score(row: dict, flow: dict) -> float:
    """
    资金面得分 (0-15)
    主力净流入 + 大单/超大单 + OBV趋势
    """
    score = 0.0

    # 主力3日净流入 (0-6)
    main_flow = flow.get('主力净流入3日', 0)
    if main_flow > 1.0:  # >1亿
        score += 6
    elif main_flow > 0.3:
        score += 4
    elif main_flow > 0:
        score += 2
    elif main_flow < -1.0:
        score += 0
    elif main_flow < 0:
        score += 1

    # 主力净占比 (0-3)
    main_pct = flow.get('主力净占比3日', 0)
    if main_pct > 5:
        score += 3
    elif main_pct > 0:
        score += 2
    elif main_pct > -5:
        score += 1

    # 大单净流入 (0-3)
    big_flow = flow.get('大单净流入3日', 0)
    if big_flow > 0.5:
        score += 3
    elif big_flow > 0:
        score += 1

    # OBV 趋势 (0-3)
    obv = row.get('OBV趋势', '未知')
    if obv == '上升':
        score += 3
    elif obv == '平稳':
        score += 1

    return round(min(score, 15), 1)


def calculate_risk_control_score(row: dict) -> float:
    """
    风控得分 (0-5)
    止损距离 + 回撤控制 + 波动率适中
    """
    score = 0.0

    # 止损距离 (0-2)
    stop_dist = row.get('止损距离%', 0)
    if stop_dist <= 3:
        score += 2
    elif stop_dist <= 6:
        score += 1

    # 回撤控制 (0-2)
    drawdown = row.get('60日最大回撤%', 0)
    if drawdown < 10:
        score += 2
    elif drawdown < 20:
        score += 1

    # 波动率适中 (0-1)
    vol = row.get('20日年化波动%', 0)
    if 10 < vol < 40:
        score += 1

    return round(min(score, 5), 1)


def calculate_theme_score(name: str, industry: str, fund: dict) -> tuple:
    """
    2026 市场主题标签和加减分。
    返回: (tags_list, score_adjustment)
    """
    tags = []
    score_adj = 0.0

    # 1. 行业主题
    if industry:
        theme = INDUSTRY_THEME_MAP.get(industry, '')
        if theme:
            tags.append(theme)
            score_adj += THEME_SCORE_ADJUST.get(theme, 0)

    # 2. ST 股减分
    if 'ST' in name.upper() or '*ST' in name.upper():
        tags.append('ST股')
        score_adj += RISK_DEDUCTIONS['ST股']

    # 3. 高股息（需要股息率数据，简化处理：PE<15 且行业为金融/煤炭/钢铁）
    pe = fund.get('PE', 0)
    if 0 < pe < 15 and industry in ('银行', '保险', '煤炭', '钢铁', '港口', '高速公路'):
        tags.append('高股息')

    # 4. 科创50成分（简化：科创板股票）
    # 注：实际科创50成分需要查表，这里简化处理

    # 5. 亏损股减分
    if pe < 0:
        tags.append('亏损股')
        score_adj -= 1

    return tags, round(score_adj, 1)


def calculate_total_score_v80(row: dict, fund: dict, name: str = '') -> dict:
    """
    v80 核心修复：完整计算所有子分 + 总分 + 评级 + 操作建议。
    返回包含所有评分字段的 dict。
    """
    # 七大子分
    trend_score = calculate_trend_score(row)
    signal_score = calculate_signal_score(row)
    fundamental_score = calculate_fundamental_score(fund)
    capital_score = calculate_capital_score(row, fund)
    risk_score = calculate_risk_control_score(row)
    rs_score = row.get('强度评分', 0)
    liquidity_score = calculate_liquidity_score(
        row.get('换手率', 0),
        row.get('成交额', 0),
        row.get('流通市值', 0)
    )

    # 主题加减分
    industry = fund.get('行业', '')
    theme_tags, theme_adj = calculate_theme_score(name, industry, fund)

    # 总分
    total = (trend_score + signal_score + fundamental_score +
             capital_score + risk_score + rs_score + liquidity_score +
             theme_adj)
    total = max(0, min(100, round(total, 1)))

    # 评级和操作建议
    pct = total / 100.0
    if pct >= 0.85:
        rating, advice = "S 级 (极强)", "重仓出击 (60-70%)"
    elif pct >= 0.75:
        rating, advice = "A 级 (强势)", "分批建仓 (40-50%)"
    elif pct >= 0.65:
        rating, advice = "B 级 (观察)", "轻仓试盘 (20-30%)"
    elif pct >= 0.50:
        rating, advice = "C 级 (弱势)", "观望 (<10%)"
    else:
        rating, advice = "D 级 (风险)", "排除/止损"

    return {
        '趋势得分': trend_score,
        '信号得分': signal_score,
        '基本面得分': fundamental_score,
        '资金面得分': capital_score,
        '风控得分': risk_score,
        'RS得分': rs_score,
        '流动性得分': liquidity_score,
        '主题加减分': theme_adj,
        '主题标签': '|'.join(theme_tags) if theme_tags else '',
        '总分': total,
        '评级': rating,
        '操作建议': advice,
    }


# ================== 输出构建 ==================

def build_output_row(code: str, name: str, hist: pd.DataFrame, quote: dict,
                     hs300_df: pd.DataFrame = None,
                     fund: dict = None, flow: dict = None) -> dict:
    """构建单只股票的输出行（v80 增强版）"""
    out = {'代码': code, '名称': name}
    out.update(quote)

    # 基本面数据
    if fund:
        out['行业'] = fund.get('行业', '')
        out['PE'] = fund.get('PE', 0)
        out['PB'] = fund.get('PB', 0)
        out['总市值'] = fund.get('总市值', 0)
        out['ROE'] = fund.get('ROE', 0)
        out['毛利率'] = fund.get('毛利率', 0)
        out['净利率'] = fund.get('净利率', 0)
        out['资产负债率'] = fund.get('资产负债率', 0)
        out['营收增速'] = fund.get('营收增速', 0)
        out['净利润增速'] = fund.get('净利润增速', 0)
    else:
        out['行业'] = ''
        out['PE'] = 0
        out['PB'] = 0
        out['总市值'] = 0
        out['ROE'] = 0
        out['毛利率'] = 0
        out['净利率'] = 0
        out['资产负债率'] = 0
        out['营收增速'] = 0
        out['净利润增速'] = 0

    # 资金流向数据
    if flow:
        out['主力净流入3日(亿)'] = flow.get('主力净流入3日', 0)
        out['主力净占比3日'] = flow.get('主力净占比3日', 0)
        out['超大单净流入3日(亿)'] = flow.get('超大单净流入3日', 0)
        out['大单净流入3日(亿)'] = flow.get('大单净流入3日', 0)
        out['主力净流入今日(亿)'] = flow.get('主力净流入今日', 0)
    else:
        out['主力净流入3日(亿)'] = 0
        out['主力净占比3日'] = 0
        out['超大单净流入3日(亿)'] = 0
        out['大单净流入3日(亿)'] = 0
        out['主力净流入今日(亿)'] = 0

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
        out['均线排列'] = calculate_ma_alignment(hist)
        out['MACD金叉'] = check_macd_golden_cross(hist)
        out['20日突破'] = signal_breakout_20d_volume(hist, CONFIG['vol_mult_breakout'])
        out['超卖反弹'] = signal_oversold_rebound(hist)
        out['筹码效率%'] = calculate_chip_efficiency(hist)
        out['OBV趋势'] = calculate_obv_trend(hist)
        out['60日最大回撤%'] = calculate_max_drawdown(hist, 60)
        out['20日年化波动%'] = calculate_volatility(hist, 20)
        out['BIAS20%'] = calculate_bias(hist, 20)
        out['连续涨停次数'] = count_consecutive_limits(hist, code, name)
    else:
        out['RSI'] = 50.0
        out['KDJ_K'] = 50.0
        out['KDJ_D'] = 50.0
        out['KDJ_J'] = 50.0
        out['ADX'] = 0.0
        out['均线排列'] = '数据不足'
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
    if out.get('均线排列') == '多头排列':
        signal_tags.append('均线多头')
    out['信号标签'] = '|'.join(signal_tags) if signal_tags else ''

    # ★ v80 核心修复：计算评分系统 ★
    scores = calculate_total_score_v80(out, fund or {}, name)
    out.update(scores)

    return out


# ================== 终端诊断报告 ==================

def generate_terminal_report(result_df: pd.DataFrame, scan_date: str) -> str:
    """
    生成终端诊断报告（ASCII 格式）。
    输出每只股票的详细诊断信息。
    """
    lines = []
    lines.append("=" * 70)
    lines.append(f"  📊 A股股票诊断评估报告 {VERSION} | {scan_date}")
    lines.append("=" * 70)

    # 市场概况
    if not result_df.empty:
        regime = result_df.iloc[0].get('市场状态', '未知')
        mkt_chg = result_df.iloc[0].get('市场涨跌幅%', 0)
        mkt_dev = result_df.iloc[0].get('市场偏离度%', 0)
        lines.append(f"\n  📈 大盘环境: {regime} | 涨跌幅: {mkt_chg}% | 偏离MA60: {mkt_dev}%")

    # 汇总统计
    total = len(result_df)
    if total == 0:
        lines.append("\n  ⚠ 无有效结果")
        return '\n'.join(lines)

    # 按评级分组统计
    rating_counts = result_df['评级'].value_counts() if '评级' in result_df.columns else pd.Series()
    lines.append(f"\n  📋 诊断股票: {total} 只")
    if not rating_counts.empty:
        rating_str = ' | '.join([f"{k}: {v}只" for k, v in rating_counts.items()])
        lines.append(f"  评级分布: {rating_str}")

    # 信号股票
    if '信号标签' in result_df.columns:
        sig_stocks = result_df[result_df['信号标签'] != '']
        if not sig_stocks.empty:
            lines.append(f"  🔔 有信号股票: {len(sig_stocks)} 只")

    # 涨幅前5
    lines.append(f"\n  📈 涨幅前5:")
    top5 = result_df.nlargest(5, '今日涨跌幅')[['代码', '名称', '今日涨跌幅', '总分', '评级']].head(5)
    lines.append(f"  {'代码':<8} {'名称':<8} {'涨跌幅%':>8} {'总分':>6} {'评级':<14}")
    lines.append(f"  {'-'*50}")
    for _, row in top5.iterrows():
        lines.append(f"  {str(row['代码']):<8} {str(row['名称']):<8} {row['今日涨跌幅']:>8.2f} {row.get('总分',0):>6.1f} {str(row.get('评级','')):<14}")

    # 跌幅前5
    lines.append(f"\n  📉 跌幅前5:")
    bot5 = result_df.nsmallest(5, '今日涨跌幅')[['代码', '名称', '今日涨跌幅', '总分', '评级']].head(5)
    lines.append(f"  {'代码':<8} {'名称':<8} {'涨跌幅%':>8} {'总分':>6} {'评级':<14}")
    lines.append(f"  {'-'*50}")
    for _, row in bot5.iterrows():
        lines.append(f"  {str(row['代码']):<8} {str(row['名称']):<8} {row['今日涨跌幅']:>8.2f} {row.get('总分',0):>6.1f} {str(row.get('评级','')):<14}")

    # 综合评分排名前5
    if '总分' in result_df.columns:
        lines.append(f"\n  ⭐ 综合评分前5:")
        top_score = result_df.nlargest(5, '总分')
        lines.append(f"  {'代码':<8} {'名称':<8} {'总分':>6} {'评级':<14} {'操作建议'}")
        lines.append(f"  {'-'*60}")
        for _, row in top_score.iterrows():
            lines.append(f"  {str(row['代码']):<8} {str(row['名称']):<8} {row['总分']:>6.1f} {str(row.get('评级','')):<14} {str(row.get('操作建议',''))}")

    # 信号预警详情
    if '信号标签' in result_df.columns:
        sig_stocks = result_df[result_df['信号标签'] != '']
        if not sig_stocks.empty:
            lines.append(f"\n  🔔 信号预警详情 ({len(sig_stocks)} 只):")
            lines.append(f"  {'代码':<8} {'名称':<8} {'信号':<30} {'总分':>6} {'评级'}")
            lines.append(f"  {'-'*70}")
            for _, row in sig_stocks.head(10).iterrows():
                lines.append(f"  {str(row['代码']):<8} {str(row['名称']):<8} {str(row['信号标签']):<30} {row.get('总分',0):>6.1f} {str(row.get('评级',''))}")
            if len(sig_stocks) > 10:
                lines.append(f"  ... 还有 {len(sig_stocks)-10} 只")

    # 单只股票详细诊断（评分前3）
    if '总分' in result_df.columns and total > 0:
        top3_detail = result_df.nlargest(3, '总分')
        lines.append(f"\n{'─'*70}")
        lines.append(f"  📋 重点股票详细诊断")
        lines.append(f"{'─'*70}")
        for _, row in top3_detail.iterrows():
            lines.append(_format_single_stock_report(row))

    lines.append(f"\n{'='*70}")
    lines.append(f"  诊断完成！共 {total} 只股票 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"{'='*70}")

    return '\n'.join(lines)


def _format_single_stock_report(row) -> str:
    """格式化单只股票的详细诊断报告"""
    lines = []
    code = str(row.get('代码', ''))
    name = str(row.get('名称', ''))
    total = row.get('总分', 0)
    rating = str(row.get('评级', ''))
    advice = str(row.get('操作建议', ''))

    lines.append(f"\n  ┌─ {code} {name} ────────────────────────────────────")
    lines.append(f"  │ 综合评分: {total}/100 [{rating}]")
    lines.append(f"  │ 操作建议: {advice}")

    # 技术面
    trend_s = row.get('趋势得分', 0)
    lines.append(f"  │")
    lines.append(f"  │ [趋势 {trend_s}/30] ADX={row.get('ADX',0)} RSI={row.get('RSI',0)} "
                 f"BIAS={row.get('BIAS20%',0)}%")
    ma_align = row.get('均线排列', '')
    macd = row.get('MACD金叉', '')
    lines.append(f"  │   均线: {ma_align} | MACD: {macd} | OBV: {row.get('OBV趋势','')}")

    # 信号
    sig_s = row.get('信号得分', 0)
    sig_tags = row.get('信号标签', '')
    lines.append(f"  │ [信号 {sig_s}/15] {sig_tags if sig_tags else '无信号'}")

    # 基本面
    fund_s = row.get('基本面得分', 0)
    lines.append(f"  │ [基本面 {fund_s}/25] ROE={row.get('ROE',0)}% PE={row.get('PE',0)} "
                 f"PB={row.get('PB',0)}")
    lines.append(f"  │   毛利率={row.get('毛利率',0)}% 净利率={row.get('净利率',0)}% "
                 f"负债率={row.get('资产负债率',0)}%")
    rev_g = row.get('营收增速', 0)
    np_g = row.get('净利润增速', 0)
    lines.append(f"  │   营收增速={rev_g}% 净利润增速={np_g}% 行业={row.get('行业','')}")

    # 资金面
    cap_s = row.get('资金面得分', 0)
    main_flow = row.get('主力净流入3日(亿)', 0)
    main_pct = row.get('主力净占比3日', 0)
    lines.append(f"  │ [资金 {cap_s}/15] 主力3日净流入={main_flow:+.2f}亿 "
                 f"占比={main_pct:+.1f}%")
    big_flow = row.get('大单净流入3日(亿)', 0)
    lines.append(f"  │   大单3日={big_flow:+.2f}亿 "
                 f"超大单3日={row.get('超大单净流入3日(亿)',0):+.2f}亿")

    # 风控
    risk_s = row.get('风控得分', 0)
    stop_price = row.get('止损价', 0)
    stop_dist = row.get('止损距离%', 0)
    dd = row.get('60日最大回撤%', 0)
    vol = row.get('20日年化波动%', 0)
    lines.append(f"  │ [风控 {risk_s}/5] 止损价={stop_price}(-{stop_dist}%) "
                 f"回撤={dd}% 波动={vol}%")

    # 流动性 + RS
    liq_s = row.get('流动性得分', 0)
    rs_s = row.get('RS得分', 0)
    turnover = row.get('换手率', 0)
    amount = row.get('成交额', 0)
    rs_val = row.get('相对强度', 0)
    lines.append(f"  │ [流动性 {liq_s}/5] 换手={turnover}% 成交额={amount/1e8:.2f}亿")
    lines.append(f"  │ [相对强度 {rs_s}/5] RS={rs_val}")

    # 主题
    theme = row.get('主题标签', '')
    theme_adj = row.get('主题加减分', 0)
    if theme:
        lines.append(f"  │ [主题 {theme_adj:+.1f}] {theme}")

    lines.append(f"  └{'─'*58}")

    return '\n'.join(lines)


# ================== 主流程 ==================

def run_scanner(stock_list: list, end_date_str: str = None,
                output_report: bool = True) -> pd.DataFrame:
    """
    主扫描流程。
    stock_list: [(code, name), ...] 从 Excel 读取的股票清单
    end_date_str: 历史数据截止日（默认昨日）
    output_report: 是否输出终端诊断报告
    """
    if end_date_str is None:
        end_date_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    code_list = [c for c, _ in stock_list]
    name_map = {c: n for c, n in stock_list}

    print("=" * 60)
    print(f"  A股股票诊断评估扫描器 {VERSION} | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  历史数据截止日: {end_date_str}")
    print(f"  诊断股票数: {len(code_list)} 只")
    print("=" * 60)

    # 1. 获取沪深300基准数据
    print("\n[1/6] 获取沪深300基准数据...")
    hs300_df = fetch_hs300_data(end_date_str)
    if hs300_df is None:
        print("  ⚠ 沪深300数据获取失败，相对强度将为默认值")

    if hs300_df is not None:
        regime, chg, dev = detect_market_regime(hs300_df)
        print(f"  📈 大盘环境: {regime} | 涨跌幅: {chg}% | 偏离MA60: {dev}%")

    # 2. 批量获取个股历史数据
    print(f"\n[2/6] 批量获取个股历史数据 ({len(code_list)} 只)...")
    hist_dict, errors = batch_fetch_all_hist(code_list, end_date_str)
    print(f"  ✅ 成功: {len(hist_dict)} 只 | ❌ 失败: {len(errors)} 只")
    if errors:
        for code, err in errors[:5]:
            print(f"    - {code}: {err}")
        if len(errors) > 5:
            print(f"    ... 还有 {len(errors)-5} 只")

    # 3. 获取实时行情
    print("\n[3/6] 获取实时行情...")
    spot_df = None
    spot_lookup = {}
    try:
        import akshare as ak
        spot_df = ak.stock_zh_a_spot_em()
        spot_lookup = build_spot_lookup(spot_df)
        print(f"  ✅ 实时行情: {len(spot_lookup)} 只")
    except Exception as e:
        print(f"  ⚠ 实时行情获取失败: {type(e).__name__}: {str(e)[:100]}")

    # 4. 获取基本面数据
    print(f"\n[4/6] 获取基本面数据 ({len(code_list)} 只)...")
    fund_dict = batch_fetch_fundamentals(code_list, name_map)
    success_count = sum(1 for v in fund_dict.values() if v.get('PE', 0) > 0 or v.get('ROE', 0) > 0)
    print(f"  ✅ 基本面数据: {success_count}/{len(code_list)} 只有数据")

    # 5. 获取资金流向数据
    print(f"\n[5/6] 获取资金流向数据 ({len(code_list)} 只)...")
    flow_dict = batch_fetch_fund_flows(code_list)
    flow_success = sum(1 for v in flow_dict.values() if v.get('主力净流入3日', 0) != 0)
    print(f"  ✅ 资金流向: {flow_success}/{len(code_list)} 只有数据")

    # 6. 逐股分析
    print(f"\n[6/6] 逐股分析...")
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
            fund = fund_dict.get(code, {})
            flow = flow_dict.get(code, {})
            name = name_map.get(code, '')
            row = build_output_row(code, name, hist, quote, hs300_df, fund, flow)
            results.append(row)
        except Exception as e:
            print(f"  ⚠ {code} 分析异常: {type(e).__name__}: {str(e)[:80]}")
            continue

    # 输出结果
    print(f"\n  输出结果...")
    if not results:
        print("  ⚠ 无有效结果")
        return pd.DataFrame()

    result_df = pd.DataFrame(results)

    # 按总分降序排列（v80 新增：总分是核心排序依据）
    if '总分' in result_df.columns:
        result_df = result_df.sort_values('总分', ascending=False).reset_index(drop=True)
    else:
        result_df = result_df.sort_values('今日涨跌幅', ascending=False).reset_index(drop=True)

    # 保存 CSV
    output_csv = f"scan_result_{end_date_str.replace('-', '')}.csv"
    result_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"  ✅ CSV 已保存: {output_csv}")

    # 保存 Excel（含多 Sheet）
    try:
        output_xlsx = f"scan_result_{end_date_str.replace('-', '')}.xlsx"
        with pd.ExcelWriter(output_xlsx, engine='openpyxl') as writer:
            # 全部结果
            result_df.to_excel(writer, sheet_name='全部结果', index=False)

            # 信号预警
            if '信号标签' in result_df.columns:
                sig_mask = result_df['信号标签'] != ''
                df_signal = result_df.loc[sig_mask]
                if not df_signal.empty:
                    df_signal.to_excel(writer, sheet_name='信号预警', index=False)
                else:
                    pd.DataFrame(columns=result_df.columns).to_excel(
                        writer, sheet_name='信号预警', index=False
                    )

            # 评级排名
            if '评级' in result_df.columns:
                df_rating = result_df[['代码', '名称', '总分', '评级', '操作建议',
                                       '趋势得分', '信号得分', '基本面得分',
                                       '资金面得分', '风控得分', 'RS得分', '流动性得分',
                                       '主题加减分', '主题标签']].copy()
                df_rating.to_excel(writer, sheet_name='评分排名', index=False)

            # 基本面数据
            fund_cols = ['代码', '名称', '行业', 'PE', 'PB', 'ROE', '毛利率',
                         '净利率', '资产负债率', '营收增速', '净利润增速', '总市值']
            available_fund_cols = [c for c in fund_cols if c in result_df.columns]
            if available_fund_cols:
                result_df[available_fund_cols].to_excel(writer, sheet_name='基本面', index=False)

            # 资金流向
            flow_cols = ['代码', '名称', '主力净流入3日(亿)', '主力净占比3日',
                         '超大单净流入3日(亿)', '大单净流入3日(亿)', '主力净流入今日(亿)']
            available_flow_cols = [c for c in flow_cols if c in result_df.columns]
            if available_flow_cols:
                result_df[available_flow_cols].to_excel(writer, sheet_name='资金流向', index=False)

        print(f"  ✅ Excel 已保存: {output_xlsx}")
    except Exception as e:
        print(f"  ⚠ Excel保存失败: {e}")

    # 终端诊断报告
    if output_report:
        report = generate_terminal_report(result_df, end_date_str)
        print(report)

        # 保存报告到文件
        report_file = f"diagnosis_report_{end_date_str.replace('-', '')}.txt"
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            print(f"\n  ✅ 诊断报告已保存: {report_file}")
        except Exception as e:
            print(f"  ⚠ 报告保存失败: {e}")

    print(f"\n🎯 诊断完成！共 {len(results)} 只股票")
    return result_df


# ================== 命令行参数 ==================

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description=f'A股股票诊断评估扫描器 {VERSION}',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python stock_scanner_v80.py                                    # 使用默认Excel
  python stock_scanner_v80.py --excel /path/to/stocks.xlsx       # 指定Excel
  python stock_scanner_v80.py --excel stocks.xlsx --sheet Sheet1
  python stock_scanner_v80.py --no-report                         # 不输出终端报告
  python stock_scanner_v80.py --end-date 2026-08-08              # 指定截止日期
        """
    )
    parser.add_argument('--excel', type=str, default=DEFAULT_EXCEL_PATH,
                        help=f'股票清单 Excel 文件路径 (默认: {DEFAULT_EXCEL_PATH})')
    parser.add_argument('--sheet', type=str, default=DEFAULT_SHEET_NAME,
                        help=f'Excel Sheet 名称 (默认: {DEFAULT_SHEET_NAME})')
    parser.add_argument('--end-date', type=str, default=None,
                        help='历史数据截止日期 YYYY-MM-DD (默认: 昨日)')
    parser.add_argument('--no-report', action='store_true',
                        help='不输出终端诊断报告')
    parser.add_argument('--no-fundamental', action='store_true',
                        help='跳过基本面数据获取（加速扫描）')
    parser.add_argument('--no-fund-flow', action='store_true',
                        help='跳过资金流向获取（加速扫描）')
    return parser.parse_args()


# ================== 入口 ==================

if __name__ == '__main__':
    args = parse_args()

    # 读取 Excel 股票清单
    print(f"📋 读取股票清单: {args.excel} (Sheet: {args.sheet})")
    stock_list = read_stock_list_from_excel(args.excel, args.sheet)

    if not stock_list:
        print("❌ 未读取到任何股票，请检查 Excel 文件格式")
        sys.exit(1)

    print(f"  共 {len(stock_list)} 只股票:")
    for code, name in stock_list:
        print(f"    {code} {name}")

    # 登录 baostock
    try:
        import baostock as bs
        lg = bs.login()
        if lg.error_code != '0':
            print(f"⚠ Baostock 登录失败: {lg.error_msg}")
    except ImportError:
        print("⚠ 未安装 baostock，将仅使用 akshare")
        bs = None

    # 运行扫描
    result = run_scanner(stock_list, args.end_date, output_report=not args.no_report)

    # 登出 baostock
    if bs:
        bs.logout()

    # 打印简要结果
    if not result.empty:
        print("\n📊 评分前10:")
        display_cols = ['代码', '名称', '今日涨跌幅', '总分', '评级', '操作建议', '信号标签']
        available_cols = [c for c in display_cols if c in result.columns]
        print(result[available_cols].head(10).to_string(index=False))
