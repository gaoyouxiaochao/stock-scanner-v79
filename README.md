# stock-scanner-v79
A股强势股扫描器 v7.9（支持多数据源 + Baostock 回退）
# A股强势股扫描器 v7.9

一个稳定、高效的 **A股强势股扫描工具**，支持多数据源自动回退。

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![License](https://img.shields.io/badge/License-MIT-green)

## ✨ 主要功能

- **双数据源智能回退**：优先使用 AkShare，失败自动切换 Baostock
- **多板型适配**：主板 / 科创板 / 创业板 / 北交所 使用不同参数
- **智能评分系统**：输出 S/A/B/C/D 五级评级 + 操作建议
- **详细技术指标**：大阳次数、筹码效率、ADX趋势、OBV资金流、相对强度 RS 等
- **风险控制**：动态 ATR 止损位 + 最大回撤保护
- **输出报告**：生成完整 Excel（含指标解读手册、放量Top20、市场统计等）

## 📁 项目结构
