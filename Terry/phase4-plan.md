# Phase 4 規劃文件 — Final Factory Prototype (V9.x)

> 建立日期：2026-06-06（自動排程產出）
> 建立者：Claude Code 排程規劃任務
> 基準：Phase 3 資產盤點完成後輸出

---

## Phase 3 遺產盤點

| 資產 | 路徑 | 狀態 | Phase 4 可重用性 |
|---|---|---|---|
| 台股訊號快照 | `backtest/backtest_report.json` | 201 檔訊號，**報酬欄全 null** | 高 — 框架已有，缺填充腳本 |
| 四維選股引擎 | `compound_model.py` | 手動打分，5 支股票示範 | 高 — 核心邏輯可擴充 |
| 美股觀察 MVP | `labs/2026-22/` | 28 檔、Task Scheduler 自動化 | 中 — 可作為台股版範本 |
| 情報掃描 Skill | `skills/pulse/` | 多平台掃描框架 | 中 — 需接台股輿情源 |
| CI 測試 | `test_radar.py` | 環境 + 複利模型測試 | 低 — 擴充測試覆蓋率 |
| 藍圖頁面 | `index.html` | Phase 4 標記進行中 | 參考 |

**關鍵缺口診斷：**
- 訊號框架完整，但從未計算過真實報酬（backtest_report 全 null）
- 選股邏輯停留在 5 支手動示範股，未接全市場掃描
- 輿情分析描述在藍圖，但沒有對應的台股輿情抓取程式
- `projects/` 完全空白 — Phase 3 沒有任何實驗晉升至孵化

---

## 三大主軸評估

### 主軸 A：全台股覆蓋

| 面向 | 現況落點 | 缺口 |
|---|---|---|
| 股票清單 | 201 檔（魚池 + ocean，Phase 3 手選） | 全市場 1,800+ 檔掃描缺席 |
| 數據源 | 無（backtest 訊號為手動輸入） | 台股 API 未整合 |
| 掃描自動化 | 無（美股 refresh.py 為 Yahoo Finance） | 台股版 refresh 腳本缺席 |
| 魚池邏輯 | 4 個魚池命名存在，進場條件未程式化 | 篩選規則文件化缺失 |

**可行第一步：**
1. 建立 `labs/2026-tw-scan/` — 以 labs/2026-22 為範本，對接台股 API
2. 整合 FinMind 或 TWSE 開放 API，抓取日線數據
3. 將 4 個魚池的篩選條件寫成 Python 函式（條件需 Terry 確認）

**依賴與風險：**
- **依賴**：台股數據源選型（FinMind 免費版有速率限制）
- **風險**：全市場掃描每日執行成本（時間 / API quota）

---

### 主軸 B：深度研究整合

| 面向 | 現況落點 | 缺口 |
|---|---|---|
| 情報掃描 | skills/pulse 框架完整（Reddit/HN/Web/X） | 未接台股特定來源（PTT 股板、Cmoney、鉅亨） |
| 題材分類 | 藍圖描述為已啟動，無對應程式 | 題材標籤邏輯缺失 |
| 自動化報告 | 無 | 掃描→結構化 Markdown 報告管線缺席 |
| 輿情與選股整合 | 無 | pulse 輸出未接入 compound_model.py |

**可行第一步：**
1. 建立台股輿情抓取模組：PTT 股版 + Cmoney 新聞 RSS
2. 設計題材標籤表（AI、半導體、航運、綠能等，Terry 初始化 15–20 個）
3. 以 Claude API 做自動摘要 → 輸出每日研究快報 Markdown

**依賴與風險：**
- **依賴**：Claude API 使用量（每日報告生成需估算 token 費用）
- **風險**：PTT 爬蟲穩定性；Cmoney 可能需要帳號

---

### 主軸 C：量化模型強化

| 面向 | 現況落點 | 缺口 |
|---|---|---|
| 訊號框架 | 201 筆訊號，格式完整 | 所有 return_5d/30d/126d 為 null |
| 回測引擎 | 結構在 backtest_report.json | 實際回填腳本缺席 |
| 四維評分 | SkillScore + StockScreener 可用 | 仍為主觀手動打分，未自動化 |
| 基準比較 | TWII 欄位在報告結構中 | TWII 數據未抓取 |

**可行第一步：**
1. 撰寫 `backtest/fill_returns.py`：讀取 signal_date + signal_price，從台股 API 取得 5d/30d/126d 後收盤，計算真實報酬
2. 以已有 4 個魚池計算勝率、超額報酬（對比 TWII）
3. 四維評分的部分指標自動化（Wayne 面/Miles 面可接財報 API）

**依賴與風險：**
- **依賴**：主軸 A 的台股 API 數據源（共用）
- **風險**：魚池訊號 signal_date 集中在 2026-05-12~13，持有期尚短，126d 報酬需等到 9 月才能計算

---

## 優先序評估（WSJF 概念）

| 項目 | 商業價值 | 時間緊迫 | 技術風險 | 工作量 | WSJF 分 | 建議順序 |
|---|---|---|---|---|---|---|
| C1. 回測報酬填充（fill_returns.py）| 高 | 高（訊號等待中）| 低 | 小 | ★★★★★ | **① 起手** |
| A1. 台股 API 整合（FinMind）| 高 | 高（A/C 共依賴）| 中 | 中 | ★★★★☆ | **② 並行** |
| A2. 台股掃描 Lab（labs/2026-tw-scan）| 高 | 中 | 低 | 中 | ★★★★☆ | ③ |
| B1. 台股輿情抓取模組 | 中 | 中 | 中 | 中 | ★★★☆☆ | ④ |
| B2. 每日研究快報管線 | 高 | 低 | 中 | 大 | ★★★☆☆ | ⑤ |
| C2. 四維評分自動化 | 高 | 低 | 高 | 大 | ★★☆☆☆ | ⑥ |
| B3. 題材標籤系統 | 中 | 低 | 低 | 小 | ★★★☆☆ | ④ 並行 |
| A3. 魚池條件程式化 | 高 | 中 | 低 | 小 | ★★★★☆ | ③ 並行 |

---

## 各主軸可執行項目（Phase 4 衝刺清單）

### 主軸 A — 全台股覆蓋

| # | 項目 | 輸出 | 負責 |
|---|---|---|---|
| A1 | 台股日線 API 選型與驗證（FinMind/TWSE） | api_test.py + 選型報告 | Wesley |
| A2 | 魚池篩選條件文件化（Terry 確認規則） | pools_logic.md | Terry |
| A3 | 台股掃描 Lab MVP（labs/2026-tw-scan） | scan.py + tw_dashboard.html | Wayne |
| A4 | 全市場日掃描排程（Task Scheduler） | register_tw_scan.ps1 | Wesley |
| A5 | 掃描結果寫入 JSON，供其他模組共用 | tw_quotes.json schema 定義 | Miles |

### 主軸 B — 深度研究整合

| # | 項目 | 輸出 | 負責 |
|---|---|---|---|
| B1 | 台股輿情數據源調研（PTT/Cmoney/鉅亨） | intel/tw_sources.md | Wayne |
| B2 | 題材標籤表初始化（15–20 個標籤） | intel/theme_tags.json | Terry |
| B3 | 輿情抓取腳本 MVP | labs/2026-research/crawl.py | Miles |
| B4 | Claude API 自動摘要串接 | labs/2026-research/summarize.py | Terry |
| B5 | 每日研究快報排程輸出 | reports/daily_YYYYMMDD.md | 全員 |

### 主軸 C — 量化模型強化

| # | 項目 | 輸出 | 負責 |
|---|---|---|---|
| C1 | fill_returns.py（回測報酬填充）| backtest/fill_returns.py | Wesley |
| C2 | TWII 基準數據抓取 | backtest/fetch_twii.py | Wayne |
| C3 | 魚池績效報告（勝率/超額報酬）| backtest/pool_performance.md | Miles |
| C4 | 四維評分部分自動化（Wayne 成長/Miles 基本面）| compound_model_v2.py | Wayne+Miles |
| C5 | 回測結果視覺化（整合進 index.html）| blueprint 更新 | Terry |

---

## 建議起手項目

**立即動手（本週）：**

```
C1 fill_returns.py
  → 訊號已在，價格歷史取得難度低
  → 執行完可立刻看到第一個真實績效數字
  → 是主軸 C 其他項目的前提

A1 台股 API 選型（FinMind 免費版驗證）
  → 主軸 A 和 C 的共同依賴
  → 需要 1–2 小時驗證，無需 Terry 決策
```

**第二週（確認 API 後）：**
- A2 魚池條件文件化（需 Terry 決策）
- A3 台股掃描 Lab MVP

---

## 風險矩陣

| 風險 | 發生可能 | 衝擊程度 | 緩解策略 |
|---|---|---|---|
| FinMind 免費版速率不足，全市場掃描受限 | 高 | 中 | 先用 ETL 批次（非即時），或升級付費版 |
| 訊號持有期太短（126d 需等至 9 月）| 確定 | 中 | 先跑 5d/30d 績效；9 月自動補算 126d |
| PTT 爬蟲被 ban | 中 | 低 | 備用：Cmoney RSS + 鉅亨 API |
| Claude API 成本超出預算 | 低 | 中 | 先用批次模式，限定每日摘要數量 |
| 魚池篩選條件難以程式化（規則模糊）| 中 | 高 | Terry 決策：先用啟發式規則，後期用 ML 學習 |
| projects/ 持續空白（無實驗晉升）| 中 | 低 | C1 + A3 通過驗證後直接晉升至 projects/ |

---

## 需要 Terry 決策的開放問題

| # | 問題 | 影響範圍 | 緊迫度 |
|---|---|---|---|
| Q1 | 台股數據源預算？FinMind 付費（約 $600 NTD/月）或維持免費版限速？ | 主軸 A、C 全部 | 高 |
| Q2 | 4 個魚池的進場條件是什麼？需要文件化才能程式化 | A2, A3 | 高 |
| Q3 | 深度研究快報的目標受眾？Terry 個人使用 or 未來對外輸出？ | 主軸 B 的規模設計 | 中 |
| Q4 | Phase 4 目標完成時間？（Phase 5 Pilot Run 預計何時啟動？）| 全體排序 | 中 |
| Q5 | 四維評分要維持主觀打分（靈活）還是強制自動化（可驗證）？ | C4 工作量 3x 差異 | 低 |

---

## 建議第一個動手的項目

**→ `backtest/fill_returns.py`**

理由：
- 訊號數據（201 筆）已就位，只差從 API 取歷史價格並計算報酬
- 完成後即可看到第一個真實績效數字，驗證魚池邏輯是否有效
- 不需要 Terry 決策任何開放問題即可啟動
- 執行時間預計 2–4 小時
- 成功後可直接晉升至 `projects/`，讓孵化區不再空白

---

*此文件由排程任務自動產出，供 Terry 審閱與決策使用。*
*建議 Terry 確認 Q1/Q2 後，授權 Wesley 立即啟動 C1。*
