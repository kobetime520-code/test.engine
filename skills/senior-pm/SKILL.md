---
name: senior-pm
description: "資深 PM 全套工具：專案健康儀表板、風險矩陣、資源容量規劃、WSJF/RICE 優先排序、EMV 風險計算。觸發條件：'專案健康'、'里程碑追蹤'、'排優先順序'、'資源分配'、'專案卡住了'、'Lab 孵化進度'。"
owner: Miles（首席創造長）
source: https://github.com/alirezarezvani/claude-skills/tree/main/project-management/skills/senior-pm
installed: 2026-05-25
---

# Senior PM — 資深專案管理工具包

> Miles 用於 Magic Lab 實驗孵化全生命週期管理：從 Lab 驗證 → 正式 Project → 上線追蹤。

「沒有被測量的東西就無法被改善。沒有風險管理的計畫只是一個願望清單。」

---

## 三種進入模式

### Mode 1 — 組合健康儀表板（多專案同步追蹤）

1. **健康評分** — 對每個 Lab / Project 計算 RAG 狀態
2. **三層分析**：
   - 進度健康（計畫 vs 實際里程碑）
   - 風險暴露（已識別風險 × 發生機率 × 影響）
   - 資源容量（目前使用率 vs 目標 70-85%）
3. **輸出** — 一頁式儀表板：所有專案 RAG + 需立即處理的前 3 項風險

### Mode 2 — 優先排序決策（哪個先做）

選擇框架（Miles 依場景選用）：

| 框架 | 公式 | 適用場景 |
|---|---|---|
| **WSJF** | (業務價值 + 時效性 + 降風) ÷ 工作規模 | Lab 孵化排程決策 |
| **RICE** | (觸及 × 影響 × 信心) ÷ 工作量 | 新功能優先順序 |
| **ICE** | Impact × Confidence × Ease | 快速粗排（決策 < 10 分鐘） |
| **MoSCoW** | Must / Should / Could / Won't | 範疇鎖定、需求拆解 |

使用方式：「用 RICE 幫我排這 5 個 Lab 的優先順序」→ 輸入各 Lab 的 R/I/C/E 數值 → 產出排序表

### Mode 3 — 風險矩陣 + EMV 計算

1. **識別風險** — 列出每個專案前 5 大風險
2. **EMV 計算**：預期貨幣價值 = 機率 × 影響金額
3. **緩解策略** — 每個 RED 風險對應具體行動
4. **Monte Carlo（選用）** — 多場景模擬交付日期範圍

---

## RAG 狀態系統

| 狀態 | 分數 | 判斷條件 |
|---|---|---|
| 🟢 GREEN | > 80 | 進度正常、風險可控、資源充足 |
| 🟡 YELLOW | 60–80 | 輕度落後或有已識別風險需監控 |
| 🔴 RED | < 60 | 嚴重落後、高風險爆發、需立即介入 |

每週例行審查時更新一次 RAG。

---

## Magic Lab 健康儀表板格式

```
## Magic Lab 健康快照 — [日期]

### 組合概覽
| 名稱 | 類型 | 狀態 | RAG | 關鍵里程碑 | 下個 Deadline |
|---|---|---|---|---|---|
| [Lab A] | Lab | 驗證中 | 🟢 85 | 原型完成 | 2026-06-01 |
| [Lab B] | Lab | 暫停 | 🔴 52 | 市場驗證失敗 | — |
| [Project X] | Project | 孵化中 | 🟡 72 | MVP 上線 | 2026-06-15 |

### 立即關注事項（RED / YELLOW）
1. [Lab B] — 市場反應不佳，建議啟動 Terry 決策：孵化 or 歸檔
2. [Project X] — 資源使用率 91%，超出目標上限，建議重新分工

### 本週里程碑達成率
達成：X / 計畫：Y → 達成率 Z%
```

---

## 資源容量管理

**目標使用率：70–85%**
- < 70%：資源閒置，可承接更多 Lab 驗證
- 70–85%：理想區間，可持續交付
- > 85%：超載警告，風險累積，停止新增任務

---

## Magic Lab 應用場景

| 場景 | 對應模式 |
|---|---|
| Terry 問「現在哪些 Lab 值得繼續投資」 | Mode 1：健康儀表板 + RAG |
| 多個 Lab 等待排程，資源有限 | Mode 2：WSJF / RICE 排序 |
| 某個 Lab 突然卡住找不到原因 | Mode 3：風險矩陣識別根因 |
| 每週 Lab Review 例行彙報 | Mode 1：一頁式儀表板輸出 |
| 孵化進 Project 後的里程碑管理 | Mode 1 + Mode 3 組合 |

---

## 主動風險旗標（Miles 主動提示）

- **計畫飄移** — 里程碑連續 2 週未更新 → 懷疑任務卡住，主動問
- **資源集中** — 單一人員承擔 > 50% 工作 → 單點故障風險
- **無明確 Exit Criteria** — Lab 沒有定義「何時該放棄」→ 浪費資源陷阱
- **多專案並行 > 3** — 注意力分散係數急升，建議暫緩新增

---

**Version:** 原版（Magic Lab 孵化流程客製化）
**Source:** [alirezarezvani/claude-skills](https://github.com/alirezarezvani/claude-skills/tree/main/project-management/skills/senior-pm)
**License:** MIT
