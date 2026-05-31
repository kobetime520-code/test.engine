---
name: pulse
description: "多平台即時情報掃描：Reddit + HN + Web + X/Twitter 交叉驗證，產出帶引用來源的主題簡報。觸發條件：'pulse on [主題]'、'trending: [主題]'、'[主題] 最新動態'、'市場情報'、'技術趨勢'、'競品動向'。"
owner: Terry（執行長）、Wayne（首席創新長）
source: https://github.com/alirezarezvani/claude-skills/tree/main/research/pulse
installed: 2026-05-25
---

# Pulse — 多平台即時情報引擎

> Terry 用於決策前的快速市場情報蒐集；Wayne 用於技術趨勢雷達掃描。

「不掌握最新情報就做決策，等於用過期地圖開車。」

---

## 啟動前的 4 個定錨問題（Phase 0）

開始任何 Pulse 調查前，先確認：

1. **主題** — 調查核心是什麼？（越具體越好：技術名稱、公司名稱、事件關鍵字）
2. **切入角度** — 使用者視角？開發者？投資人？競品分析？
3. **時間窗口** — 過去 24 小時？7 天？還是過去一個月？
4. **平台範圍** — 全平台？還是指定來源（Reddit / HN / Web / X）？

若問題不清晰 → 主動詢問，不要假設。

---

## 執行流程（Phase 1–4）

### Phase 1 — Reddit 掃描（技術社群 + 使用者情緒）
- 目標：r/technology, r/MachineLearning, r/programming, r/investing 等相關子版
- 蒐集：高票文章、熱門討論串、常見痛點、情緒傾向（正/負/中立）
- 輸出：前 5 個最有價值的討論摘要 + 連結

### Phase 2 — Hacker News 掃描（工程師 + 創業社群）
- 目標：前幾日的 HN Show/Ask/Top
- 蒐集：技術深度討論、批評聲音、實踐案例
- 輸出：前 3–5 個高評分討論要點

### Phase 3 — Web 精準搜尋（新聞 + 官方來源）
- 目標：最新新聞、官方公告、行業報告
- 蒐集：發布日期、來源可信度、事實核對
- 輸出：3–5 條帶日期的事實摘要

### Phase 4 — X/Twitter 脈衝（選用）
- 僅在需要即時反應時啟用（如重大公告、產品發布當日）
- 目標：KOL 評論、viral threads、社群情緒
- 輸出：5–10 條有代表性的推文觀點

> Phases 1–3 並行執行。Phase 4 按需啟用。

---

## 輸出格式

```
## Pulse 報告：[主題]
時間窗口：[XXX] | 執行時間：[ISO 8601]

### 核心發現（3 句以內）
[最重要的訊號，直接影響決策的部分]

### 各平台信號彙整
| 平台 | 熱度 | 情緒傾向 | 關鍵主題 |
|---|---|---|---|
| Reddit | 🔴高/🟡中/🟢低 | 正/負/混合 | ... |
| HN | ... | ... | ... |
| Web | ... | ... | ... |

### 重要引用（帶來源）
1. "[引用內容]" — 來源 + 日期
2. ...

### 跨平台規律
[多平台共同出現的主題或情緒，代表真實信號而非單一泡沫]

### Magic Lab 應用建議
[對 Terry / Wayne 當前決策或雷達掃描的直接建議]
```

---

## Magic Lab 應用場景

| 場景 | 使用方式 |
|---|---|
| 某新技術是否值得建 Lab 實驗 | `pulse on [技術名]` → Phase 1+2 社群反應 |
| 競品剛發布新功能 | `trending: [競品名]` → Phase 3+4 官方 + 即時反應 |
| 投資人 / 市場對某方向的態度 | 指定角度「投資人視角」 |
| Terry 決策前的情報蒐集 | 全平台 7 天窗口，完整 4 Phase |
| Wayne 每週技術趨勢報告 | HN + Reddit，過去 7 天，工程師視角 |

---

## 主動風險旗標

- **信號 vs 噪音** — 單一平台爆紅不等於真實趨勢，要求跨平台佐證
- **情緒泡沫** — Reddit 高票可能是 echo chamber，HN 批評可能是少數聲音
- **時間衰減** — 超過 30 天的資料在快速領域已過期，主動標記
- **來源偏差** — 開發者社群（HN）的觀點不代表一般用戶或商業市場

---

**Version:** 原版（Magic Lab 客製化說明）
**Source:** [alirezarezvani/claude-skills](https://github.com/alirezarezvani/claude-skills/tree/main/research/pulse)
**License:** MIT
