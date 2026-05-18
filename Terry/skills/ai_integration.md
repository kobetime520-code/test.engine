# Skill：AI 整合術（AI Integration）

> 模組負責人：Terry
> 版本：V1.0 | 2026-05-14
> 獨立於 AI Magic 系統

---

## 技能定義

設計、組合、串接多模型 AI 功能，打造 Magic Lab 獨有的 AI 驅動產品原型。所有整合方案**完全獨立**，不調用 AI Magic 的任何 API 或 token。

---

## 可用 AI 模型矩陣

| 模型 | 適用場景 | 接入方式 |
|---|---|---|
| Claude（Anthropic API） | 推理、程式生成、文件分析 | `ANTHROPIC_API_KEY`（Magic Lab 獨立 key） |
| GPT-4o（OpenAI API） | 多模態、視覺理解 | `OPENAI_API_KEY`（Magic Lab 獨立 key） |
| Gemini（Google API） | 長上下文、跨語言 | `GOOGLE_API_KEY`（Magic Lab 獨立 key） |
| 本地模型（Ollama） | 隱私敏感場景、離線運作 | localhost:11434 |

> **所有 API Key 以環境變數注入，不寫入任何程式碼或設定檔**

---

## 多模型串接設計模式

### Pattern 1：路由模式（Router）
```
用戶輸入 → 分類器（輕量模型）→ 路由至最適模型 → 輸出
適用：不同類型請求需要不同模型能力
```

### Pattern 2：驗證模式（Validator）
```
模型 A 生成答案 → 模型 B 驗證與評分 → 輸出高信度結果
適用：需要高準確率的關鍵決策
```

### Pattern 3：管線模式（Pipeline）
```
原始資料 → 模型 A（萃取）→ 模型 B（分析）→ 模型 C（格式化）→ 輸出
適用：複雜多步驟資料處理
```

### Pattern 4：集成模式（Ensemble）
```
相同問題 → 模型 A + 模型 B + 模型 C（並行）→ 投票/加權合併 → 最終輸出
適用：需要降低單一模型偏誤
```

---

## 標準整合原型架構

```python
# Magic Lab AI 整合範本（獨立，不依賴 AI Magic）
import os
import anthropic

class MagicLabAI:
    def __init__(self):
        # 環境變數注入，不硬編碼
        self.claude = anthropic.Anthropic(
            api_key=os.environ["MAGIC_LAB_ANTHROPIC_KEY"]
        )
    
    def run(self, prompt: str) -> str:
        response = self.claude.messages.create(
            model="claude-opus-4-7",
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text
```

---

## AI 功能評估清單

整合任何 AI 功能前，Terry 需確認：

- [ ] 此功能的 API Key 是 Magic Lab 專屬（非 AI Magic 共用）
- [ ] 程式碼無硬編碼的 token/key
- [ ] 功能可在 `labs/[名稱]/prototype/` 獨立執行
- [ ] 有 fallback 機制（API 失敗時的備案）
- [ ] 成本估算已完成（每次呼叫 × 預估呼叫量）

---

## 觸發條件

- 「設計一個 AI 功能串接方案」
- 「用多模型做 [任務]」
- 「這個 Lab 要用哪個模型最合適？」
- 「幫我寫 Claude API 整合的原型程式碼」
