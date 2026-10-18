# Magic Lab 清理盤點報告（Cleanup Audit）

> 日期：2026-10-18
> 主導：Terry（首席創意長），Miles / Wayne / Wesley 的產出一併納入盤點
> 性質：**只盤點與建議**。本輪未刪除、移動、修改任何既有檔案，未 commit / push。唯一新增檔案為本報告。
> 範圍：`C:\AIworkplace\test.engine`（線上版 https://kobetime520-code.github.io/test.engine/）。`AI Magic\` 全程未讀取。

---

## 0. 一眼看結論

| 判定 | 數量 | 說明 |
|---|---|---|
| 保留 | 18 | 運作中，或結構上被依賴（CI、選情儀表板、團隊頁等） |
| 合併 | 5 | 功能或檔案重複，留一份即可 |
| 封存 | 7 | 已過時或無維運，但可逆；建議移至 `labs/archived/` 並移除入口 |
| 建議刪除 | 6 | 殘留物、無引用、或本身無功能 |
| 待確認 | 8 | 證據不足以斷定，需 JW 回答 |
| 合計 | 44 | 分頁 18 列 + 檔案 26 列 |

另有 **藍圖頁 Phase 3/4/5/6 文案問題 10 項**（見第 5 節），其中 2 項屬可驗證的事實錯誤。

**最確定該清的前幾項（證據鏈完整）**

| 排序 | 項目 | 一句話理由 |
|---|---|---|
| 1 | `test_ocean_history.json`、`test_plum_blossom_data.json` | 兩檔內容完全相同（同一 git blob），內容是 Base64 的「已清除」佔位符，全倉零引用 |
| 2 | `worldcup_preview.html` | 5/23 嵌入前的審稿草稿，已有正式 ⑧ 分頁取代，全倉零引用 |
| 3 | `skills/*.skill` 四個 | 同內容的壓縮包副本；其中 Wayne 那份有一行亂碼，與 `SKILL.md` 不一致 |
| 4 | ⑧ 世足賽分頁 | 賽事 7/19 已結束；賠率是 `Math.random` 模擬卻掛「立即下注」連結 |
| 5 | 根目錄 `compound_model.py` | 與 `wesley/compound_model.py` 只差空白對齊；CI 實際只 import `wesley/` 那份 |
| 6 | ⑤ 會議記錄整理工具 | 輸出是空白框架，程式內自己建議「貼到 Claude 或 ChatGPT」 |

**真正的警訊不在檔案數量，而在兩件事：**
1. ⓪ 美股測試區的資料停在 **2026-07-03（已停更 107 天）**，本機排程器查無對應排程，但 phase4-plan 仍寫「Task Scheduler 自動化」。
2. 藍圖頁的 Phase 4「9 月發行」已逾期 18 天且**零產出**，而「126 天閘門 9 月解鎖」的算法本身有誤（實際約 11/04–11/10）。

---

## 1. 方法與證據欄位

| 查證面向 | 做法 |
|---|---|
| 最後修改 | `git log` 逐檔最後一次實質 commit（排除「選情儀表板每週更新」「美股自動刷新」類 chore），並對照檔案系統時間（8/16 為 clone 時間，不具參考價值） |
| 被引用 | 全倉 Grep（html / md / yml / py / ps1 / json），排除 `.git/` |
| 是否重複 | 位元組級與正規化後（忽略 CRLF）內容比對 |
| 是否運作 | `Get-ScheduledTask` 查本機排程；`gh run list` 查 GitHub Actions 近 15 次紀錄；`curl` 查 Pages 實際可訪問性 |
| 是否空殼 | 檔案大小、內容解碼（Base64）、`.gitkeep` 盤點 |
| 有人在用 | 能查到的客觀證據（排程、CI、commit）。**瀏覽器端使用量無法查到**，見第 8 節 |

**全域事實（影響多項判定）**

| # | 事實 | 證據 |
|---|---|---|
| G1 | 目前唯一持續被機器維運的只有兩條：⑨ 選情儀表板、GitHub Actions CI | `ElectionDashboard-Weekly` 上次執行 2026-10-18 12:00、結果碼 0；CI 10/8–10/17 連續 10 次 schedule 全綠 |
| G2 | 2026-08-30 之後，非選情的實質 commit 只有 1 筆（9/24 移除 Office 鎖定暫存檔） | `git log --since=2026-08-31` |
| G3 | ②–⑦ 各分頁自 2026-05-17（Wesley v3/v4）後未再實質修改 | `git log -- index.html` |
| G4 | 倉庫為 **public**，所有檔案（含 `Terry/phase4-plan.md`、`backtest_report.json`、`.claude/settings.local.json`）皆可由 Pages 以 HTTP 200 取得 | `gh api repo` 顯示 `visibility: public`；10 個路徑 curl 皆 200 |
| G5 | 本機排程器**沒有** `MagicLab-USStockWatch-AutoUpdate`，`labs/2026-22/` 也沒有 `auto_update.log` | `Get-ScheduledTask` 比對；檔案系統 |
| G6 | `projects/`、`intel/`、`labs/`（根）、`miles/plans`、`wayne/radar`、`wesley/bugs`、`wesley/tests`、`terry/briefs` 共 8 處只有 `.gitkeep`，成立 5 個月以來**沒有任何實驗晉升到 `projects/`** | `git ls-files`；phase4-plan 自陳「projects/ 完全空白」 |
| G7 | 多個分頁功能與其他 Team 已登記的職能重疊（Team Stock 的 Grace / Joe、Team Life 的 shufeng / Jimmy / Luna） | Agent 登記資料（職能描述），未讀取 AI Magic 內容 |

---

## 2. 分頁盤點（index.html）

| ID | 分頁 / 區塊 | 證據 | 判定 | 建議動作 |
|---|---|---|---|---|
| T0 | ⓪ 美股測試區（iframe → `labs/2026-22/dashboard.html`） | 資料時點 `2026-07-03 11:49 UTC`，已停更 107 天；git 最後自動刷新 7/3；排程器查無對應任務；無 `auto_update.log`。記憶備註：7 月曾換新電腦，排程可能未重新註冊（**推測，未驗證**） | 待確認 | 二選一：(a) 仍要看 → 以 `register_task.ps1` 重新註冊；(b) 不看 → 連同 `labs/2026-22/` 封存並移除分頁。決定前，頁面上是 3.5 個月前的價格，至少應加「已停更」標示 |
| T1 | ① 藍圖 | 見第 5 節，Phase 4/5/6 多處與實況不符 | 保留 | 保留頁面，改寫文案 |
| T2 | ② 月複利 | 固定月報酬 10%（頁面自標「年化等效 213.8%」），前提不切實際；與 ③、④雙曲線為同源計算，全站 JS 內共 3 套複利實作，加 Python 共 4 套；自稱「Team Stock 專屬」但無任何與 Team Stock 資料對接；最後修改 5/16–5/17 | 合併 | 併入 ③，以「月複利 / 年複利」切換呈現；或直接移除 |
| T3 | ③ 複利計算 | 預設值（30 萬 / 5 千 / 8% / 20 年）與 CI 測試案例相同，可作為單一正本 | 保留 | 作為 T2、T4a 的合併目標 |
| T4a | ④ 財富儀表板 › 雙曲線對比 | 與 T2 同源（月複利 10% vs 年化 8%） | 合併 | 併入 ③ |
| T4b | ④ › 自由日計算 | 4% 提領率試算，全站僅此一處 | 保留 | 保留 |
| T4c | ④ › 投資題材雷達 | `radarData` 6 項寫死於 JS，語氣停在 2024 年（「特斯拉降價壓縮毛利」「ChatGPT 掀起革命」），5/17 後未更新；題材追蹤已有 Team Stock 的 Grace 每日維運 | 封存 | 封存或移除；若要保留須改為讀取資料檔 |
| T4d | ④ › 波段紀律心法 | 靜態清單 + `localStorage` 勾選（key：`ml_discipline`），是否有人在勾無法從倉庫端得知 | 待確認 | 請 JW 檢查瀏覽器 localStorage 是否有勾選紀錄（做法見第 8 節） |
| T5a | ⑤ 工作自由 › Excel 摘要 | `runExcel` 僅以正則抓數字做加總 / 平均 / 極值 | 封存 | 封存 |
| T5b | ⑤ › 書信生成 | 5 套固定字串範本，輸入文字只是塞進模板 | 封存 | 封存 |
| T5c | ⑤ › 會議記錄整理 | 輸出為空白框架（【決議事項】（請從逐字稿中擷取…）），程式內自承「建議將逐字稿貼至 Claude 或 ChatGPT」；完全未處理輸入內容 | 建議刪除 | 移除 |
| T5d | ⑤ › 任務拆解 | 固定 8 步模板，僅把使用者輸入替換進標題 | 封存 | 封存 |
| T5e | ⑤ › 週報生成 | 固定框架，僅填入日期與輸入文字 | 封存 | 封存 |
| T5f | ⑤ › AI 翻譯 | **全頁唯一有實際外部功能者**（MyMemory API，單次 500 字、免費 5,000 字元/日）；實為機器翻譯，並非 AI 模型，標題易誤導 | 待確認 | 若 JW 有在用則保留並改名「翻譯」；瀏覽器內建翻譯與 Claude 已可取代 |
| T6 | ⑥ 生活儀表板（消費、運動、旅遊、家庭財務、育兒） | 消費 / 運動為 localStorage 記帳（`ml_exp`、`ml_sport`）；旅遊 / 財務 / 育兒為公式模板；職能與 Team Life（shufeng 財管、Jimmy 旅遊、Luna 教育）重疊；5/17 後未修改。**是否有人真的在記帳 / 記運動，倉庫端無法得知** | 待確認 | 請 JW 確認使用情形；若無使用，整頁封存 |
| T7 | ⑦ 團隊 | 四人介紹卡 + 公式橫幅，與現行編制一致，僅約 2.4 KB | 保留 | 保留 |
| T8 | ⑧ 世足賽 | 賽事 2026/6/11–7/19 已結束 3 個月；倒數結束後僅顯示「開幕」字樣；賠率由 `Math.random` 每 30 秒模擬波動（頁面自註「模擬示意」），卻附「立即下注」按鈕連至台灣運彩，且為公開頁面；佔 `index.html` 約 21 KB（HTML 約 5 KB + JS 約 16 KB，另有專屬 CSS）；5/27 後未修改；程式註解編號仍寫 ⑨，與分頁 ⑧ 不一致 | 封存 | 移至 `labs/archived/` 並移除分頁。模擬賠率 + 下注連結的組合在公開頁面上有誤導風險，優先處理 |
| T9 | ⑨ 選情儀表板 | 每週日 12:00 排程自動更新並 push，最近一次 2026-10-18，結果碼 0；已於 `election-dashboard` 專案維護 | 保留 | **勿動**（本輪指定為運作中） |

---

## 3. 檔案盤點

| ID | 路徑 | 證據 | 判定 | 建議動作 |
|---|---|---|---|---|
| F01 | `compound_model.py`（根目錄） | 與 `wesley/compound_model.py` 僅有 `SkillScore(...)` 範例行的空白對齊差異（5,919 vs 5,928 bytes）；CI 實際 import 的是 `wesley/` 那份；根目錄版只被文件提及（phase4-plan、chaos-engineering SKILL） | 合併 | 留 `wesley/` 版，刪根目錄版，並同步修兩處文件路徑；V10 計畫中的 `compound_model_v2.py` 亦應放 `wesley/` |
| F02 | `wesley/compound_model.py` | CI 依賴（見第 4 節）；最後修改 5/16 | 保留 | 不可動 |
| F03 | `test_radar.py` | CI 的執行對象；實際只測 3 件事：印環境、檢查四資料夾存在、複利數學。與分頁 / 選情 / 美股完全無關 | 保留 | 保留；若日後要強化 CI，從這裡擴充 |
| F04 | `requirements.txt` | 列 pytest / requests / pandas / openpyxl，而 `test_radar.py` 與 `compound_model.py` 只用標準庫，4 個套件皆未使用；但 CI 第 3 步 `pip install -r requirements.txt` 需要此檔存在 | 保留 | 可瘦身為空內容或僅留註解，但**不可刪檔**（刪檔會使 CI 紅燈） |
| F05 | `.github/workflows/test_auto_radar.yml` | 每日 06:00（台灣）執行，近 10 次 schedule 全綠，每次 16–25 秒；另被每週選情 push 觸發 | 保留 | 保留；job 名稱「核心引擎」言過其實，可改名 |
| F06 | `test_ocean_history.json` | 92 bytes；Base64 解碼為 `{"magic_lab": true, "status": "cleared", "note": "…已清除"}`（備註字串已亂碼）；5/15「清空舊資料」殘留，5/19 刪除後 5/20 又以「Add files via upload」重新上傳；全倉零引用 | 建議刪除 | 刪除 |
| F07 | `test_plum_blossom_data.json` | 與 F06 為**同一 git blob**（`5681ec2d…`），內容完全相同；零引用 | 建議刪除 | 刪除 |
| F08 | `worldcup_preview.html` | 55 KB；5/23 commit 訊息自稱「審稿草稿」，同日正式嵌入 ⑧ 分頁；全倉零引用；Pages 上仍可公開存取 | 建議刪除 | 刪除（正式版內容在 `index.html`；若 T8 封存，則隨 T8 一併封存） |
| F09 | `MagicLab_Skill說明文件_v2.xlsx` | 5/16 建立，9 張工作表；內文「存檔路徑 C:\AIworkplace\Magic Lab\」為舊路徑，只列四位成員的 skill（現況已有 7 個）；全倉零引用；內容與 `SKILL.md`、README 技能總表重複 | 封存 | 封存；若 JW 仍用它做簡報，改標為「v2 歷史版」 |
| F10 | `backtest/backtest_report.json` | 生成於 2026-05-13；`details` 共 **224 筆（219 檔不重複）**，`return_5d / 30d / 126d`、TWII、超額報酬欄位 **0 筆有值**；是 V10 的 C1 輸入；來源註記為「ocean_history」 | 待確認 | 去留取決於 V10 是否續行（見 D4）。若續行，保留並補算；若 V10 擱置，封存 |
| F11 | `election/index.html` | 226 KB，10/18 12:00 更新 | 保留 | 勿動 |
| F12 | `labs/2026-22/dashboard.html`、`quotes.json`、`refresh.py` | 美股觀察 MVP，28 檔；全倉唯一的引用者是 ⓪ 分頁與 phase4-plan（「台股掃描 Lab 的範本」）；最後更新 7/3 | 待確認 | 隨 T0 決定；若 Phase 4 台股掃描 Lab 續行，此為範本，不宜刪 |
| F13 | `labs/2026-22/register_task.ps1`、`auto_update.ps1`、`.gitignore` | 腳本邏輯完整可用，但**本機未註冊**，排程失聯 | 待確認 | 隨 T0 決定；選 (a) 時直接執行 `register_task.ps1` 即可恢復 |
| F14 | 8 處 `.gitkeep` 空殼（`labs/`、`intel/`、`projects/`、`miles/plans/`、`wayne/radar/`、`wesley/bugs/`、`wesley/tests/`、`terry/briefs/`） | 零成本，且為 CLAUDE.md 定義的流程落點（labs → projects）；其中 `terry/briefs/` 因本報告而不再是空殼 | 保留 | 不建議為清理而動；但空殼本身是「孵化流程 0 產出」的證據 |
| F15 | `terry/`、`miles/`、`wayne/`、`wesley/` 各自的 `README.md`（4 份） | CI 的「架構驗證」job 檢查這四個資料夾存在（小寫）；README 中描述的子資料夾 `concepts/`、`references/`、`trackers/`、`reports/`、`experiments/`、`insights/`、`support/` 實際都不存在 | 保留 | 保留，並修訂 README 與現況不符之處 |
| F16 | `Terry/`（大寫）與 `terry/`（小寫）並存於 git | git tree 內同時有兩個目錄：`Terry/`（profile、phase4-plan、skills、藍圖）與 `terry/`（README、briefs）。Windows 不分大小寫所以看不出，但 Linux CI、GitHub 網頁會視為兩個目錄；CI 驗證的是小寫 `terry` | 合併 | 統一大小寫（需兩段式 `git mv` 並同步改 workflow 與 `test_radar.py` 的 `required` 清單）。**高風險，須與 F03、F05 一起處理** |
| F17 | `Terry/profile.md` | 寫「單位路徑 C:\AIworkplace\Magic Lab」（舊路徑）、職稱「執行長（CEO）」（現行為首席創意長）、技能路徑寫 `skills/innovation_radar.md`（實際在 `Terry/skills/`） | 保留 | 保留，修訂路徑與職稱 |
| F18 | `Terry/skills/*.md`（5 份：創新雷達、快速原型、市場情報、決策矩陣、AI 整合術） | 僅被 README / profile 引用；**未註冊為可呼叫 skill**（系統 skill 清單中只有 `terry-creative-brief`、`wayne-innovation-radar`、`pulse` 等）；內文提到的 `intel/competitors/`、`intel/tech_trends/` 等子資料夾不存在；功能與 `wayne-innovation-radar`、`pulse` 重疊。Terry 為作者，此項屬自評 | 待確認 | 請 JW 決定：併入 `pulse` / `wayne-innovation-radar`、封存、或註冊為正式 skill |
| F19 | `Terry/phase4-plan.md` | 內容與 F20 互為母本（文件自述）；含 5 處與實況不符之處（見第 5 節） | 保留 | 保留，修訂；長期可與 F20 擇一為唯一正本 |
| F20 | `Terry/v10-release-blueprint.html` | 同上；藍圖頁有連結指向它 | 保留 | 保留，修訂 |
| F21 | `skills/*.skill`（4 個） | 為 `SKILL.md` 的壓縮包副本；`wayne-innovation-radar.skill` 內第 216 行為亂碼，與同名資料夾的 `SKILL.md` 不一致；全倉零引用 | 建議刪除 | 刪除 |
| F22 | `skills/` 下 7 個資料夾的 `SKILL.md` | 與 `C:\AIworkplace\AI Agent\` 內同名 master 內容相同（正規化換行後逐檔比對 7/7 一致，僅 CRLF 差異）；Terry 的角色定義指定以 AI Agent 版為準 | 合併 | 擇一為唯一正本（見 D5）。若以 AI Agent 為正本，則 CLAUDE.md 的 `skills/…` 路徑一併改指 |
| F23 | `.claude/launch.json` | 設定名稱仍叫 `warroom`，但戰情室分頁 5/27 已移除；功能（`npx serve` 靜態預覽，port 3456）仍可用 | 保留 | 改名為 `preview`，其餘不動 |
| F24 | `.claude/settings.local.json` | 本機權限設定被追蹤進公開倉庫；內容僅兩條 allow，無敏感值，但性質屬本機檔 | 建議刪除 | 自版控移除（`git rm --cached` + 加入 `.gitignore`），本機檔保留 |
| F25 | `README.md` | 10 KB 入口文件；提到 `labs/archived/`（不存在）、工作區表缺 `election/`、`backtest/`、`Terry/` 文件 | 保留 | 保留，修訂 |
| F26 | `CLAUDE.md` | 「已安裝 Skills」只列 3 個（實有 7 個）；結構表缺 `election/`、`backtest/`；稱 Terry「執行長」，與 Agent 登記不一致 | 保留 | 保留，修訂 |

---

## 4. CI 依賴鏈（清理時不可單獨動）

```
.github/workflows/test_auto_radar.yml
 ├─ job「核心引擎」
 │    ├─ pip install -r requirements.txt        ← 檔案必須存在（內容可瘦身）
 │    └─ python test_radar.py
 │         ├─ 檢查 terry / miles / wayne / wesley 資料夾存在（小寫，Linux 區分大小寫）
 │         └─ sys.path.insert(0,"wesley") → import wesley/compound_model.py
 └─ job「架構驗證」→ 再檢查一次同樣四個資料夾（小寫）
```

| 若動了 | 後果 |
|---|---|
| 刪 `requirements.txt` | CI 第 3 步失敗，紅燈 |
| 刪 / 搬 `wesley/compound_model.py` | `test_radar.py` 走 ImportError 分支，被當成「非阻斷性」跳過，**CI 仍綠但實際什麼都沒測**（靜默失效） |
| 刪小寫 `terry/`、`wayne/`、`miles/`、`wesley/` 任一 | 兩個 job 皆失敗 |
| 只改 F16（大小寫統一）不改 workflow | 同上 |
| 刪根目錄 `compound_model.py` | 無影響（CI 不 import 它） |

---

## 5. 藍圖頁（Phase 4 / 5 / 6）查核

| ID | 藍圖說法 | 查證結果 | 嚴重度 | 建議改寫 |
|---|---|---|---|---|
| P4-1 | Phase 4 徽章「9 月發行 · 大作」，狀態 active | 今日 10/18，逾期 18 天。藍圖與 phase4-plan 列出的 A1–A5、B1–B5、C1–C5 產出中，`fill_returns.py`、`api_test.py`、`fetch_twii.py`、`pools_logic.md`、`labs/2026-tw-scan/`、`intel/tw_sources.md`、`intel/theme_tags.json`、`pool_performance.md`、`compound_model_v2.py`、`register_tw_scan.ps1` **全部不存在**；8/30 之後無任何 Phase 4 相關 commit | 高 | 改為「進行中 · 延後」並重訂日期，或明確降為「規劃中」 |
| P4-2 | 「9 月，126 個交易日閘門解鎖」「126 交易日 · 就緒」「5d ✓ 30d ✓ 126d ✓ 全數解鎖」 | **算法錯誤**。2026-05-12 起算，126 個交易日約落在 **11/04（僅扣週末）至 11/10（再扣端午、中秋、國慶補假）**。9/30 時僅約 98 個交易日，今日 10/18 約 109 個。「9 月解鎖」不成立 | 高（事實錯誤） | 改為「預計 2026-11 上旬解鎖」，移除「就緒 / 全數解鎖」字樣 |
| P4-3 | 「報酬可回算」「量化引擎首次能吐出真實報酬」 | 僅指資料條件。`backtest_report.json` 目前 0/224 筆有值，TWII 基準亦全空；5d / 30d 本已可算卻從未算 | 中 | 區分「資料就緒」與「已計算」 |
| P4-4 | 「201 檔訊號分布在四個魚池」（藍圖 hero、魚池區、phase4-plan） | 四大魚池合計 **23 筆**（5+6+6+6）；201 筆是另一個 `ocean` 池；`details` 共 224 筆 / 219 檔不重複。藍圖把 201 與 23 混為一談，魚池卡片加總與標題數字對不上 | 高（事實錯誤） | 改為「四大魚池 23 檔 + ocean 池 201 檔，共 224 筆訊號」 |
| P4-5 | phase4-plan：美股觀察「28 檔、Task Scheduler 自動化」 | 排程未註冊，資料停在 7/3（見 T0 / G5） | 中 | 改為「28 檔、已停更」或補註冊後維持現文 |
| P4-6 | 決策閘門 Q1–Q4（數據源預算、魚池進場條件、受眾、Phase 5 時程） | 自 2026-06-06 建檔起即為開放；8/29 更新版仍全部開放；全倉無任何決策紀錄。Q1、Q2 被標為「卡住 A1/C1 / 掃描 Lab」，是 Phase 4 零產出的直接原因 | 高 | 列為 JW 必答項（見第 7 節 D4） |
| P4-7 | 版本線 V6.9.12 → V7.x → V8.x → **V10** | Agent 登記資料中，Team Stock 現行 `radar.py` 仍為 **V9.3**，看不到 V10 已發行的跡象（**未讀取 AI Magic 驗證**）。藍圖版本號與 Team Stock 的版本線掛鉤，與「Magic Lab 完全獨立於 AI Magic」的隔離原則存在張力 | 中 | 請 JW 確認：藍圖追蹤的是 Team Stock 版本線，還是 Magic Lab 自己的 |
| P3-1 | Phase 3「已完成」：含「題材分析引入、輿情分析、量化模型初探」 | phase4-plan 自陳「輿情分析描述在藍圖，但沒有對應的台股輿情抓取程式」「題材標籤邏輯缺失」。若成果指 Team Stock（Grace 題材、Joe 回測），應註明範圍；若指 Magic Lab，與自陳缺口矛盾 | 中 | 註明「完成範圍」或調降為「部分完成」 |
| P5 | Phase 5「Pilot Run Phase · 待啟動」：「V11.x 旗艦。全自動化排程下單、全 Agent AI 接管、商業化啟動」 | **過度承諾**。(1) 前置的 Phase 4 產出全數未成；(2) 倉庫內沒有任何券商 API、下單、風控規格的程式或文件，「排程下單」涉及金融責任與法遵，一句話帶過不妥；(3) 「商業化」的受眾（Q3）至今未答；(4) 「何時啟動」（Q4）至今未答；(5) 一個階段塞三個重大承諾，沒有任何退出標準 | 高 | 改為「願景（非里程碑）」，列出啟動前置條件（Phase 4 的 C1、A1 完成且魚池績效公布），並移除「排程下單」或改為「人工確認下單輔助」 |
| P6 | Phase 6「MP Phase — 終極目標 · 終點」：「AI Agent 全面運作。工作自由 × 財富自由 × 生活自由 × 人生自由」 | **不可驗證**。無衡量指標、無時程、無責任人。若以現況對照：29 人 AI 編制確實存在，但「全面運作」的定義未寫 | 中 | 改標「願景」，並至少掛 1–2 個可量化代理指標（如被動收入覆蓋率，Team Life 的 shufeng 已在評估類似指標） |

藍圖文案問題共 **10 列**（P4-1 至 P4-7、P3-1、P5、P6），其中 P4-2、P4-4 為可由資料直接驗證的事實錯誤。

---

## 6. 建議執行順序（待 JW 授權，本輪未執行）

| 批次 | 動作 | 涉及項目 | 風險 | 備註 |
|---|---|---|---|---|
| 1 | 刪除零引用殘留 | F06、F07、F08、F21 | 極低 | 一個 commit，可由 git 還原 |
| 2 | 修正藍圖與文件的事實錯誤 | P4-2、P4-4、P4-5 與 F17、F25、F26 | 低 | 純文字，不影響功能；P4-2 優先 |
| 3 | 封存世足賽 | T8 | 低 | 移除分頁入口與對應 JS / CSS；原始碼放 `labs/archived/worldcup-2026/` |
| 4 | 合併重複 | F01；T2 + T4a → T3 | 低–中 | F01 只改文件路徑；T2 / T4a 合併會動 `index.html` JS，需手動驗證計算結果一致 |
| 5 | 封存低價值工具 | T4c、T5a/b/d/e、F09；刪除 T5c | 低 | 視 D3 的回答調整範圍 |
| 6 | 藍圖 Phase 5/6 改寫 | P5、P6、P4-1 | 低 | 措辭須 JW 拍板（D7） |
| 7 | 大小寫統一 | F16 | **高** | 必須與 F03、F05、F15 同批處理，並在推送前本機模擬 Linux 大小寫行為 |
| 8 | 技能副本單一來源 | F22 | 中 | 取決於 D5 |

**明確不建議動的：** `wesley/compound_model.py`、`test_radar.py`、`requirements.txt`（檔案本身）、`.github/workflows/`、四個小寫資料夾與其 README、`election/`、⑨ 分頁、⑦ 團隊頁、8 處 `.gitkeep`。

---

## 7. 需要 JW 決定的事

| # | 問題 | 影響 | 建議選項 |
|---|---|---|---|
| D1 | ⓪ 美股觀察還要看嗎？ | T0、F12、F13 | (a) 重新註冊排程（一行指令）；(b) 整個封存 |
| D2 | ⑧ 世足賽封存？ | T8、F08 | 建議封存（賽事已結束，模擬賠率 + 下注連結為公開風險） |
| D3 | ⑥ 生活儀表板、④ 紀律心法、⑤ 翻譯，有在用嗎？ | T4d、T5f、T6 | 見第 8 節的自查方式；無使用則整批封存 |
| D4 | **V10 / Phase 4 是否續行？** 若續行，需答 Q1（數據源預算）、Q2（四個魚池的進場條件），並重訂日期。另需釐清台股掃描 / 回測與 Team Stock（Joe、Right）及 `quant-research` 專案的分工，避免同一件事做兩次 | F10、F12、F19、F20、P4-1/6/7 | 續行 → 先答 Q1、Q2；擱置 → 藍圖降級、F10 封存 |
| D5 | 技能檔以哪裡為唯一正本？ | F22 | 建議以 `C:\AIworkplace\AI Agent\` 為正本，倉庫改為引用說明 |
| D6 | 是否授權統一 `Terry/` 與 `terry/` 大小寫？ | F16 | 建議統一，但須與 CI 一起改，並由 Wesley 驗證 |
| D7 | Phase 5 / 6 的措辭：降為「願景」，還是維持里程碑並補上標準？ | P5、P6 | 建議降為願景，移除「排程下單」 |
| D8 | 倉庫目前為 public，是否要維持？ | G4 | 封存只是移動位置，內容仍公開；若不希望公開魚池與規劃文件，需改為 private（Pages 屆時需評估方案） |

---

## 8. 侷限與未驗證項目

| 項目 | 說明 |
|---|---|
| 網頁端實際使用量 | GitHub 的 traffic API 只統計倉庫頁面而非 Pages 站點，回傳 0 且無意義；站上亦無分析碼。**無法判定任何分頁被點閱的次數** |
| localStorage 使用情形 | 資料存在 JW 的瀏覽器內，倉庫端看不到。自查方式：開啟線上版 → 開發者工具 → Application → Local Storage，查看 `ml_exp`（消費）、`ml_sport`（運動）、`ml_discipline`（紀律勾選）是否有資料 |
| 排程失聯原因 | 「換新電腦後未重新註冊」只是與時間點吻合的推測，未驗證 |
| 交易日計算 | 假日以 2026-06-19、09-25、09-28、10-09 估算，與實際行事曆可能差 1–2 日；結論（11 月上旬、非 9 月）不受影響 |
| Team Stock / Team Life 重疊 | 依 Agent 登記的職能描述判斷，未讀取 AI Magic 內容，重疊程度須由各 Team 確認 |
| 排程查詢範圍 | 僅查本機 Windows 工作排程器；GitHub 端排程僅有 CI 一條（已確認） |
| `quant-research` | 僅確認該專案存在且無 `fill_returns` 類程式，未深入比對內容 |
