# Magic Lab — 美股觀察 自動更新與推送
# 流程：python refresh.py → 比對報價數值（忽略時點戳記）→ 有變動才 commit + push
# 手動執行：右鍵以 PowerShell 執行，或  powershell -ExecutionPolicy Bypass -File auto_update.ps1
# 排程執行：見同資料夾 register_task.ps1

$ErrorActionPreference = "Stop"
$lab  = $PSScriptRoot                       # labs/2026-22
$repo = Resolve-Path (Join-Path $lab "..\..")  # 倉庫根
$log  = Join-Path $lab "auto_update.log"

function Log($msg) {
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg
    Add-Content -Path $log -Value $line -Encoding utf8
    Write-Host $line
}

$quotesPath = Join-Path $lab "quotes.json"

# 取報價數值的標準化字串（排除 as_of 時點，只看 quotes 陣列）
function Get-QuoteFingerprint($path) {
    if (-not (Test-Path $path)) { return "" }
    try {
        $obj = Get-Content -Raw -Encoding UTF8 $path | ConvertFrom-Json
        return ($obj.quotes | ConvertTo-Json -Depth 10 -Compress)
    } catch { return "" }
}

try {
    Log "=== 開始自動更新 ==="

    # 0) 記錄刷新前的報價指紋
    $before = Get-QuoteFingerprint $quotesPath

    # 1) 抓報價、重產 dashboard.html / quotes.json
    Push-Location $lab
    python refresh.py
    if ($LASTEXITCODE -ne 0) { throw "refresh.py 失敗 (exit $LASTEXITCODE)" }
    Pop-Location

    # 2) 比對報價數值（忽略時點戳記）
    $after = Get-QuoteFingerprint $quotesPath
    Push-Location $repo
    if ($before -eq $after) {
        Log "報價數值未變動（僅時點戳記），還原檔案並略過 commit/push"
        git checkout -- "labs/2026-22/dashboard.html" "labs/2026-22/quotes.json" 2>$null
        Pop-Location
        return
    }

    # 3) commit + push
    $stamp = (Get-Date -Format "yyyy-MM-dd HH:mm")
    git add "labs/2026-22/dashboard.html" "labs/2026-22/quotes.json"
    git commit -m "chore: 美股觀察自動刷新報價（$stamp）" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
    if ($LASTEXITCODE -ne 0) { throw "git commit 失敗 (exit $LASTEXITCODE)" }
    git push origin main
    if ($LASTEXITCODE -ne 0) { throw "git push 失敗 (exit $LASTEXITCODE)" }
    Pop-Location

    Log "=== 完成：已推送 ==="
}
catch {
    Log "!!! 錯誤：$_"
    exit 1
}

