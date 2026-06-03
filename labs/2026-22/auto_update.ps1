# Magic Lab — 美股觀察 自動更新與推送
# 流程：python refresh.py → git add → commit（無變更則略過） → push
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

try {
    Log "=== 開始自動更新 ==="

    # 1) 抓報價、重產 dashboard.html / quotes.json
    Push-Location $lab
    python refresh.py
    if ($LASTEXITCODE -ne 0) { throw "refresh.py 失敗 (exit $LASTEXITCODE)" }
    Pop-Location

    # 2) 偵測是否有變更
    Push-Location $repo
    $changes = git status --porcelain -- "labs/2026-22/dashboard.html" "labs/2026-22/quotes.json"
    if ([string]::IsNullOrWhiteSpace($changes)) {
        Log "無資料變更，略過 commit/push"
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

