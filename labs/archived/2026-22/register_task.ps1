# Magic Lab — 註冊／移除 美股觀察自動更新 的 Windows 工作排程
# 註冊（預設每日 06:00 本地時間，約美股收盤後）：
#   powershell -ExecutionPolicy Bypass -File register_task.ps1
# 自訂時間：
#   powershell -ExecutionPolicy Bypass -File register_task.ps1 -At 06:30
# 移除：
#   powershell -ExecutionPolicy Bypass -File register_task.ps1 -Unregister

param(
    [string]$At = "06:00",
    [switch]$Unregister
)

$taskName = "MagicLab-USStockWatch-AutoUpdate"
$script   = Join-Path $PSScriptRoot "auto_update.ps1"

if ($Unregister) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "已移除排程：$taskName"
    return
}

$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$script`""

$trigger = New-ScheduledTaskTrigger -Daily -At $At

$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -DontStopOnIdleEnd `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 15)

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description "Magic Lab 美股觀察：每日刷新報價並推送至 GitHub" `
    -Force | Out-Null

Write-Host "已註冊排程：$taskName（每日 $At）"
Write-Host "立即測試： Start-ScheduledTask -TaskName $taskName"

