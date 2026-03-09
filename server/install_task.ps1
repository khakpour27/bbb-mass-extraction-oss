# BB5 Pipeline — Scheduled Tasks Installer
# Run from an elevated (Run as Administrator) prompt:
#   powershell -ExecutionPolicy Bypass -File install_task.ps1

$username = "PC04355\Administrator"
$password = Read-Host "Enter password for $username" -AsSecureString
$cred = New-Object System.Management.Automation.PSCredential($username, $password)
$plainPassword = $cred.GetNetworkCredential().Password

# ============================================================
# Task 1: Pipeline Server (runs at startup, polls SharePoint)
# ============================================================
$task1Name = "BB5 Pipeline Server"
$pythonExe = "C:\Program Files\Python311\python.exe"
$serverScript = "C:\Users\MHKK\bbb_mass_extraction\server\pipeline_server.py"
$serverWorkDir = "C:\Users\MHKK\bbb_mass_extraction\server"

Unregister-ScheduledTask -TaskName $task1Name -Confirm:$false -ErrorAction SilentlyContinue

$action1 = New-ScheduledTaskAction -Execute $pythonExe -Argument "-u `"$serverScript`"" -WorkingDirectory $serverWorkDir
$trigger1 = New-ScheduledTaskTrigger -AtStartup
$settings1 = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit (New-TimeSpan -Days 0)

Register-ScheduledTask -TaskName $task1Name -Action $action1 -Trigger $trigger1 -Settings $settings1 `
    -User $username -Password $plainPassword -RunLevel Highest `
    -Description "BB5 Pipeline Server - polls SharePoint for trigger.json every 15s via Graph API"

Write-Host "`n[OK] '$task1Name' registered (trigger: at startup)" -ForegroundColor Green

# ============================================================
# Task 2: Weekly v3 Pipeline Run (Saturdays 02:00)
# ============================================================
$task2Name = "BB5 Weekly Pipeline Run"
$propyExe = "C:\Program Files\ArcGIS\Pro\bin\Python\Scripts\propy.bat"
$runnerScript = "C:\Users\MHKK\bbb_mass_extraction\runner_v3.py"
$pipelineWorkDir = "C:\Users\MHKK\bbb_mass_extraction"
$runnerArgs = "`"$runnerScript`" --moderate --publish --publish-target production"

Unregister-ScheduledTask -TaskName $task2Name -Confirm:$false -ErrorAction SilentlyContinue

$action2 = New-ScheduledTaskAction -Execute $propyExe -Argument $runnerArgs -WorkingDirectory $pipelineWorkDir
$trigger2 = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Saturday -At 02:00
$settings2 = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 4)

Register-ScheduledTask -TaskName $task2Name -Action $action2 -Trigger $trigger2 -Settings $settings2 `
    -User $username -Password $plainPassword -RunLevel Highest `
    -Description "BB5 Weekly v3 pipeline - moderate tier, publishes to production AGOL (Saturdays 02:00)"

Write-Host "[OK] '$task2Name' registered (trigger: Saturdays 02:00)" -ForegroundColor Green

# ============================================================
# Start the server now & show status
# ============================================================
Write-Host "`nStarting pipeline server..."
Start-ScheduledTask -TaskName $task1Name
Start-Sleep -Seconds 2

Write-Host "`n--- Scheduled Tasks Status ---" -ForegroundColor Cyan
Get-ScheduledTask -TaskName "BB5*" | Format-Table TaskName, State, @{N='NextRun';E={(Get-ScheduledTaskInfo -TaskName $_.TaskName).NextRunTime}} -AutoSize
