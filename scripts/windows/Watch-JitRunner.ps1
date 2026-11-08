param(
    [string] $RunnerDirectory = 'C:\actions-runner',
    [string] $JitPath = 'C:\ProgramData\ActionsRunnerScaleSet\jitconfig'
)
# Runs as SYSTEM at startup. The controller publishes jitconfig atomically.
$ErrorActionPreference = 'Stop'
$stateDir = 'C:\ProgramData\ActionsRunnerScaleSet'
$configPath = $JitPath
$markerPath = Join-Path $stateDir 'started'
$runnerPath = Join-Path $RunnerDirectory 'run.cmd'
$mutex = New-Object System.Threading.Mutex($false, 'Global\ActionsRunnerScaleSetWatcher')
$ownsMutex = $false
$shouldShutdown = $false
try {
    try { $ownsMutex = $mutex.WaitOne(0) } catch [System.Threading.AbandonedMutexException] { $ownsMutex = $true }
    if (-not $ownsMutex) { return }
    $shouldShutdown = $true
    . (Join-Path $stateDir 'Test-JitConfig.ps1')
    if (Test-Path -LiteralPath $markerPath) { return }
    while (-not (Test-Path -LiteralPath $configPath -PathType Leaf)) { Start-Sleep -Seconds 2 }
    # Read only after publication. Do not print the configuration or subprocess output.
    $config = [System.IO.File]::ReadAllText($configPath)
    if (-not (Test-JitConfig $config)) { throw 'Invalid JIT configuration.' }
    # CreateNew persists across reboot and prevents a second attempt even after a crash.
    $marker = [System.IO.File]::Open($markerPath, [System.IO.FileMode]::CreateNew, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
    $marker.Dispose()
    if (-not (Test-Path -LiteralPath $runnerPath -PathType Leaf)) { throw 'Runner executable is missing.' }
    # Base64 has no cmd metacharacters. Invoke the batch file directly and wait.
    & $runnerPath --jitconfig $config *> $null
    if ($LASTEXITCODE -ne 0) { throw "Runner exited with code $LASTEXITCODE." }
} catch {
    # Never include exception text from the runner or the JIT config in logs.
    [System.Diagnostics.EventLog]::WriteEntry('Application', 'Windows runner watcher failed; inspect the VM locally.', [System.Diagnostics.EventLogEntryType]::Error, 1000)
} finally {
    if ($ownsMutex) { $mutex.ReleaseMutex() }
    $mutex.Dispose()
    # Only the mutex owner shuts down; a duplicate watcher must not interrupt a job.
    if ($shouldShutdown) { & "$env:SystemRoot\System32\shutdown.exe" /s /t 0 /f }
}
