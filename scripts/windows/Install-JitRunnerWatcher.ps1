[CmdletBinding()]
param(
    [string] $RunnerDirectory = 'C:\actions-runner',
    [string] $JitPath = 'C:\ProgramData\ActionsRunnerScaleSet\jitconfig',
    # Only use after deregistering the old runner in GitHub using a removal token.
    [switch] $RemoveExistingRegistration
)
$ErrorActionPreference = 'Stop'
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = New-Object Security.Principal.WindowsPrincipal($identity)
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { throw 'Run this script elevated as Administrator.' }
$runnerDir = $RunnerDirectory
$stateDir = 'C:\ProgramData\ActionsRunnerScaleSet'
$taskName = 'ActionsRunnerScaleSetWatcher'
if ($runnerDir -notmatch '^[A-Za-z]:\\[A-Za-z0-9_. -]+(?:\\[A-Za-z0-9_. -]+)*$') { throw 'RunnerDirectory must be an absolute Windows path with safe characters.' }
if ($JitPath -notmatch '^[A-Za-z]:\\[A-Za-z0-9_. -]+(?:\\[A-Za-z0-9_. -]+)*$' -or -not [string]::Equals((Split-Path -Parent $JitPath), $stateDir, [StringComparison]::OrdinalIgnoreCase)) { throw 'JitPath must be a file in C:\ProgramData\ActionsRunnerScaleSet; other locations require a custom watcher.' }
if ((Split-Path -Leaf $JitPath) -in @('started', 'Watch-JitRunner.ps1', 'Test-JitConfig.ps1')) { throw 'JitPath conflicts with watcher state files.' }
if (-not (Test-Path -LiteralPath (Join-Path $runnerDir 'run.cmd') -PathType Leaf)) { throw 'Install the GitHub Actions runner in RunnerDirectory first.' }
# A conventional runner service can race the JIT runner. Never remove it implicitly.
$services = @(Get-CimInstance Win32_Service | Where-Object { $_.PathName -and $_.PathName.IndexOf($runnerDir, [StringComparison]::OrdinalIgnoreCase) -ge 0 })
$registrationFiles = @('.runner', '.credentials', '.credentials_rsaparams', '.service') | ForEach-Object { Join-Path $runnerDir $_ } | Where-Object { Test-Path -LiteralPath $_ }
if (($services.Count -gt 0 -or @($registrationFiles).Count -gt 0) -and -not $RemoveExistingRegistration) { throw 'Existing runner registration or service found. Deregister it in GitHub using a removal token, then rerun with -RemoveExistingRegistration.' }
if ($RemoveExistingRegistration) {
    foreach ($service in $services) {
        if ($service.State -ne 'Stopped') { Stop-Service -Name $service.Name -Force -ErrorAction Stop }
        & "$env:SystemRoot\System32\sc.exe" delete $service.Name | Out-Null
        if ($LASTEXITCODE -ne 0) { throw 'Could not delete the existing runner service.' }
    }
    foreach ($file in $registrationFiles) { Remove-Item -LiteralPath $file -Force }
}
New-Item -ItemType Directory -Path $stateDir -Force | Out-Null
# Replace inherited ACLs: only SYSTEM and Administrators can read or modify secrets.
$acl = New-Object System.Security.AccessControl.DirectorySecurity
$acl.SetAccessRuleProtection($true, $false)
$inheritance = [System.Security.AccessControl.InheritanceFlags]'ContainerInherit, ObjectInherit'
$propagation = [System.Security.AccessControl.PropagationFlags]::None
foreach ($sid in @('S-1-5-18', 'S-1-5-32-544')) {
    $account = New-Object System.Security.Principal.SecurityIdentifier($sid)
    $rule = New-Object System.Security.AccessControl.FileSystemAccessRule($account, 'FullControl', $inheritance, $propagation, 'Allow')
    $acl.AddAccessRule($rule)
}
Set-Acl -LiteralPath $stateDir -AclObject $acl
# Never silently reset a used clone or remove a controller recovery handoff.
if ((Test-Path -LiteralPath (Join-Path $stateDir 'started')) -or (Test-Path -LiteralPath $JitPath)) { throw 'State directory contains a handoff or started marker. Clean the golden image deliberately before preparing it.' }
$watcher = Join-Path $stateDir 'Watch-JitRunner.ps1'
Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'Watch-JitRunner.ps1') -Destination $watcher -Force
Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'Test-JitConfig.ps1') -Destination (Join-Path $stateDir 'Test-JitConfig.ps1') -Force
$action = New-ScheduledTaskAction -Execute "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" -Argument "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$watcher`" -RunnerDirectory `"$runnerDir`" -JitPath `"$JitPath`""
$trigger = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit ([TimeSpan]::Zero)
$taskPrincipal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Principal $taskPrincipal -Force | Out-Null
Write-Host 'Installed boot watcher. Shut down the clean golden image before converting it to a template.'
