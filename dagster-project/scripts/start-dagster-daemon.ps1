param(
    [switch]$Restart
)

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$dagsterHome = Join-Path $projectRoot ".dagster-home"
$workspacePath = Join-Path $projectRoot "workspace.yaml"
$logDir = Join-Path $dagsterHome "logs"
$stdoutLog = Join-Path $logDir "daemon.stdout.log"
$stderrLog = Join-Path $logDir "daemon.stderr.log"
$pidFile = Join-Path $dagsterHome "daemon.pid"

New-Item -ItemType Directory -Force -Path $dagsterHome, $logDir | Out-Null

if (Test-Path $pidFile) {
    $existingPid = (Get-Content -Path $pidFile -ErrorAction SilentlyContinue | Select-Object -First 1)
    if ($existingPid) {
        $existingProcess = Get-Process -Id ([int]$existingPid) -ErrorAction SilentlyContinue
        if ($existingProcess -and -not $Restart) {
            Write-Output "Dagster daemon already running with PID $existingPid"
            exit 0
        }
        if ($existingProcess -and $Restart) {
            Stop-Process -Id $existingProcess.Id -Force
            Start-Sleep -Seconds 2
        }
    }
}

$command = @"
`$env:DAGSTER_HOME = '$dagsterHome'
Set-Location '$projectRoot'
uv run dagster-daemon run -w '$workspacePath' --log-level info
"@

$process = Start-Process `
    -FilePath "powershell.exe" `
    -ArgumentList "-NoProfile", "-Command", $command `
    -WorkingDirectory $projectRoot `
    -WindowStyle Hidden `
    -RedirectStandardOutput $stdoutLog `
    -RedirectStandardError $stderrLog `
    -PassThru

Set-Content -Path $pidFile -Value $process.Id
Write-Output "Started Dagster daemon with PID $($process.Id)"
Write-Output "stdout: $stdoutLog"
Write-Output "stderr: $stderrLog"
