$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$dagsterHome = Join-Path $projectRoot ".dagster-home"
$workspacePath = Join-Path $projectRoot "workspace.yaml"
$startScript = Join-Path $PSScriptRoot "start-dagster-daemon.ps1"

& $startScript | Out-Host

$env:DAGSTER_HOME = $dagsterHome
Set-Location $projectRoot
uv run dagster job launch -w $workspacePath -j rebuild_lakehouse
