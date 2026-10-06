$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
$python = Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python)) {
    Write-Error 'The project environment is missing. Follow DASHBOARD_GUIDE.md to install it.'
    exit 1
}
& $python -m india_trader dashboard
exit $LASTEXITCODE
