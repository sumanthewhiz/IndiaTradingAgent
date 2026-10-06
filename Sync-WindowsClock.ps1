$ErrorActionPreference = 'Stop'
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = [Security.Principal.WindowsPrincipal]::new($identity)
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'Open PowerShell as Administrator, then run this script. No elevation or clock changes were attempted.'
}
$service = Get-Service -Name W32Time -ErrorAction Stop
if ($service.StartType -eq 'Disabled') {
    throw 'Windows Time is disabled. Ask your administrator to restore the approved time policy.'
}
Start-Service -Name W32Time
& w32tm /resync /rediscover
if ($LASTEXITCODE -ne 0) {
    throw 'Windows time synchronization failed. Verify the configured time source and network with your administrator.'
}
& w32tm /query /status
if ($LASTEXITCODE -ne 0) {
    throw 'Could not verify Windows time-service status.'
}
Write-Output 'Requested synchronization using the configured Windows time source. No time-server policy was changed.'
