# Work Log Admin - start/restart script
# Double-click or run: powershell -File start.ps1

$port = 8800
$pidFile = Join-Path $PSScriptRoot ".uvicorn.pid"

# Kill any existing process on the port
$listening = netstat -ano | Select-String "127.0.0.1:$port\s+.*LISTENING"
foreach ($line in $listening) {
    $procId = ($line.ToString().Trim() -split '\s+')[-1]
    if ($procId -match '^\d+$') {
        Write-Host "Killing stale process PID $procId on port $port"
        Stop-Process -Id ([int]$procId) -Force -ErrorAction SilentlyContinue
    }
}

# Also kill by saved PID if the file exists
if (Test-Path $pidFile) {
    $oldPid = Get-Content $pidFile -ErrorAction SilentlyContinue
    if ($oldPid -match '^\d+$') {
        Stop-Process -Id ([int]$oldPid) -Force -ErrorAction SilentlyContinue
    }
    Remove-Item $pidFile -Force
}

Start-Sleep -Seconds 1

# Start uvicorn
Set-Location $PSScriptRoot
$proc = Start-Process -FilePath ".venv\Scripts\uvicorn.exe" `
    -ArgumentList "app:app", "--host", "127.0.0.1", "--port", "$port" `
    -WindowStyle Normal -PassThru

# Save PID for next restart
$proc.Id | Out-File $pidFile -Encoding ascii
Write-Host "Started on http://127.0.0.1:$port (PID $($proc.Id))"
Write-Host "To restart, run this script again."
