# FS Factbase Dashboard Restarter
$ErrorActionPreference = "SilentlyContinue"

Write-Host "--- Restarting FS Factbase Dashboard ---" -ForegroundColor Cyan

# 1. Kill existing process on port 8000
$Port = 8000
$Process = Get-NetTCPConnection -LocalPort $Port | Select-Object -ExpandProperty OwningProcess -ErrorAction SilentlyContinue
if ($Process) {
    Write-Host "Found existing process $Process on port $Port. Terminating..." -ForegroundColor Yellow
    Stop-Process -Id $Process -Force
    Start-Sleep -Seconds 1
} else {
    Write-Host "No process found on port $Port." -ForegroundColor Gray
}

# 2. Get Root Directory
$RootDir = Get-Item ".." | Select-Object -ExpandProperty FullName
Write-Host "Project Root: $RootDir"

# 3. Check for Virtual Environment
$PythonExe = "$RootDir\.venv\Scripts\python.exe"
if (-not (Test-Path $PythonExe)) {
    Write-Host "Virtual environment not found at $PythonExe. Using system python." -ForegroundColor Red
    $PythonExe = "python"
}

# 4. Start the Dashboard from the root (to ensure imports work)
Write-Host "Starting Dashboard..." -ForegroundColor Green
$AppPath = "p03_Analytics_Dashboard\app.py"

# Run in a new job or process so it doesn't block the caller if needed, 
# but usually, we want to see the output.
# We'll run it directly here.
& $PythonExe $AppPath
