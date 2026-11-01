$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$apiPython = Join-Path $projectRoot '.venv\Scripts\python.exe'
$node = Join-Path $env:USERPROFILE '.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe'
$vite = Join-Path $projectRoot 'frontend\node_modules\vite\bin\vite.js'
$logDir = Join-Path $projectRoot 'logs'
New-Item -ItemType Directory -Path $logDir -Force | Out-Null

foreach ($requiredPath in @($apiPython, $node, $vite)) {
    if (-not (Test-Path -LiteralPath $requiredPath)) {
        throw "Scanner runtime file not found: $requiredPath"
    }
}

function Test-LocalPort([int]$Port) {
    $client = [System.Net.Sockets.TcpClient]::new()
    try {
        $client.Connect('127.0.0.1', $Port)
        return $client.Connected
    } catch {
        return $false
    } finally {
        $client.Dispose()
    }
}

if (-not (Test-LocalPort 8000)) {
    Start-Process -FilePath $apiPython `
        -ArgumentList @('-m', 'uvicorn', 'main:app', '--app-dir', 'backend', '--host', '127.0.0.1', '--port', '8000') `
        -WorkingDirectory $projectRoot -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $logDir 'api-output.log') `
        -RedirectStandardError (Join-Path $logDir 'api-error.log') | Out-Null

    $apiReady = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        Start-Sleep -Milliseconds 500
        if (Test-LocalPort 8000) { $apiReady = $true; break }
    }
    if (-not $apiReady) { throw "The API did not start. Check $logDir\api-error.log" }
}

if (-not (Test-LocalPort 5173)) {
    Start-Process -FilePath $node -ArgumentList @('node_modules/vite/bin/vite.js', '--host', '127.0.0.1') `
        -WorkingDirectory (Join-Path $projectRoot 'frontend') -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $logDir 'frontend-output.log') `
        -RedirectStandardError (Join-Path $logDir 'frontend-error.log') | Out-Null

    $frontendReady = $false
    for ($attempt = 0; $attempt -lt 30; $attempt++) {
        Start-Sleep -Milliseconds 500
        if (Test-LocalPort 5173) { $frontendReady = $true; break }
    }
    if (-not $frontendReady) { throw "The interface did not start. Check $logDir\frontend-error.log" }
}

Start-Process 'http://localhost:5173/'
Write-Host 'Delta Signal Scanner is running at http://localhost:5173/' -ForegroundColor Green
