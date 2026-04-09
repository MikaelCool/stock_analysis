param(
    [switch]$RestartIfRunning
)

$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$pythonExe = Join-Path $projectRoot '.venv\Scripts\python.exe'
$logDir = Join-Path $projectRoot 'logs'
$hostUrl = 'http://127.0.0.1:8000'
$healthUrl = "$hostUrl/api/health"
$startupTimeoutSeconds = 90

if (-not (Test-Path $pythonExe)) {
    throw "Python virtual environment not found: $pythonExe"
}

if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}

$stdout = Join-Path $logDir 'webui_stdout.log'
$stderr = Join-Path $logDir 'webui_stderr.log'

$env:PYTHONUTF8 = '1'
$env:PYTHONIOENCODING = 'utf-8'

try {
    chcp 65001 | Out-Null
    [Console]::InputEncoding = [System.Text.UTF8Encoding]::new($false)
    [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
    $OutputEncoding = [System.Text.UTF8Encoding]::new($false)
}
catch {
}

function Test-WebUiReady {
    param(
        [string]$Url
    )

    try {
        $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 3
        return $response.StatusCode -eq 200
    }
    catch {
        return $false
    }
}

function Get-ProjectWebUiProcessInfo {
    try {
        $connection = Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue |
            Select-Object -First 1
        if (-not $connection) {
            return $null
        }

        $processInfo = Get-CimInstance Win32_Process -Filter "ProcessId = $($connection.OwningProcess)" -ErrorAction SilentlyContinue
        if (-not $processInfo) {
            return $null
        }

        $commandLine = ''
        if ($null -ne $processInfo.CommandLine) {
            $commandLine = [string]$processInfo.CommandLine
        }
        if ($commandLine -like '*D:\codex\daily_stock_analysis*' -and $commandLine -like '*main.py*') {
            return [pscustomobject]@{
                Id = [int]$connection.OwningProcess
                CommandLine = $commandLine
            }
        }
    }
    catch {
    }

    return $null
}

if (Test-WebUiReady -Url $healthUrl) {
    if (-not $RestartIfRunning) {
        Start-Process $hostUrl | Out-Null
        Write-Output "Web UI is already running: $hostUrl"
        exit 0
    }

    $existingProcess = Get-ProjectWebUiProcessInfo
    if (-not $existingProcess) {
        Start-Process $hostUrl | Out-Null
        Write-Output "Port 8000 is already serving the site, but the owning process is not recognized as this project. Opened browser only: $hostUrl"
        exit 0
    }

    Stop-Process -Id $existingProcess.Id -Force
    Start-Sleep -Seconds 2
}

$proc = Start-Process `
    -FilePath $pythonExe `
    -ArgumentList 'main.py','--webui-only','--host','0.0.0.0','--port','8000' `
    -WorkingDirectory $projectRoot `
    -RedirectStandardOutput $stdout `
    -RedirectStandardError $stderr `
    -PassThru

$deadline = (Get-Date).AddSeconds($startupTimeoutSeconds)
while ((Get-Date) -lt $deadline) {
    if ($proc.HasExited) {
        throw "Web UI exited early. Check logs:`nSTDOUT=$stdout`nSTDERR=$stderr"
    }

    if (Test-WebUiReady -Url $healthUrl) {
        Start-Process $hostUrl | Out-Null
        Write-Output "Web UI started: $hostUrl"
        Write-Output "PID=$($proc.Id)"
        Write-Output "STDOUT=$stdout"
        Write-Output "STDERR=$stderr"
        exit 0
    }

    Start-Sleep -Seconds 2
}

throw "Timed out waiting for Web UI. Check logs:`nSTDOUT=$stdout`nSTDERR=$stderr"
