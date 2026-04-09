@echo off
setlocal
cd /d "%~dp0"
powershell -ExecutionPolicy Bypass -File "%~dp0launch_webui.ps1" -RestartIfRunning
endlocal
