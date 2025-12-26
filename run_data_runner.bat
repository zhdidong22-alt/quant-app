@echo off
setlocal EnableExtensions

set PROJECT=C:\Users\admin\Desktop\quant-app
set LOGDIR=%PROJECT%\logs
if not exist "%LOGDIR%" mkdir "%LOGDIR%"

for /f "tokens=1-3 delims=/- " %%a in ("%date%") do set D=%%a%%b%%c
for /f "tokens=1-3 delims=:." %%a in ("%time%") do set T=%%a%%b%%c
set LOG=%LOGDIR%\data_runner_%D%_%T%.log

echo ==== %date% %time% : start ====>> "%LOG%"
cd /d "%PROJECT%" >> "%LOG%" 2>&1

echo ---- python (venv) ---->> "%LOG%"
"%PROJECT%\venv\Scripts\python.exe" -V >> "%LOG%" 2>&1

echo ---- docker ---->> "%LOG%"
where docker >> "%LOG%" 2>&1
docker compose -f "%PROJECT%\docker\docker-compose.yml" up -d >> "%LOG%" 2>&1

echo ---- run main_data_runner (unbuffered) ---->> "%LOG%"
"%PROJECT%\venv\Scripts\python.exe" -u -m app.main_data_runner >> "%LOG%" 2>&1
echo PY_EXITCODE=%ERRORLEVEL%>> "%LOG%"

echo ==== %date% %time% : end ====>> "%LOG%"

endlocal