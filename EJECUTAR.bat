@echo off
title ChileCompra Data Processor
echo.
echo ==============================================================
echo   ChileCompra Data Processor - Setup Automatico
echo   Autor: Guillermo Fuentes (djwillichile)
echo ==============================================================
echo.

:: Buscar Python en multiples ubicaciones
set PYTHON_CMD=

:: Opcion 1: py launcher
py --version >nul 2>&1
if %errorlevel% equ 0 (
    set PYTHON_CMD=py
    goto :found
)

:: Opcion 2: python en PATH
python --version >nul 2>&1
if %errorlevel% equ 0 (
    set PYTHON_CMD=python
    goto :found
)

:: Opcion 3: python3 en PATH
python3 --version >nul 2>&1
if %errorlevel% equ 0 (
    set PYTHON_CMD=python3
    goto :found
)

:: Opcion 4: Buscar en rutas comunes de Windows
for %%P in (
    "%LOCALAPPDATA%\Programs\Python\Python313\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python311\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python310\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python39\python.exe"
    "C:\Python313\python.exe"
    "C:\Python312\python.exe"
    "C:\Python311\python.exe"
    "C:\Python310\python.exe"
    "%USERPROFILE%\anaconda3\python.exe"
    "%USERPROFILE%\miniconda3\python.exe"
    "%PROGRAMFILES%\Python313\python.exe"
    "%PROGRAMFILES%\Python312\python.exe"
    "%PROGRAMFILES%\Python311\python.exe"
) do (
    if exist %%P (
        set PYTHON_CMD=%%P
        goto :found
    )
)

:: No se encontro Python - ofrecer instalacion automatica
echo [!] Python no fue encontrado en el sistema.
echo.
echo Deseas instalar Python 3.12 automaticamente? (S/N)
set /p INSTALL_CHOICE="> "
if /i "%INSTALL_CHOICE%" neq "S" (
    echo.
    echo Instalacion cancelada. Puedes instalar Python manualmente desde:
    echo https://www.python.org/downloads/
    echo Asegurate de marcar "Add Python to PATH".
    pause
    exit /b 1
)

:: Descargar Python
echo.
echo [INFO] Descargando Python 3.12 desde python.org...
echo        Esto puede tomar unos minutos...
echo.

set PYTHON_INSTALLER=%TEMP%\python-3.12.7-amd64.exe
set PYTHON_URL=https://www.python.org/ftp/python/3.12.7/python-3.12.7-amd64.exe

:: Usar PowerShell para descargar (disponible en Windows 10+)
powershell -Command "& {[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri '%PYTHON_URL%' -OutFile '%PYTHON_INSTALLER%'}" 2>nul

if not exist "%PYTHON_INSTALLER%" (
    :: Intentar con curl (disponible en Windows 10 1803+)
    curl -L -o "%PYTHON_INSTALLER%" "%PYTHON_URL%" 2>nul
)

if not exist "%PYTHON_INSTALLER%" (
    echo [ERROR] No se pudo descargar Python.
    echo Descargalo manualmente desde: https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [OK] Descarga completada.
echo.
echo [INFO] Instalando Python 3.12 (esto puede tomar 1-2 minutos)...
echo        Se instalara con "Add to PATH" activado.
echo.

:: Instalar Python silenciosamente con PrependPath (agrega al PATH)
"%PYTHON_INSTALLER%" /quiet InstallAllUsers=0 PrependPath=1 Include_test=0 Include_launcher=1

if %errorlevel% neq 0 (
    echo [WARN] La instalacion silenciosa fallo. Abriendo instalador interactivo...
    echo        IMPORTANTE: Marca "Add Python to PATH" en la primera pantalla.
    "%PYTHON_INSTALLER%"
)

:: Limpiar instalador
del "%PYTHON_INSTALLER%" 2>nul

:: Refrescar PATH en esta sesion
set "PATH=%LOCALAPPDATA%\Programs\Python\Python312;%LOCALAPPDATA%\Programs\Python\Python312\Scripts;%PATH%"

:: Verificar instalacion
py --version >nul 2>&1
if %errorlevel% equ 0 (
    set PYTHON_CMD=py
    goto :found
)

python --version >nul 2>&1
if %errorlevel% equ 0 (
    set PYTHON_CMD=python
    goto :found
)

:: Buscar en la ruta recien instalada
if exist "%LOCALAPPDATA%\Programs\Python\Python312\python.exe" (
    set "PYTHON_CMD=%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
    goto :found
)

echo [ERROR] Python se instalo pero no se puede encontrar.
echo Cierra esta ventana, abre una nueva terminal y ejecuta:
echo   python "%~dp0chilecompra_processor.py"
pause
exit /b 1

:found
echo [OK] Python encontrado: %PYTHON_CMD%
%PYTHON_CMD% --version
echo.

:: Instalar dependencias
echo [1/3] Instalando dependencias (pandas, pyarrow, requests)...
%PYTHON_CMD% -m pip install pandas pyarrow requests --quiet --upgrade 2>nul
if %errorlevel% neq 0 (
    echo [INFO] Intentando instalacion con --user...
    %PYTHON_CMD% -m pip install pandas pyarrow requests --user --quiet --upgrade
)
echo [OK] Dependencias instaladas.
echo.

:: Seleccion de periodo
echo [2/3] Selecciona el periodo a procesar:
echo.
echo   1) Todo el historico disponible (2015-2026) [recomendado]
echo   2) Periodo personalizado
echo.
set PERIOD_CHOICE=1
set /p PERIOD_CHOICE="Opcion (1-2) [1]: "

if "%PERIOD_CHOICE%"=="2" goto :custom_period

:: Procesamiento completo
echo.
echo [INFO] Iniciando descarga y procesamiento (2015-2026)...
echo        Esto puede tomar entre 30-60 minutos dependiendo de tu conexion.
echo        Los archivos ZIP pesan entre 15-50 MB cada uno (136 archivos).
echo.
%PYTHON_CMD% "%~dp0chilecompra_processor.py"
goto :after_run

:custom_period
echo.
set START_YEAR=2015
set END_YEAR=2026
set /p START_YEAR="  Anio inicial (2015-2026) [2015]: "
set /p END_YEAR="  Anio final   (2015-2026) [2026]: "
echo.
echo [INFO] Iniciando procesamiento del periodo %START_YEAR% - %END_YEAR%...
echo.
%PYTHON_CMD% "%~dp0chilecompra_processor.py" --start-year %START_YEAR% --end-year %END_YEAR%

:after_run
echo.
echo [3/3] Proceso finalizado.
echo.
echo Los archivos Parquet estan en: %~dp0data\processed\
echo.
echo ==============================================================
echo   SIGUIENTE PASO:
echo   Copia la carpeta data\processed\ a tu repositorio local
echo   chile-public-procurement-analysis\data\processed\
echo ==============================================================
echo.
pause
