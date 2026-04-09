@echo off
REM =====================================================
REM RCA PoC - Setup and Execution Script (Windows)
REM Comprehensive setup for running the RCA PoC
REM =====================================================

setlocal enabledelayedexpansion

echo.
echo =====================================================
echo RCA PoC - Asset Data Modernisation on Snowflake
echo Setup ^& Execution Script (Windows)
echo =====================================================
echo.

REM Check Python installation
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python 3.9+ is not installed
    pause
    exit /b 1
)

for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
echo ✅ Python %PYTHON_VERSION% detected

REM Navigate to project directory
cd /d "%~dp0"
set PROJECT_DIR=%cd%
echo 📂 Project directory: %PROJECT_DIR%

REM Create virtual environment
echo.
echo Setting up Python virtual environment...
if not exist "venv" (
    python -m venv venv
    echo ✅ Virtual environment created
) else (
    echo ✅ Virtual environment already exists
)

REM Activate virtual environment
echo Activating virtual environment...
call venv\Scripts\activate.bat
echo ✅ Virtual environment activated

REM Install dependencies
echo.
echo Installing Python dependencies...
python -m pip install --upgrade pip
pip install -r requirements.txt
echo ✅ Dependencies installed

REM Display configuration
echo.
echo =====================================================
echo Configuration Summary
echo =====================================================
echo Snowflake Account: TQWSLTQ-TW60698
echo Snowflake User: RANIT532
echo Database: RCA_POC_DB
echo Warehouse: RCA_ANALYTICS_WH
echo ---
echo 🔗 Before running, ensure you:
echo    1. Have run SQL setup scripts (sql/01-07)
echo    2. Updated Snowflake credentials if needed
echo    3. Have valid Snowflake account access
echo.

REM Run orchestration
echo =====================================================
echo Execution Options
echo =====================================================
echo.
echo 1. Run Data Quality ^& Reconciliation Orchestration
echo    python orchestrate.py
echo.
echo 2. Launch Streamlit Controls Cockpit
echo    cd streamlit_app ^&^& streamlit run app.py
echo.
echo 3. Run DYD Integration Example
echo    python examples\dyd_backend_integration.py
echo.
echo 4. Generate Sample DYD Configuration
echo    python examples\dyd_sample_config.py
echo.
echo =====================================================
echo.

set /p choice="Select option (1-4): "

if "%choice%"=="1" (
    echo.
    echo Running Orchestration...
    python orchestrate.py
) else if "%choice%"=="2" (
    echo.
    echo Launching Streamlit Dashboard...
    cd /d streamlit_app
    streamlit run app.py
) else if "%choice%"=="3" (
    echo.
    echo Running DYD Backend Integration Example...
    python examples\dyd_backend_integration.py
) else if "%choice%"=="4" (
    echo.
    echo Generating Sample DYD Configuration...
    python examples\dyd_sample_config.py
) else (
    echo Invalid option
    exit /b 1
)

pause
