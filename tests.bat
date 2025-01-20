@echo off
REM change to your repo root dir
cd E:\_UNHCR\CODE\unhcr_module
e:

set VENV_DIR=.venv

if not exist "%VENV_DIR%" (
    echo Creating virtual environment in %VENV_DIR%...
    python -m venv %VENV_DIR%
    echo Virtual environment created successfully.
    call .venv\Scripts\activate.bat
    .venv\Scripts\pip install -r requirements.txt
) else (
    echo Virtual environment "%VENV_DIR%" already exists.
    call .venv\Scripts\activate.bat
)

.venv\Scripts\python.exe -c "import unhcr" 2>NUL
if %errorlevel% equ 0 (
    echo Module '<module_name>' is installed.
) else (
    echo Module '<module_name>' is not installed.
    .venv\Scripts\pip install .
)

cd tests
pytest -v --cache-clear --cov=.. --cov-report=html
cd ..
deactivate

