@echo off
REM change to your repo root dir
cd /d E:\_UNHCR\CODE\unhcr_module

set VENV_DIR=test_venv

if not exist "%VENV_DIR%" (
    echo Creating virtual environment in %VENV_DIR%...
    python -m venv %VENV_DIR%
    echo Virtual environment created successfully.
    call test_venv\Scripts\activate.bat
    test_venv\Scripts\pip install -r requirements.txt
) else (
    echo Virtual environment "%VENV_DIR%" already exists.
    call test_venv\Scripts\activate.bat
)

cd tests
test_venv\Scripts\python.exe -c "import unhcr" 2>NUL
if %errorlevel% equ 0 (
    echo Module '<module_name>' is installed.
) else (
    echo Module '<module_name>' is not installed.
    test_venv\Scripts\pip install ..\
)
cd /d E:\_UNHCR\CODE\unhcr_module\tests
echo XXXXX %CD%
pytest -v --cache-clear --cov=.. --cov-report=html --env=E:/_UNHCR/CODE/unhcr_module/.env --log=INFO

cd ..
deactivate

