@echo off
REM change to your repo root dir
cd /d E:\_UNHCR\CODE\unhcr_module

set VENV_DIR=vfedot

if not exist "%VENV_DIR%" (
    echo Creating virtual environment in %VENV_DIR%...
    python -m venv %VENV_DIR%
    echo Virtual environment created successfully.
    call %VENV_DIR%\Scripts\activate.bat
    %VENV_DIR%\Scripts\pip install -r fedotreqs.txt
) else (
    echo Virtual environment "%VENV_DIR%" already exists.
    call %VENV_DIR%\Scripts\activate.bat
)

cd tests
%VENV_DIR%\Scripts\python.exe -c "import unhcr" 2>NUL
if %errorlevel% equ 0 (
    echo Module '<module_name>' is installed.
) else (
    echo Module '<module_name>' is not installed.
    %VENV_DIR%\Scripts\pip install ..\
)
cd /d E:\_UNHCR\CODE\unhcr_module\tests
echo XXXXX %CD%
pytest -v --cache-clear --cov=.. --cov-report=html --env=E:/_UNHCR/CODE/unhcr_module/.env --log=INFO

cd ..
deactivate

