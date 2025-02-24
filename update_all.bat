@echo off
REM change to your repo root dir
cd E:\_UNHCR\CODE\unhcr_module
e:

set VENV_DIR=venv

if not exist "%VENV_DIR%" (
    echo Creating virtual environment in %VENV_DIR%...
    python -m venv %VENV_DIR%
    echo Virtual environment created successfully.
    call venv\Scripts\activate.bat
    venv\Scripts\pip install -r requirements.txt
) else (
    echo Virtual environment "%VENV_DIR%" already exists.
    call venv\Scripts\activate.bat
)

REM pip install --upgrade pip

REM any commandline vars, install unhcr module
IF "%1"=="" (
    venv\Scripts\python.exe -c "import unhcr" 2>NUL
    if %errorlevel% equ 0 (
        echo Module 'unhcr' is installed.
    ) else (
        echo Module 'unhcr' is not installed.
        venv\Scripts\pip install .
    )
) ELSE (
    venv\Scripts\pip install -r requirements.txt
    venv\Scripts\pip install .
)

REM changed python path to make it work in Windows scheduler -- was running a different python
venv\Scripts\python.exe update_all.py --log INFO
deactivate
