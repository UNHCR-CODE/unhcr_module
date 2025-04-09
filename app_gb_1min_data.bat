@echo off
REM change to your repo root dir
cd E:\_UNHCR\CODE\unhcr_module
e:

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

REM pip install --upgrade pip

REM any commandline vars, install unhcr module
IF "%1"=="" (
    venv\Scripts\python.exe -c "import unhcr" 2>NUL
    if %errorlevel% equ 0 (
        echo Module 'unhcr' is installed.
    ) else (
        echo Module 'unhcr' is not installed.
        %VENV_DIR%\Scripts\pip install .
    )
) ELSE (
    %VENV_DIR%\Scripts\pip install -r fedotreqs.txt
    %VENV_DIR%\Scripts\pip install .
)

REM changed python path to make it work in Windows scheduler -- was running a different python
%VENV_DIR%\Scripts\python.exe app_gb_1min_data.py --log INFO
deactivate
