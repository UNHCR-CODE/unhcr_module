REM change to your repo root dir
cd E:\_UNHCR\CODE\unhcr_module
e:

set VENV_DIR=.venv

if not exist "%VENV_DIR%" (
    echo Creating virtual environment in %VENV_DIR%...
    python -m venv %VENV_DIR%
    echo Virtual environment created successfully.
) else (
    echo Virtual environment "%VENV_DIR%" already exists.
)

call .venv\Scripts\activate.bat
pip install -r requirements.txt
pip install .
REM changed python path to make it work in Windows scheduler -- was running a different python
.venv\Scripts\python.exe unhcr\full_test.py --log INFO
