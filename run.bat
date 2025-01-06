REM change to your venv path
call E:\_UNHCR\CODE\unhcr_module\.venv\Scripts\activate.bat
REM changed python path to make it work in Windows scheduler -- was running a different python
E:\_UNHCR\CODE\unhcr_module\.venv\Scripts\python.exe E:\_UNHCR\CODE\unhcr_module\unhcr\full_test.py --log INFO
