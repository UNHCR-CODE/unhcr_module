#!/bin/bash

# change to your repo root dir
cd ~/code/unhcr_module || exit 1

# Activate the virtual environment directory
VENV_DIR="venvl"

if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment in $VENV_DIR..."
    python3 -m venv $VENV_DIR
    echo "Virtual environment created successfully."
    source venvl/bin/activate
    pip install -r requirements.txt
else
    echo "Virtual environment '$VENV_DIR' already exists."
    source venvl/bin/activate
fi

pip install --upgrade pip
python3 -c "import unhcr" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "Module 'unhcr' is installed."
else
    echo "Module 'unhcr' is not installed."
    pip install .
fi

# Run the sm_weather.py script
python3 sm_weather.py --log INFO 
EXIT_CODE=$?  # Store the exit code of Python

deactivate

# If Python script fails, log the exit code
if [ $EXIT_CODE -ne 0 ]; then
    echo "$(date): sm_weather.py FAILED with exit code $EXIT_CODE" >> error_sm_weather.log
fi

# Exit with the same exit code as Python
exit $EXIT_CODE
