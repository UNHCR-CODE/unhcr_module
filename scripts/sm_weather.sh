#!/bin/bash

# every 6 hours
# 0 */6 * * * cd /datadrive/unhcr_module && pgrep -fx "sudo /bin/bash ./scripts/sm_weather.sh" > /dev/null || (sudo /bin/bash ./scripts/sm_weather.sh | sudo tee -a /datadrive/logs/sm_weather.log 2>&1 && sudo bash -c 'echo $(( $(cat /datadrive/logs/run_count_sm_weather.log 2>/dev/null || echo 0) + 1 )) > /datadrive/logs/run_count_sm_weather.log')


# change to your repo root dir
cd /home/unhcr_admin/code/unhcr_module || exit 1
echo "Current directory: $(pwd)"
echo "Current user: $(whoami)"

# Activate the virtual environment directory
VENV_DIR="vfedot"

if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment in $VENV_DIR..."
    python3 -m venv $VENV_DIR
    echo "Virtual environment created successfully."
    source $VENV_DIR/bin/activate
    pip install -r fedotreqs.txt
else
    echo "Virtual environment '$VENV_DIR' already exists."
    source $VENV_DIR/bin/activate
fi

#pip install --upgrade pip

if [ -z "$1" ]; then
    python3 -c "import unhcr" 2>/dev/null
    if [ $? -eq 0 ]; then
        echo "Module 'unhcr' is installed."
    else
        echo "Module 'unhcr' is not installed."
        pip install .
    fi
else
    pip install -r fedotreqs.txt
    pip install .
fi

# Run the sm_weather.py script
python3 app_sm_weather.py --log INFO 
EXIT_CODE=$?  # Store the exit code of Python

deactivate

# If Python script fails, log the exit code
if [ $EXIT_CODE -ne 0 ]; then
    echo "$(date): app_sm_weather.py FAILED with exit code $EXIT_CODE" >> /datadrive/logs/error_sm_weather.log
fi

# Exit with the same exit code as Python
exit $EXIT_CODE
