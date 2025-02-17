#!/bin/bash

# change to your repo root dir
cd /mnt/e/_UNHCR/CODE/unhcr_module || exit 1

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

python3 -c "import unhcr" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "Module 'unhcr' is installed."
else
    echo "Module 'unhcr' is not installed."
    pip install .
fi

# Run the update_all.py script
python3 update_all.py --log INFO

# Deactivate the virtual environment
deactivate
