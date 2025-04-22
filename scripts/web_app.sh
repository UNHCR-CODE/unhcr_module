#!/bin/bash

# every 1 min
# */1 * * * * cd /home/unhcr_admin/code/unhcr_module && pgrep -fx "sudo /bin/bash ./scripts/web_app.sh" > /dev/null || (sudo /bin/bash ./scripts/web_app.sh | sudo tee -a /datadrive/logs/web_app.log 2>&1 && sudo bash -c 'echo $(( $(cat /datadrive/logs/run_count_web_app.log 2>/dev/null || echo 0) + 1 )) > /datadrive/logs/run_count_web_app.log')


# Check if the application is alive
if ! curl -s --head --request GET http://localhost:5000/alive | grep "200 OK" > /dev/null; then
    echo "Service is down, killing the process..."

    # Get the PID of the running web app
    # Loop until no PID is found
while true; do
    # Get the PIDs of the running web app, excluding 'grep' itself
    pids=$(ps aux | grep "python3 ./web_app/web_app.py" | grep -v "grep" | awk '{print $2}')

    # Check if any PIDs are found
    if [ -n "$pids" ]; then
        # Loop over each PID and kill it
        for pid in $pids; do
            sudo kill -9 "$pid" && echo "Process with PID $pid killed."
        done
    else
        echo "No running process found for 'python3 web_app/web_app.py'."
        break  # Exit the loop if no process is found
    fi

    # Optional: Sleep for a few seconds before checking again
    sleep 2
done

    # Restart the service
    echo "Restarting the web application..."

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
        pip install --no-deps -r fedotreqs.txt
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
    python3 ./web_app/web_app.py --log INFO
    EXIT_CODE=$?  # Store the exit code of Python

    deactivate

    # If Python script fails, log the exit code
    if [ $EXIT_CODE -ne 0 ]; then
        echo "$(date): web_app.py FAILED with exit code $EXIT_CODE" >> /datadrive/logs/error_web_app.log
    fi

    # Exit with the same exit code as Python
    exit $EXIT_CODE


    echo "Service restarted."
else
    echo "Service is alive."
fi
