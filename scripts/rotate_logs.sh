#!/bin/bash

# 0 6,18 * * * cd ~/code/unhcr_module && pgrep -fx "/bin/bash ./scripts/rotate_logs.sh" > /dev/null || (/bin/bash ./scripts/rotate_logs.sh | tee -a ~/code/logs/rotate_logs.log 2>&1 && echo $(( $(cat ~/code/logs/run_count_rotate_logs.log 2>/dev/null || echo 0) + 1 )) > ~/code/logs/run_count_rotate_logs.log)

# Logrotate script to rotate logs and move them to a different drive
# This script is intended to be run as a cron job
# cat /etc/logrotate.d/unhcr
# /home/unhcr_admin/code/logs/*.log {
#     daily
#     rotate 7
#     compress
#     missingok
#     notifempty
# }


# Define variables
LOGROTATE_CONF="/etc/logrotate.conf"
SOURCE_LOG_DIR="/home/code/logs"
DESTINATION_LOG_DIR="/datadrive/logs"

# Run logrotate -- -f to force rotation
sudo logrotate -v "$LOGROTATE_CONF"

# Check if logrotate was successful
if [ $? -eq 0 ]; then
    echo "Logrotate completed successfully."
else
    echo "Logrotate encountered an error."
fi

# Move rotated log files to the destination directory
# rotate could do this, but not to a different drive, so we do it
# Assuming rotated files have a .1 extension (you can adjust this as needed)
sudo mv "$SOURCE_LOG_DIR"/*.1 "$DESTINATION_LOG_DIR"/

# Check if the move was successful
if [ $? -eq 0 ]; then
    echo "Moved rotated log files to $DESTINATION_LOG_DIR."
else
    echo "Failed to move log files."
fi

