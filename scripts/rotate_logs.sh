#!/bin/bash

# Optional: uncomment to enable
# 0 6,18 * * * cd /datadrive/unhcr_module && pgrep -fx "/bin/bash ./scripts/rotate_logs.sh" > /dev/null || (/bin/bash ./scripts/rotate_logs.sh | tee -a /datadrive/logs/rotate_logs.log 2>&1 && sudo bash -c 'echo $(( $(cat /datadrive/logs/run_count_rotate_logs.log 2>/dev/null || echo 0) + 1 )) > /datadrive/logs/run_count_rotate_logs.log')

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
SOURCE_LOG_DIR="/home/unhcr_admin/code/logs"
DESTINATION_LOG_DIR="/datadrive/logs"

# Run logrotate -- -f to force rotation
sudo logrotate -v -f "$LOGROTATE_CONF"

# Check if logrotate was successful
if [ $? -eq 0 ]; then
    echo "Logrotate completed successfully."
else
    echo "Logrotate encountered an error."
fi

#!!!! this is not quite right moves 1.gz files only
# Move rotated log files to the destination directory
# rotate could do this, but not to a different drive, so we do it
# Assuming rotated files have a .1 extension (you can adjust this as needed)
# Check for .gz files in the source directory
# if ls "$SOURCE_LOG_DIR"/*.gz 1> /dev/null 2>&1; then
#     # Move rotated log files to the destination directory
#     mv "$SOURCE_LOG_DIR"/*.gz "$DESTINATION_LOG_DIR"/

#     # Check if the move was successful
#     if [ $? -eq 0 ]; then
#         echo "Moved rotated log files to $DESTINATION_LOG_DIR."
#     else
#         echo "Failed to move log files."
#     fi
# else
#     echo "No .gz files found in $SOURCE_LOG_DIR. Nothing to move."
# fi

