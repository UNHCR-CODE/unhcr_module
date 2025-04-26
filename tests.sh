#!/bin/bash

# Navigate to the tests directory
cd /home/unhcr_admin/code/unhcr_module/tests

# Set the PYTHONPATH
export PYTHONPATH=/home/unhcr_admin/code/unhcr_module/unhcr

# Run pytest
pytest -v --cache-clear --cov=.. --cov-report=html --env=/home/unhcr_admin/code/unhcr_module/.env --log=INFO

cd /home/unhcr_admin/code/unhcr_module