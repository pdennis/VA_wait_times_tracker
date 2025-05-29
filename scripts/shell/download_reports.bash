#!/bin/bash
#
cd /Users/davids/Documents/dev/BridgeEnv/VA_wait_times_tracker
source ../bin/activate
export PYTHONPATH=.
python cli/download_reports.py
touch download_reports_last_run
