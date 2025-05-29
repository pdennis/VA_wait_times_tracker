#!/bin/bash
#
pwd
cd /Users/davids/Documents/dev/BridgeEnv/VA_wait_times_tracker
pwd
source ../bin/activate
export PYTHONPATH=.
touch running_download_reports
../bin/python cli/download_reports.py
rm -fr running_download_reports
touch download_reports_last_run
