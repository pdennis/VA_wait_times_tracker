#!/bin/zsh
#
source .zshrc
cd /Users/davids/Documents/dev/BridgeEnv/VA_wait_times_tracker
source ../bin/activate
export PYTHONPATH=.
touch running_download_reports
env
pwd
../bin/python cli/download_reports.py
rm -fr running_download_reports
touch download_reports_last_run
