# VA Facility Wait Time Tracker

## Project Overview
This project aims to track changes in wait times at VA facilities over time, particularly in light of recent staffing changes and budget cuts at the Department of Veterans Affairs. While the VA provides current wait time data for individual facilities, there is no publicly available historical dataset to analyze trends and impacts of policy changes.

## Data Sources
- Current wait time data: Individual facility performance data from the VA's Access to Care tool
  - Source: https://www.accesstopwt.va.gov/FacilityPerformanceData/
  - Data is available as XLSX downloads for each facility
- Facility identifiers: Master list of VA facilities 
  - Source: https://www.va.gov/directory/guide/rpt_fac_list.cfm?sort=StaID&list_by=all&oid=all
  - Archived copy included in repository as `VAfacilities.csv`

## How It Works
The script (`vadownloader.py`) performs the following:

1. Reads the master facility list to get all possible VA facility identifiers
2. For each facility ID, attempts to download current wait time data
3. Saves valid Excel files in a date-stamped folder
4. Creates a log of successful downloads to identify which facilities actually report data

The script specifically:
- Only saves files that return valid Excel data (Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet)
- Skips facilities that return errors or no data
- Maintains a 2-second delay between requests to avoid overwhelming the VA's servers

## Requirements
- Python 3.x
- Required packages: `pandas`, `requests`

Install dependencies:
```bash
pip install pandas requests
```

## Usage
```bash
python vadownloader.py
```

The script will create a new folder with today's date (e.g., `va_facility_data_2025-02-19`) and download all available facility data.

## Project Goals
1. Create a historical dataset of VA facility wait times that can be used to analyze trends
2. Track potential impacts of policy changes, including:
   - Recent staffing reductions
   - Grant freezes
   - Budget cuts
3. Provide transparency and data for public analysis of VA healthcare access

## Future Plans
- Modify the script to only check facilities that are known to report data (based on initial survey)
- Implement daily automated data collection
- Create analysis tools for the collected historical data
- Provide public access to the collected dataset

## Contributing
This is an open source project aimed at increasing transparency around veterans' healthcare access. Contributions, particularly in data analysis and visualization, are welcome.

## Data Notes
- Not all facility IDs in the master list actively report wait time data
- Data collection began in February 2025
- Files are saved with the naming convention: `facility_data_[STATION_ID].xlsx`
