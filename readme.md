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

## System Components

### Data Collection
The script (`vadownloader.py`) performs the following:
1. Reads the master facility list to get all possible VA facility identifiers
2. For each facility ID, attempts to download current wait time data
3. Saves valid Excel files in a date-stamped folder
4. Creates a log of successful downloads to identify which facilities actually report data

The script specifically:
- Only saves files that return valid Excel data (Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet)
- Skips facilities that return errors or no data
- Maintains a 2-second delay between requests to avoid overwhelming the VA's servers

### Data Processing
The database script (`va_db_script.py`):
1. Creates and maintains a SQLite database of wait times
2. Processes Excel files from all date-stamped folders
3. Normalizes facility names and data
4. Tracks both new and established patient wait times
5. Maintains historical data over time

### Data Visualization
The visualization script (`va_viz.py`):
1. Provides an interactive Streamlit dashboard
2. Shows wait time trends over time
3. Allows filtering by:
   - Facility
   - Appointment type
4. Displays both new and established patient wait times
5. Includes summary statistics and raw data view

## Requirements
- Python 3.x
- Required packages:
```bash
pip install pandas requests sqlite3 streamlit plotly
```

## Usage

### Data Collection
```bash
python vadownloader.py
```
Creates a new folder with today's date (e.g., `va_facility_data_2025-02-19`) and downloads all available facility data.

### Database Updates
```bash
python va_db_script.py
```
Processes all data folders and updates the SQLite database with new data.

### Visualization
```bash
streamlit run va_viz.py
```
Launches the interactive visualization dashboard in your web browser.

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
- Expand analysis tools and visualizations
- Add automated trend detection and alerting
- Provide public access to the collected dataset
- Add geographic visualization of wait times
- Implement automated data quality checks

## Contributing
This is an open source project aimed at increasing transparency around veterans' healthcare access. Contributions, particularly in data analysis and visualization, are welcome.

## Data Notes
- Not all facility IDs in the master list actively report data
- Data collection began in February 2025
- Raw data files are saved with the naming convention: `facility_data_[STATION_ID].xlsx`
- The SQLite database maintains a normalized and cleaned version of all historical data
- Wait times are tracked separately for new and established patients
- Data is collected daily but some facilities may update less frequently

## Database Schema
The SQLite database (`va_wait_times.db`) contains:
- `facilities` table: Normalized facility names and IDs
- `wait_times` table: Historical wait time data including:
  - Collection date
  - Facility ID
  - Appointment type
  - Wait times for new and established patients
  - Data source information
