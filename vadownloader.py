import pandas as pd
from datetime import datetime
import os
import time
from pathlib import Path
import requests

def create_dated_folder():
    """Create a folder with today's date for storing downloaded files"""
    today = datetime.now().strftime('%Y-%m-%d')
    folder_path = Path(f'va_facility_data_{today}')
    folder_path.mkdir(exist_ok=True)
    return folder_path

def download_facility_data(station_number, output_folder):
    """
    Download facility data for a given station number, saving with original filename
    """
    url = f'https://www.accesstocare.va.gov/FacilityPerformanceData/FacilityDataExcel?stationNumber={station_number}'
    
    try:
        response = requests.get(url)
        content_type = response.headers.get("Content-Type", "")
        
        if 'spreadsheetml.sheet' in content_type:
            # Get original filename from Content-Disposition header
            content_disposition = response.headers.get('Content-Disposition', '')
            if 'filename=' in content_disposition:
                # Extract filename from the header
                filename = content_disposition.split('filename=')[-1].strip('"\'')
            else:
                # Fallback to default naming if no filename provided
                filename = f'facility_data_{station_number}.xlsx'
            
            output_file = output_folder / filename
            with open(output_file, 'wb') as f:
                f.write(response.content)
            print(f'Downloaded Excel file for station {station_number} as {filename}')
            return True
        else:
            print(f'Skipped station {station_number} - received {content_type}')
            return False
            
    except Exception as e:
        print(f'Error downloading station {station_number}: {str(e)}')
        return False

def get_unique_station_ids(csv_path):
    """Read and process the simplified CSV to get station IDs"""
    try:
        # Read CSV with a single column named 'identifier'
        df = pd.read_csv(csv_path, keep_default_na=False)
        # Get unique values from the identifier column and clean them
        station_ids = df['identifier'].dropna().unique()
        station_ids = [str(id).strip() for id in station_ids if str(id).strip()]
        return sorted(set(station_ids))
    except Exception as e:
        print(f'Error processing CSV file: {str(e)}')
        return []

def main():
    csv_path = 'VAfacilities.csv'
    station_ids = get_unique_station_ids(csv_path)
    
    if not station_ids:
        print('No valid station IDs found in the CSV file.')
        return
    
    print(f'Found {len(station_ids)} unique station IDs')
    
    output_folder = create_dated_folder()
    print(f'Downloading files to: {output_folder}')
    
    successful_downloads = 0
    failed_downloads = []
    
    for station in station_ids:
        time.sleep(2)  # Be nice to the server
        
        if download_facility_data(station, output_folder):
            successful_downloads += 1
        else:
            failed_downloads.append(station)
    
    print('\nDownload Summary:')
    print(f'Successfully downloaded: {successful_downloads} Excel files')
    print(f'Skipped or failed: {len(failed_downloads)} stations')
    
    # Save the successfully downloaded station IDs for reference
    success_log = output_folder / 'successful_downloads.txt'
    with open(success_log, 'w') as f:
        f.write(f'Download completed at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
        f.write(f'Successfully downloaded {successful_downloads} Excel files\n\n')
        f.write('Station IDs with valid Excel files:\n')
        f.write('\n'.join(id for id in station_ids if id not in failed_downloads))

if __name__ == '__main__':
    main()