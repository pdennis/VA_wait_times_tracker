import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import os
import re

def init_database(db_path):
    """Initialize the SQLite database with necessary tables"""
    conn = sqlite3.connect(db_path)
    
    # Create tables
    conn.execute('''
    CREATE TABLE IF NOT EXISTS facilities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        facility_name TEXT UNIQUE NOT NULL
    )''')
    
    conn.execute('''
    CREATE TABLE IF NOT EXISTS wait_times (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        facility_id INTEGER NOT NULL,
        collection_date DATE NOT NULL,
        appointment_type TEXT NOT NULL,
        established_patient_wait_days REAL,
        new_patient_wait_days REAL,
        data_source TEXT,
        FOREIGN KEY (facility_id) REFERENCES facilities (id),
        UNIQUE(facility_id, collection_date, appointment_type)
    )''')
    
    # Create indices for better query performance
    conn.execute('CREATE INDEX IF NOT EXISTS idx_facility_date ON wait_times (facility_id, collection_date)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_appointment_type ON wait_times (appointment_type)')
    
    return conn

def clean_facility_name(filename):
    """Extract clean facility name from filename"""
    # Remove the UTF8 suffix and file extension
    name = filename.replace('filenameUTF8', '').replace('.xlsx22', '')
    name = name.replace(' - Facility Data', '')
    
    # Clean up any remaining special characters
    name = re.sub(r'["\';]', '', name)
    return name.strip()

def get_or_create_facility_id(conn, facility_name):
    """Get facility ID from database or create if doesn't exist"""
    cursor = conn.cursor()
    
    # Try to get existing facility
    cursor.execute('SELECT id FROM facilities WHERE facility_name = ?', (facility_name,))
    result = cursor.fetchone()
    
    if result:
        return result[0]
    
    # Create new facility if doesn't exist
    cursor.execute('INSERT INTO facilities (facility_name) VALUES (?)', (facility_name,))
    conn.commit()
    return cursor.lastrowid

def adapt_date(date):
    """Adapt Python date to SQLite date string"""
    return date.isoformat()

def process_excel_file(conn, file_path):
    """Process a single Excel file and insert data into database"""
    try:
        # Clean facility name from filename
        facility_name = clean_facility_name(file_path.name)
        facility_id = get_or_create_facility_id(conn, facility_name)
        
        # Read the Excel file
        df = pd.read_excel(file_path, sheet_name='Wait Times')
        
        # Convert 'N/A' to None for SQL NULL
        df = df.replace('N/A', None)
        
        # Get collection date from the file
        collection_date = pd.to_datetime(df['ReportDate'].iloc[0]).date()
        
        # Prepare data for insertion
        data_to_insert = []
        for _, row in df.iterrows():
            data_to_insert.append((
                facility_id,
                adapt_date(collection_date),
                row['AppointmentType'],
                row['EstablishedPatients'],
                row['NewPatients'],
                row['DataSource']
            ))
        
        # Insert data using a single transaction
        cursor = conn.cursor()
        cursor.executemany('''
            INSERT OR REPLACE INTO wait_times 
            (facility_id, collection_date, appointment_type, 
             established_patient_wait_days, new_patient_wait_days, data_source)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', data_to_insert)
        
        conn.commit()
        return True
        
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        conn.rollback()
        return False

def find_data_folders():
    """Find all VA facility data folders in the current directory"""
    pattern = re.compile(r'va_facility_data_\d{4}-\d{2}-\d{2}')
    return [d for d in Path('.').iterdir() 
            if d.is_dir() and pattern.match(d.name)]

def process_all_facility_data(db_path='va_wait_times.db'):
    """Process all facility data from all available data folders"""
    print(f"Creating/connecting to database at {db_path}")
    conn = init_database(db_path)
    
    # Find all data folders
    data_folders = find_data_folders()
    if not data_folders:
        print("No data folders found matching pattern: va_facility_data_YYYY-MM-DD")
        return
    
    print(f"Found {len(data_folders)} data folders to process")
    
    # Process files from all folders
    total_successful_imports = 0
    total_failed_imports = 0
    
    for folder in sorted(data_folders):
        print(f"\nProcessing folder: {folder}")
        excel_files = list(folder.glob('*.xlsx*'))
        folder_total = len(excel_files)
        folder_successful = 0
        folder_failed = 0
        
        for file_path in excel_files:
            if process_excel_file(conn, file_path):
                folder_successful += 1
                total_successful_imports += 1
            else:
                folder_failed += 1
                total_failed_imports += 1
                
            if folder_successful % 50 == 0 and folder_successful > 0:
                print(f"Processed {folder_successful}/{folder_total} files in current folder...")
        
        print(f"Folder {folder} complete: {folder_successful} successful, {folder_failed} failed")
    
    # Print summary statistics
    print("\nOverall Import Summary:")
    print(f"Successfully imported: {total_successful_imports} files")
    print(f"Failed imports: {total_failed_imports} files")
    
    # Get database statistics
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(*) FROM facilities')
    facility_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM wait_times')
    record_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT collection_date) FROM wait_times')
    date_count = cursor.fetchone()[0]
    
    cursor.execute('''
        SELECT MIN(collection_date), MAX(collection_date) 
        FROM wait_times
    ''')
    date_range = cursor.fetchone()
    
    print(f"\nDatabase Statistics:")
    print(f"Total facilities: {facility_count}")
    print(f"Total wait time records: {record_count}")
    print(f"Days of data: {date_count}")
    print(f"Date range: {date_range[0]} to {date_range[1]}")
    
    conn.close()

if __name__ == '__main__':
    process_all_facility_data()