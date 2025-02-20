import sqlite3
import json
from pathlib import Path

def get_wait_times_data(db_path='va_wait_times.db'):
    """
    Get wait times data in a format suitable for visualization.
    Returns a dictionary containing facilities, appointment types, and wait time data.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get list of facilities
    cursor.execute("""
        SELECT facility_name 
        FROM facilities 
        ORDER BY facility_name
    """)
    facilities = [row[0] for row in cursor.fetchall()]
    
    # Get list of appointment types
    cursor.execute("""
        SELECT DISTINCT appointment_type 
        FROM wait_times 
        ORDER BY appointment_type
    """)
    appointment_types = [row[0] for row in cursor.fetchall()]
    
    # Function to get wait times for a specific facility and appointment type
    def get_facility_wait_times(facility_name, appointment_type):
        cursor.execute("""
            SELECT 
                w.collection_date,
                w.established_patient_wait_days,
                w.new_patient_wait_days
            FROM wait_times w
            JOIN facilities f ON w.facility_id = f.id
            WHERE f.facility_name = ? 
            AND w.appointment_type = ?
            ORDER BY w.collection_date
        """, (facility_name, appointment_type))
        
        return [{
            'date': row[0],
            'establishedPatient': row[1],
            'newPatient': row[2]
        } for row in cursor.fetchall()]
    
    # Get initial data for first facility and appointment type
    initial_data = None
    if facilities and appointment_types:
        initial_data = get_facility_wait_times(facilities[0], appointment_types[0])
    
    result = {
        'facilities': facilities,
        'appointmentTypes': appointment_types,
        'initialData': initial_data
    }
    
    conn.close()
    return result

if __name__ == '__main__':
    # Example usage
    data = get_wait_times_data()
    print(json.dumps(data, indent=2))
