import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px

def load_data(db_path='va_wait_times.db'):
    """Load data from SQLite database"""
    conn = sqlite3.connect(db_path)
    
    # Get all wait times data joined with facility names
    df = pd.read_sql("""
        SELECT 
            f.facility_name,
            w.collection_date,
            w.appointment_type,
            w.established_patient_wait_days,
            w.new_patient_wait_days
        FROM wait_times w
        JOIN facilities f ON w.facility_id = f.id
    """, conn)
    
    conn.close()
    return df

def main():
    st.title('VA Facility Wait Times Analysis')
    
    # Load data
    df = load_data()
    
    # Sidebar filters
    st.sidebar.header('Filters')
    
    # Facility selection
    facilities = sorted(df['facility_name'].unique())
    selected_facility = st.sidebar.selectbox(
        'Select Facility',
        facilities
    )
    
    # Appointment type selection
    appointment_types = sorted(df['appointment_type'].unique())
    selected_type = st.sidebar.selectbox(
        'Select Appointment Type',
        appointment_types
    )
    
    # Filter data based on selections
    filtered_df = df[
        (df['facility_name'] == selected_facility) & 
        (df['appointment_type'] == selected_type)
    ]
    
    # Create time series plot
    fig = px.line(
        filtered_df,
        x='collection_date',
        y=['new_patient_wait_days', 'established_patient_wait_days'],
        title=f'Wait Times: {selected_facility} - {selected_type}',
        labels={
            'new_patient_wait_days': 'New Patient Wait (Days)',
            'established_patient_wait_days': 'Established Patient Wait (Days)',
            'collection_date': 'Date'
        }
    )
    
    # Display the plot
    st.plotly_chart(fig, use_container_width=True)
    
    # Display summary statistics
    st.subheader('Summary Statistics (Days)')
    
    stats_df = filtered_df[
        ['new_patient_wait_days', 'established_patient_wait_days']
    ].describe()
    
    st.dataframe(stats_df)
    
    # Display raw data
    if st.checkbox('Show Raw Data'):
        st.subheader('Raw Data')
        st.dataframe(filtered_df)

if __name__ == '__main__':
    main()
