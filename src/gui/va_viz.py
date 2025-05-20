import pandas as pd
import plotly.express as px
import psycopg
import streamlit as st

from src.config.settings import DATABASE_URL


class VaViz:
    def __init__(self):
        self.database_url = DATABASE_URL
        if not self.database_url:
            raise ValueError("Database URL is required")

        st.title = "VA Facility Wait Times Analysis"
        df = self.load_data()

        # Sidebar filters
        st.sidebar.header("Filters")

        # Facility selection
        # facilities = sorted(df["facility_state"].unique())
        facilities = self.load_facilities()["facility_state"]
        selected_facility = st.sidebar.selectbox("Select Facility", facilities)

        # Appointment type selection
        # appointment_types = sorted(df["appointment_type"].unique())
        appointment_types = self.load_appointment_type()["appointment_type"]
        selected_type = st.sidebar.selectbox("Select Appointment Type", appointment_types)

        # Filter data based on selections
        filtered_df = df[(df["facility_state"] == selected_facility) & (df["appointment_type"] == selected_type)]

        # Create time series plot
        fig = px.line(
            filtered_df,
            x="report_date",
            y=["new_patient_wait_days", "established_patient_wait_days"],
            title=f"Wait Times: {selected_facility} - {selected_type}",
            labels={
                "new_patient_wait_days": "New Patient Wait (Days)",
                "established_patient_wait_days": "Established Patient Wait (Days)",
                "report_date": "Date",
            },
        )

        # Display the plot
        st.plotly_chart(fig, use_container_width=True)

        # Display summary statistics
        st.subheader("Summary Statistics (Days)")

        stats_df = filtered_df[["new_patient_wait_days", "established_patient_wait_days"]].describe()

        st.dataframe(stats_df)

        # Display raw data
        if st.checkbox("Show Raw Data"):
            st.subheader("Raw Data")
            st.dataframe(filtered_df)

    def load_data(self):
        with psycopg.connect(self.database_url) as conn:
            df = pd.read_sql(
                """
                    SELECT
                        f.facility,
                        f.state,
                        f.facility || ' - '::text || f.state as facility_state,
                        w.report_date,
                        w.appointment_type,
                        w.established as established_patient_wait_days,
                        w.new as new_patient_wait_days
                    FROM wait_time_report w, facility f
                    WHERE w.station_id = f.station_id
                    order by w.report_date
                """,
                conn,
            )
        return df

    def load_facilities(self):
        with psycopg.connect(self.database_url) as conn:
            df = pd.read_sql(
                """
                    SELECT DISTINCT
                        f.facility,
                        f.state,
                        f.facility || ' - '::text || f.state as facility_state
                    FROM wait_time_report w, facility f
                    WHERE w.station_id = f.station_id
                    order by f.facility, f.state
                """,
                conn,
            )
        return df

    def load_appointment_type(self):
        with psycopg.connect(self.database_url) as conn:
            df = pd.read_sql(
                """
                    SELECT DISTINCT
                        w.appointment_type
                    FROM wait_time_report w, facility f
                    WHERE w.station_id = f.station_id
                    order by w.appointment_type
                """,
                conn,
            )
        return df


if __name__ == "__main__":
    VaViz()
