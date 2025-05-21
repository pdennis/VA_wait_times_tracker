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
        # df = self.load_data()

        # Sidebar filters
        st.sidebar.header("Filters")

        # state selection
        states = sorted(self.load_facilities()["state"].unique())
        selected_state = st.sidebar.selectbox("Select State", states)

        # Facility selection
        # facilities = sorted(df["facility_state"].unique())
        facilities = self.load_facilities(selected_state)
        selected_facility = st.sidebar.selectbox("Select Facility", facilities)

        # Appointment type selection
        # appointment_types = sorted(df["appointment_type"].unique())
        appointment_types = self.load_appointment_type()["appointment_type"]
        selected_type = st.sidebar.selectbox("Select Appointment Type", appointment_types)

        df = self.load_data(selected_state, selected_facility, selected_type)
        # Filter data based on selections
        filtered_df = df[(df["facility"] == selected_facility) & (df["appointment_type"] == selected_type)]

        # Create time series plot
        st.subheader("Patient Wait Times (Days)")
        fig = px.line(
            filtered_df,
            x="report_date",
            y=["new_patient_wait_days", "established_patient_wait_days"],
            # title=title,
            labels={
                "new_patient_wait_days": "New Patient Wait (Days)",
                "established_patient_wait_days": "Established Patient Wait (Days)",
                "report_date": "Date",
            },
        )
        fig.update_layout(
            title={
                "text": f"{selected_facility} - {selected_state}<br>{selected_type}",
                "y": 0.9,
                "x": 0.5,
                "xanchor": "center",
                "yanchor": "top",
            }
        )
        fig.update_layout(yaxis_title="Days")
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

    def load_data(self, sel_state: str = None, sel_facility=None, sel_app_type=None):
        with psycopg.connect(self.database_url) as conn:
            if sel_facility and sel_app_type:
                if " - " in sel_facility and sel_state is None:
                    facility, state = sel_facility.split(" - ")
                else:
                    facility = sel_facility
                    state = sel_state
                params = (
                    state,
                    facility,
                    sel_app_type,
                )
                df = pd.read_sql(
                    """
                    SELECT
                        f.facility,
                        f.state,
                        w.report_date,
                        w.appointment_type,
                        w.established as established_patient_wait_days,
                        w.new as new_patient_wait_days
                    FROM wait_time_report w, facility f
                    WHERE w.station_id = f.station_id
                        and f.state = %s
                        and f.facility = %s
                        and w.appointment_type = %s
                    order by w.report_date
                    """,
                    conn,
                    params=params,
                )
            else:
                df = pd.read_sql(
                    """
                        SELECT
                            f.facility,
                            f.state,
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

    def load_facilities(self, sel_state: str = None):
        with psycopg.connect(self.database_url) as conn:
            if sel_state:
                params = (sel_state,)
                df = pd.read_sql(
                    """
                    SELECT DISTINCT
                        f.facility,
                        f.state
                    FROM wait_time_report w, facility f
                    WHERE w.station_id = f.station_id
                        and state = %s
                    order by f.facility, f.state
                    """,
                    conn,
                    params=params,
                )
            else:
                df = pd.read_sql(
                    """
                        SELECT DISTINCT
                            f.facility,
                            f.state
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
