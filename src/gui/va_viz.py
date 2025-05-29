import pandas as pd
import plotly.express as px
import psycopg
import streamlit as st

from src.config.settings import DATABASE_URL

DATA_SETS = {
    "All Data": "wait_time_report",
    "7-Day Averages": "wait_time_report_7",
    "28-Day Averages": "wait_time_report_28",
}

state_index: int = 0


class VaViz:
    def __init__(self):
        global state_index
        self.database_url = DATABASE_URL
        if not self.database_url:
            raise ValueError("Database URL is required")

        st.title = "VA Facility Wait Times Analysis"
        # df = self.load_data()

        # Sidebar filters
        st.sidebar.header("Filters")

        data_sets = ["All Data", "7-Day Averages", "28-Day Averages"]
        selected_data = st.sidebar.selectbox("Select Data Set", data_sets)

        # state selection
        states = sorted(self.load_facilities()["state"].unique())
        selected_state = st.sidebar.selectbox("Select State", states, index=state_index)
        state_index = states.index(selected_state)

        # Facility selection
        # facilities = sorted(df["facility_state"].unique())
        facilities = self.load_facilities(selected_state)
        selected_facility = st.sidebar.selectbox("Select Facility", facilities)

        # Appointment type selection
        # appointment_types = sorted(df["appointment_type"].unique())
        appointment_types = self.load_appointment_type(
            sel_state=selected_state,
            sel_facility=selected_facility,
        )["appointment_type"]
        selected_type = st.sidebar.selectbox("Select Appointment Type", appointment_types)

        filtered_df = self.load_data(selected_data, selected_state, selected_facility, selected_type)

        # Create a time series plot
        st.subheader("Patient Wait Times (Days)")
        if filtered_df["established_patient_wait_days"].isna().all():
            filtered_df["established_patient_wait_days"] = 0
        if filtered_df["new_patient_wait_days"].isna().all():
            filtered_df["new_patient_wait_days"] = 0

        # station_id = facilities[(facilities['facility'] == selected_facility)].station_id[0]
        fig = px.line(
            filtered_df,
            x="report_date",
            y=["new_patient_wait_days", "established_patient_wait_days"],
            # title=title,
            labels={
                "new_patient_wait_days": "New Patients",
                "established_patient_wait_days": "Established Patients",
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
        fig.update_layout(yaxis_title="Wait Days")

        # Display the plot
        st.plotly_chart(fig, use_container_width=True)

        # Display summary statistics
        st.subheader("Summary Statistics (Wait Days)")

        stats_df = filtered_df[["established_patient_wait_days", "new_patient_wait_days"]].describe()

        st.dataframe(stats_df)

        # Display raw data
        if st.checkbox("Show Raw Data"):
            st.subheader("Raw Data")
            st.dataframe(filtered_df)

    def load_data(
        self,
        sel_data: str = None,
        sel_state: str = None,
        sel_facility=None,
        sel_app_type=None,
    ):
        data_set = DATA_SETS.get(sel_data, "wait_time_report")
        with psycopg.connect(self.database_url) as conn:
            if sel_facility and sel_app_type:
                if " - " in sel_facility and sel_state is None:
                    facility, state = sel_facility.split(" - ")
                else:
                    facility = sel_facility
                    state = sel_state
                if facility == "ALL":
                    if sel_app_type == "ALL":
                        # return all data for a specific state
                        params = (state,)
                        df = pd.read_sql(
                            f"""
                            SELECT
                                w.report_date,
                                avg(w.established) as established_patient_wait_days,
                                avg(w.new) as new_patient_wait_days
                            FROM {data_set} w, facility f
                            WHERE w.station_id = f.station_id
                                and f.state = %s
                            group by w.report_date
                            order by w.report_date
                            """,
                            conn,
                            params=params,
                        )
                    else:
                        # return all data for a specific state and appointment type
                        params = (
                            state,
                            sel_app_type,
                        )
                        df = pd.read_sql(
                            f"""
                            SELECT
                                w.report_date,
                                avg(w.established) as established_patient_wait_days,
                                avg(w.new) as new_patient_wait_days
                            FROM {data_set} w, facility f
                            WHERE w.station_id = f.station_id
                              and f.state = %s
                              and w.appointment_type = %s
                            group by w.report_date
                            order by w.report_date
                            """,
                            conn,
                            params=params,
                        )
                elif sel_app_type == "ALL":
                    # return all data for a specific facility and state
                    params = (
                        state,
                        facility,
                    )
                    df = pd.read_sql(
                        f"""
                        SELECT
                            w.report_date,
                            avg(w.established) as established_patient_wait_days,
                            avg(w.new) as new_patient_wait_days
                        FROM {data_set} w, facility f
                        WHERE w.station_id = f.station_id
                            and f.state = %s
                            and f.facility = %s
                        group by w.report_date
                        order by w.report_date
                        """,
                        conn,
                        params=params,
                    )
                else:
                    params = (
                        state,
                        facility,
                        sel_app_type,
                    )
                    df = pd.read_sql(
                        f"""
                        SELECT
                            w.report_date,
                            w.appointment_type,
                            w.established as established_patient_wait_days,
                            w.new as new_patient_wait_days
                        FROM {data_set} w, facility f
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
                # return all data
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
                    Select 'ALL' as facility
                    UNION
                    select facility from (SELECT DISTINCT f.facility,
                                                          f.state,
                                                          f.station_id
                                          FROM wait_time_report w,
                                               facility f
                                          WHERE w.station_id = f.station_id
                                            and state = %s
                                          order by f.facility, f.state) s
                    """,
                    conn,
                    params=params,
                )
            else:
                df = pd.read_sql(
                    """
                        SELECT DISTINCT
                            f.facility,
                            f.state,
                            f.station_id
                        FROM wait_time_report w, facility f
                        WHERE w.station_id = f.station_id
                        order by f.facility, f.state
                    """,
                    conn,
                )
        return df

    def load_appointment_type(self, sel_state: str = None, sel_facility: str = None):
        with psycopg.connect(self.database_url) as conn:
            if sel_facility == 'ALL':
                params = (sel_state,)
                df = pd.read_sql(
                    """
                            Select 'ALL' as appointment_type
                            UNION
                            select appointment_type from (
                                SELECT DISTINCT
                                w.appointment_type
                                FROM wait_time_report w, facility f
                                WHERE w.station_id = f.station_id
                                    and state = %s
                                order by w.appointment_type) a
                    """,
                    conn,
                    params=params,
                )
            else:
                params = (sel_state, sel_facility)
                df = pd.read_sql(
                    """
                            Select 'ALL' as appointment_type
                            UNION
                            select appointment_type from (
                                SELECT DISTINCT
                                w.appointment_type
                                FROM wait_time_report w, facility f
                                WHERE w.station_id = f.station_id
                                    and f.state = %s
                                    and f.facility = %s 
                                order by w.appointment_type) a
                    """,
                    conn,
                    params=params,
                )
        return df


if __name__ == "__main__":
    VaViz()
