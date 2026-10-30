import os

import pandas as pd
import psycopg
import streamlit as st

st.set_page_config(page_title="SmartCare", layout="wide")
st.title("SmartCare - live risk dashboard")


def q(sql, params=None):
    with psycopg.connect(host=os.environ["PG_HOST"], dbname="smartcare",
                         user="smartcare_ro", password=os.environ["PG_PASSWORD"]) as con:
        cur = con.execute(sql, params)
        cols = [c.name for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def status(r):
    if pd.isna(r):
        return "⚪ no score"
    if r >= 0.95:
        return "🔴 HIGH"
    if r >= 0.8:
        return "🟠 MEDIUM"
    return "🟢 ok"


@st.fragment(run_every=10)            #
def live():
    if q("SELECT count(*) AS n FROM scores")["n"][0] == 0:
        st.info("Low data available.")
        return

    last = q("SELECT max(window_end) AS t FROM scores")["t"][0]

    df = q("""SELECT DISTINCT ON (patient_id)
                     patient_id, window_end, risk_score,
                     hr_mean_5min, spo2_min_5min, temp_mean_5min, steps_5min
              FROM scores
              ORDER BY patient_id, window_end DESC""")
    df["status"] = df["risk_score"].apply(status)
    df = df.sort_values("risk_score", ascending=False, na_position="last")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Patients", len(df))
    c2.metric("HIGH", int((df["status"] == "🔴 HIGH").sum()))
    c3.metric("MEDIUM", int((df["status"] == "🟠 MEDIUM").sum()))
    c4.metric("Average risk", round(float(df["risk_score"].mean()), 3))

    st.subheader("All patients (latest window)")
    show = df[["status", "patient_id", "risk_score", "hr_mean_5min",
               "spo2_min_5min", "temp_mean_5min", "steps_5min", "window_end"]]
    st.dataframe(
        show, hide_index=True, height=520,
        column_config={
            "status": "Status",
            "patient_id": "Patient",
            "risk_score": st.column_config.ProgressColumn(
                "Risk", min_value=0.0, max_value=1.0, format="%.3f"),
            "hr_mean_5min": st.column_config.NumberColumn("HR", format="%.0f"),
            "spo2_min_5min": st.column_config.NumberColumn("SpO2", format="%.0f"),
            "temp_mean_5min": st.column_config.NumberColumn("Temp", format="%.1f"),
            "steps_5min": "Steps",
            "window_end": "Window end",
        })

    st.subheader("Risk over time (all patients)")
    t = q("""SELECT window_end, avg(risk_score) AS avg_risk, max(risk_score) AS max_risk
             FROM scores WHERE window_end > %s - interval '3 hours'
             GROUP BY 1 ORDER BY 1""", (last,))
    st.line_chart(t.set_index("window_end"))

    st.subheader("Latest alerts")
    al = q("""SELECT patient_id, window_end, round(risk_score::numeric, 3) AS risk, level,
                     round(hr_mean_5min::numeric, 1) AS hr, round(spo2_min_5min::numeric, 1) AS spo2
              FROM alerts ORDER BY window_end DESC LIMIT 20""")
    st.write("No alerts.") if al.empty else st.dataframe(al, hide_index=True)


live()