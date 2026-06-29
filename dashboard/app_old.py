import streamlit as st
from data_loader import (
    load_executive,
    load_plant_performance,
    load_machine_health,
    load_failure_summary,
    load_oee
)

st.set_page_config(
    page_title="Smart Manufacturing Operations Center",
    layout="wide"
)

st.title("🏭 Smart Manufacturing Operations Center")

executive = load_executive()
plant = load_plant_performance()
machine = load_machine_health()
failure = load_failure_summary()
oee = load_oee()

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Production",
    int(executive["total_production"][0])
)

col2.metric(
    "Failures",
    int(executive["total_failures"][0])
)

col3.metric(
    "Failure %",
    executive["failure_rate"][0]
)

col4.metric(
    "Machines",
    int(executive["total_machines"][0])
)

st.divider()

st.subheader("Plant Performance")

st.bar_chart(
    plant.set_index("plant")["production_count"]
)

st.subheader("Failure Summary")

st.bar_chart(
    failure.set_index("failure_type")["failure_count"]
)

st.subheader("Machine Health")

st.dataframe(machine)

st.subheader("OEE")

st.dataframe(oee)
