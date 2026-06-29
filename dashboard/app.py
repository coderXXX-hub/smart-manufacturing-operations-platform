import streamlit as st

from data_loader import (
    load_executive,
    load_plant_performance,
    load_machine_health,
    load_failure_summary,
    load_oee
)

from charts import (
    production_by_plant,
    failure_summary,
    oee_gauge,
    machine_health,
    plant_failure_rate,
    tool_wear
)

st.set_page_config(
    page_title="Smart Manufacturing Operations Center",
    page_icon="🏭",
    layout="wide"
)

# ----------------------------------------------------
# Title
# ----------------------------------------------------

st.title("🏭 Smart Manufacturing Operations Center")

st.markdown("---")

# ----------------------------------------------------
# Load Data
# ----------------------------------------------------

with st.spinner("Loading Manufacturing Data..."):

    executive = load_executive()

    plant = load_plant_performance()

    machine = load_machine_health()

    failures = load_failure_summary()

    oee = load_oee()

# ----------------------------------------------------
# Executive KPIs
# ----------------------------------------------------

c1, c2, c3, c4 = st.columns(4)

with c1:

    st.metric(
        "🏭 Production",
        f"{int(executive['total_production'][0]):,}"
    )

with c2:

    st.metric(
        "❌ Failures",
        f"{int(executive['total_failures'][0]):,}"
    )

with c3:

    st.metric(
        "⚠️ Failure Rate",
        f"{executive['failure_rate'][0]} %"
    )

with c4:

    st.metric(
        "🤖 Machines",
        f"{int(executive['total_machines'][0]):,}"
    )

st.divider()

# ----------------------------------------------------
# Charts
# ----------------------------------------------------

left, right = st.columns(2)

with left:

    st.plotly_chart(
        production_by_plant(plant),
        use_container_width=True
    )

with right:

    st.plotly_chart(
        failure_summary(failures),
        use_container_width=True
    )

st.divider()

# ----------------------------------------------------
# Plant Performance
# ----------------------------------------------------

st.subheader("🏭 Plant Performance")

st.dataframe(
    plant,
    use_container_width=True
)

st.divider()

# ----------------------------------------------------
# Machine Health
# ----------------------------------------------------

st.subheader("🔧 Machine Health")

st.dataframe(
    machine,
    use_container_width=True,
    height=400
)

st.divider()

# ----------------------------------------------------
# OEE
# ----------------------------------------------------

st.subheader("📈 Overall Equipment Effectiveness")

st.dataframe(
    oee,
    use_container_width=True
)

st.divider()

# ----------------------------------------------------
# Failure Summary
# ----------------------------------------------------

st.subheader("🚨 Failure Summary")

st.dataframe(
    failures,
    use_container_width=True
)

st.success("✅ Streaming Dashboard Running Successfully")
