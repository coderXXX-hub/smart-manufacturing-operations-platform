import plotly.express as px
import plotly.graph_objects as go


def production_by_plant(df):

    fig = px.bar(
        df,
        x="plant",
        y="production_count",
        color="failure_rate",
        text="production_count",
        title="Production by Plant"
    )

    fig.update_traces(
        textposition="outside"
    )

    fig.update_layout(
        template="plotly_dark",
        height=450,
        title_x=0.5,
        xaxis_title="Plant",
        yaxis_title="Production",
        coloraxis_colorbar_title="Failure %"
    )

    return fig


def failure_summary(df):

    fig = px.pie(
        df,
        names="failure_type",
        values="failure_count",
        hole=0.55,
        title="Failure Distribution"
    )

    fig.update_traces(
        textinfo="percent+label"
    )

    fig.update_layout(
        template="plotly_dark",
        title_x=0.5,
        height=450
    )

    return fig


def oee_gauge(df):

    availability = float(df["availability"].mean())

    fig = go.Figure(

        go.Indicator(

            mode="gauge+number",

            value=availability,

            title={"text": "Availability %"},

            gauge={

                "axis": {"range": [0, 100]},

                "bar": {"color": "#00CC96"},

                "steps": [

                    {"range": [0, 60], "color": "#EF553B"},

                    {"range": [60, 85], "color": "#FECB52"},

                    {"range": [85, 100], "color": "#00CC96"}

                ]

            }

        )

    )

    fig.update_layout(
        template="plotly_dark",
        height=350
    )

    return fig


def machine_health(df):

    fig = px.scatter(

        df,

        x="avg_tool_wear",

        y="avg_temperature_delta",

        color="maintenance_status",

        hover_name="machine_id",

        title="Machine Health"

    )

    fig.update_layout(

        template="plotly_dark",

        height=500,

        title_x=0.5,

        xaxis_title="Tool Wear",

        yaxis_title="Temperature Delta"

    )

    return fig


def plant_failure_rate(df):

    fig = px.bar(

        df,

        x="plant",

        y="failure_rate",

        color="failure_rate",

        text="failure_rate",

        title="Failure Rate by Plant"

    )

    fig.update_traces(
        texttemplate="%{text:.2f}%",
        textposition="outside"
    )

    fig.update_layout(

        template="plotly_dark",

        title_x=0.5,

        height=450,

        xaxis_title="Plant",

        yaxis_title="Failure Rate (%)"

    )

    return fig


def tool_wear(df):

    fig = px.histogram(

        df,

        x="avg_tool_wear",

        nbins=25,

        title="Tool Wear Distribution"

    )

    fig.update_layout(

        template="plotly_dark",

        title_x=0.5,

        height=450,

        xaxis_title="Tool Wear",

        yaxis_title="Machines"

    )

    return fig
