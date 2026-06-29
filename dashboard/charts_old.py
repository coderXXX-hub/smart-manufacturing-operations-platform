import plotly.express as px


def production_by_plant(df):
    fig = px.bar(
        df,
        x="plant",
        y="production_count",
        color="failure_rate",
        text="production_count",
        title="Production by Plant"
    )

    fig.update_layout(
        height=450,
        template="plotly_dark"
    )

    return fig


def failure_summary(df):
    fig = px.pie(
        df,
        names="failure_type",
        values="failure_count",
        title="Failure Distribution"
    )

    fig.update_layout(
        height=450,
        template="plotly_dark"
    )

    return fig
