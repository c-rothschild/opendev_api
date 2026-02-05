"""
Developer Dashboard — explore ecosystems and developers.

Run from project root:
  uv run streamlit run app/dashboard.py
  (after: uv sync --extra dashboard)
"""

import os
import sys
from datetime import date, timedelta

import streamlit as st
import plotly.graph_objects as go

# Ensure package is importable when run as streamlit app
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from opendev_api import OpenDevData


# --- Config ---
DATA_FOLDER = os.environ.get("OPENDEV_DATA_FOLDER", "./data")
DB_FILENAME = os.environ.get("OPENDEV_DB_FILENAME", "odd.duckdb")


@st.cache_resource
def get_client():
    path = os.path.join(DATA_FOLDER, DB_FILENAME)
    if not os.path.isfile(path):
        return None
    return OpenDevData(DATA_FOLDER, DB_FILENAME)


def main():
    st.set_page_config(page_title="OpenDev Developer Dashboard", layout="wide")
    st.title("OpenDev Developer Dashboard")
    st.caption("Explore ecosystems and developers")

    client = get_client()
    if client is None:
        st.error(
            f"Database not found at `{os.path.join(DATA_FOLDER, DB_FILENAME)}`. "
            "Set OPENDEV_DATA_FOLDER and OPENDEV_DB_FILENAME if needed."
        )
        return

    # Sidebar: ecosystem selection
    with st.sidebar:
        st.header("Ecosystem")
        search = st.text_input("Search ecosystems", placeholder="e.g. Bitcoin, Ethereum")
        if search:
            results = client.search_ecosystems(search.strip(), limit=20)
        else:
            results = client.list_ecosystems(limit=50, include_repo_count=True)

        if not results:
            st.info("No ecosystems found.")
            ecosystem_id = None
        else:
            options = {f"{r['name']} (id: {r['id']})": r["id"] for r in results}
            selected_label = st.selectbox(
                "Select ecosystem",
                options=list(options.keys()),
                index=0,
            )
            ecosystem_id = options[selected_label]

    if ecosystem_id is None:
        st.info("Select an ecosystem from the sidebar to view overview, repos, and developers.")
        return

    # Main: tabs
    tab_overview, tab_repos, tab_developers = st.tabs(["Overview", "Repos", "Developers"])

    with tab_overview:
        render_overview(client, ecosystem_id)

    with tab_repos:
        render_repos(client, ecosystem_id)

    with tab_developers:
        render_developers(client, ecosystem_id)


def render_overview(client: OpenDevData, ecosystem_id: int):
    eco = client.get_ecosystem(ecosystem_id, include_latest_mads=True)
    if not eco:
        st.warning("Ecosystem not found.")
        return

    st.subheader(eco.get("name", "Ecosystem"))
    col1, col2, col3 = st.columns(3)
    with col1:
        if eco.get("launch_date"):
            st.metric("Launch date", str(eco["launch_date"]))
    with col2:
        if eco.get("derived_launch_date"):
            st.metric("Derived launch", str(eco["derived_launch_date"]))
    with col3:
        flags = []
        if eco.get("is_crypto"): flags.append("Crypto")
        if eco.get("is_chain"): flags.append("Chain")
        if eco.get("is_category"): flags.append("Category")
        st.metric("Flags", ", ".join(flags) or "—")

    if eco.get("latest_mads"):
        m = eco["latest_mads"]
        st.subheader("Latest activity (MADs)")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("All devs", m.get("all_devs"))
        c2.metric("Exclusive devs", m.get("exclusive_devs"))
        c3.metric("Commits", m.get("num_commits"))
        c4.metric("Full-time devs", m.get("full_time_devs"))

    # Hierarchy
    hier = client.ecosystem_hierarchy(ecosystem_id)
    if hier.get("parents") or hier.get("children"):
        st.subheader("Hierarchy")
        pcol, ccol = st.columns(2)
        with pcol:
            st.write("**Parents**")
            if hier["parents"]:
                for p in hier["parents"]:
                    st.write(f"- {p.get('parent_name')} (id: {p.get('parent_id')})")
            else:
                st.write("—")
        with ccol:
            st.write("**Children**")
            if hier["children"]:
                for c in hier["children"]:
                    st.write(f"- {c.get('child_name')} (id: {c.get('child_id')})")
            else:
                st.write("—")

    # MADs time series chart
    st.subheader("Activity over time (last 90 days)")
    end = date.today()
    start = end - timedelta(days=90)
    mads = client.ecosystem_mads_time_series(
        ecosystem_id, start_date=start, end_date=end, limit=90
    )
    if mads:
        mads_sorted = sorted(mads, key=lambda x: x["day"])
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=[r["day"] for r in mads_sorted],
                y=[r["all_devs"] for r in mads_sorted],
                name="All devs",
                mode="lines+markers",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=[r["day"] for r in mads_sorted],
                y=[r["num_commits"] for r in mads_sorted],
                name="Commits",
                mode="lines+markers",
                yaxis="y2",
            )
        )
        fig.update_layout(
            xaxis_title="Day",
            yaxis_title="All devs",
            yaxis2=dict(title="Commits", overlaying="y", side="right"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            hovermode="x unified",
            height=400,
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No time series data for this range.")


def render_repos(client: OpenDevData, ecosystem_id: int):
    st.subheader("Repos in ecosystem")
    recursive = st.checkbox("Include recursive (child ecosystem) repos", value=True)
    repos = client.repos_in_ecosystem(
        ecosystem_id, recursive=recursive, sort_by="num_stars", limit=100
    )
    if not repos:
        st.info("No repos found.")
        return
    # Show as table with key columns
    rows = [
        {
            "Name": r.get("name"),
            "Link": r.get("link"),
            "Stars": r.get("num_stars"),
            "Forks": r.get("num_forks"),
        }
        for r in repos
    ]
    st.dataframe(rows, use_container_width=True, hide_index=True)


def render_developers(client: OpenDevData, ecosystem_id: int):
    st.subheader("Developers in ecosystem")
    rank_filter = st.selectbox(
        "Contribution rank",
        ["All", "full_time", "part_time", "one_time"],
        index=0,
    )
    rank = None if rank_filter == "All" else rank_filter
    devs = client.developers_in_ecosystem(
        ecosystem_id,
        contribution_rank=rank,
        include_user_info=True,
        limit=200,
    )
    if not devs:
        st.info("No developers found.")
        return

    rows = [
        {
            "ID": r.get("canonical_developer_id"),
            "Login": r.get("login"),
            "Name": r.get("name"),
            "Rank": r.get("contribution_rank"),
            "Points": r.get("points"),
        }
        for r in devs
    ]
    st.dataframe(rows, use_container_width=True, hide_index=True)

    # Developer detail: select one
    st.subheader("Developer profile")
    dev_options = [f"{r.get('login') or r.get('canonical_developer_id')} (id: {r.get('canonical_developer_id')})" for r in devs]
    dev_ids = [r["canonical_developer_id"] for r in devs]
    selected_idx = st.selectbox("Select developer to view profile", range(len(dev_options)), format_func=lambda i: dev_options[i])
    if selected_idx is not None:
        dev_id = dev_ids[selected_idx]
        profile = client.get_developer_profile(dev_id, include_location=True)
        if profile:
            st.json({k: v for k, v in profile.items() if k != "locations" and v is not None})
            if profile.get("locations"):
                st.write("**Locations**")
                st.json(profile["locations"])
            # Activity in this ecosystem
            activity = client.developer_activity_in_ecosystem(
                ecosystem_id, dev_id, limit=30
            )
            if activity:
                st.write("**Recent activity (commits per day)**")
                act_sorted = sorted(activity, key=lambda x: x["day"], reverse=True)
                st.dataframe(act_sorted[:15], use_container_width=True, hide_index=True)
            tenure = client.developer_tenure_in_ecosystem(ecosystem_id, dev_id)
            if tenure:
                st.write("**Tenure**")
                st.dataframe(tenure[:10], use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
