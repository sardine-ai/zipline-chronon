import streamlit as st
import requests
from datetime import datetime
import pandas as pd
from collections import defaultdict

# Configure the page to use wide mode
st.set_page_config(layout="wide")

# Add custom CSS to make charts wider
st.markdown("""
<style>
    .element-container {
        width: 100%;
    }
    .stChart {
        width: 100%;
        min-width: 400px;
    }
</style>
""", unsafe_allow_html=True)

def format_timestamp(ts_ms):
    """Format millisecond timestamp to readable date."""
    return datetime.fromtimestamp(ts_ms/1000).strftime('%Y-%m-%d %H:%M')

def load_data():
    """Load data from the API."""
    try:
        response = requests.get("http://localhost:8181/api/drift-series")
        return response.json()
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None

def create_series_df(timestamps, values):
    """Create a DataFrame for a series."""
    return pd.DataFrame({
        'timestamp': [format_timestamp(ts) for ts in timestamps],
        'value': values
    })

def is_valid_series(series):
    return any(value is not None for value in series)

def main():
    st.title("Drift Board")

    # Load data
    data = load_data()
    if not data:
        return

    # Group data by groupName
    grouped_data = defaultdict(list)
    for entry in data:
        group_name = entry["key"].get("groupName", "unknown")
        grouped_data[group_name].append(entry)

    # Series types and their display names
    series_types = {
        "percentileDriftSeries": "Percentile Drift",
        "histogramDriftSeries": "Histogram Drift",
        "countChangePercentSeries": "Count Change %"
    }

    # Create tabs for each group
    group_tabs = st.tabs(list(grouped_data.keys()))

    # Fill each tab with its group's data
    for tab, (group_name, group_entries) in zip(group_tabs, grouped_data.items()):
        with tab:
            for entry in group_entries:
                # Create expander for each column
                column_name = entry["key"].get("column", "unknown")
                with st.expander(f"Column: {column_name}", expanded=True):
                    # Get available series for this entry
                    available_series = [s_type for s_type in series_types.keys()
                                        if s_type in entry and is_valid_series(entry[s_type])]

                    if available_series:
                        # Create columns for charts with extra padding
                        cols = st.columns([1] * len(available_series))

                        # Create charts side by side
                        for col_idx, series_type in enumerate(available_series):
                            if is_valid_series(entry[series_type]):
                                with cols[col_idx]:
                                    st.subheader(series_types[series_type])
                                    df = create_series_df(entry["timestamps"], entry[series_type])
                                    st.line_chart(df.set_index('timestamp'), height=400)

if __name__ == "__main__":
    main()