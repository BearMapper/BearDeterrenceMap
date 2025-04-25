# Filename: carpathian_bears.py
# (GeoPandas reprojection + Interactivity + st.table() fix + Heatmap location fix)

import streamlit as st
import pandas as pd
import folium
from folium.plugins import MarkerCluster, HeatMap, TimestampedGeoJson
from streamlit_folium import st_folium
import os
import numpy as np
from datetime import datetime, timedelta
import json
import plotly.express as px
import plotly.graph_objects as go
import math
import io

# --- ADDED: Import geopandas ---
import geopandas as gpd
from shapely.geometry import Point
# --- END ADDITION ---

# Import the database handler class
from carpathian_bears_db import CarpathianBearsDB

# Database file path
DB_PATH = "wildlife_data.db"

# --- Folium Map Display ---
def display_folium_map_efficiently(map_figure, key="folium_map"):
    map_data = st_folium(
        map_figure, key=key, width=1000, height=600,
        returned_objects=["last_object_clicked", "last_active_drawing"]
    )
    return map_data

# --- Data Calculation Functions ---
def calculate_distance_speed(df):
    # ... (Keep existing implementation) ...
    if df.empty or 'x' not in df.columns or 'y' not in df.columns or 'timestamp' not in df.columns: return df
    df['x'] = pd.to_numeric(df['x'], errors='coerce')
    df['y'] = pd.to_numeric(df['y'], errors='coerce')
    df = df.dropna(subset=['x', 'y']).copy()
    df = df.sort_values(by=['bear_id', 'timestamp'])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['x_prev'] = df.groupby('bear_id')['x'].shift(1)
    df['y_prev'] = df.groupby('bear_id')['y'].shift(1)
    df['timestamp_prev'] = df.groupby('bear_id')['timestamp'].shift(1)
    df['distance_m'] = np.sqrt((df['x'] - df['x_prev'])**2 + (df['y'] - df['y_prev'])**2)
    df['time_diff_h'] = (df['timestamp'] - df['timestamp_prev']).dt.total_seconds() / 3600.0
    epsilon = 1e-6
    df['speed_m_h'] = df['distance_m'] / (df['time_diff_h'] + epsilon)
    df.loc[df['time_diff_h'] <= epsilon, 'speed_m_h'] = 0
    df.loc[df['distance_m'].isna(), 'speed_m_h'] = 0
    df.loc[df['distance_m'].isna(), 'distance_m'] = 0
    df = df.drop(columns=['x_prev', 'y_prev', 'timestamp_prev', 'time_diff_h'])
    return df

def calculate_daily_stats(df):
    # ... (Keep existing implementation) ...
    if df.empty or 'distance_m' not in df.columns: return pd.DataFrame(columns=['bear_id', 'date', 'daily_distance_m'])
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['date'] = df['timestamp'].dt.date
    daily_dist = df.groupby(['bear_id', 'date'])['distance_m'].sum().reset_index()
    daily_dist = daily_dist.rename(columns={'distance_m': 'daily_distance_m'})
    return daily_dist

# --- GeoJSON Feature Generation ---
def get_geojson_features(df, color_by_col='dynamic_color'):
    # ... (Keep existing implementation) ...
    features = []
    if df.empty or 'latitude' not in df.columns or 'longitude' not in df.columns: return features
    df = df.copy()
    df['time_str'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    for idx, row in df.iterrows():
        lat = row['latitude']; lng = row['longitude']
        point_color = row.get(color_by_col, '#808080')
        if not (pd.isna(lat) or pd.isna(lng)):
            features.append({
                'type': 'Feature','geometry': {'type': 'Point', 'coordinates': [lng, lat]},
                'properties': { 'time': row['time_str'],
                    'popup': (f"<b>Bear:</b> {row['bear_id']}<br><b>Time:</b> {row['timestamp'].strftime('%Y-%m-%d %H:%M')}<br><b>Speed:</b> {row.get('speed_m_h', 'N/A'):.1f} m/h<br><b>Season:</b> {row.get('season', 'N/A')}<br><b>Sex:</b> {row.get('sex', 'N/A')}<br><b>Age:</b> {row.get('age', 'N/A')}"),
                    'tooltip': f"Bear: {row['bear_id']} | Sex: {row['sex']} | Age: {row['age']} | Season: {row['season']} | Time: {row['timestamp'].strftime('%H:%M')}",
                    'style': {'color': point_color, 'fillColor': point_color, 'fillOpacity': 0.7, 'weight': 1, 'radius': 8},
                    'icon': 'circle', 'iconstyle': {'fillColor': point_color, 'fillOpacity': 0.7, 'stroke': 'true', 'radius': 8},
                    'id': idx }})
    return features

# --- Legend Generation ---
def generate_legend_html(title, color_map):
    # ... (Keep existing implementation) ...
    legend_html = f"""<div style="position: fixed; bottom: 50px; right: 50px; width: 180px; height: auto; border:2px solid grey; z-index:9999; font-size:14px; background-color:white; padding: 10px; border-radius: 5px; opacity: 0.9; max-height: 300px; overflow-y: auto;">&nbsp; <b>{title}</b> <br>"""
    sorted_items = sorted(color_map.items(), key=lambda item: str(item[0]))
    for name, color in sorted_items:
        display_name = str(name) if pd.notna(name) else "Unknown"
        legend_html += f'&nbsp; <i class="fa fa-circle" style="color:{color}"></i>&nbsp; {display_name}<br>'
    legend_html += "</div>"; return legend_html

# --- Cached Data Loading and Processing ---
@st.cache_data(ttl=3600)
def load_and_process_data(db_path, selected_bears, start_datetime, end_datetime, query_season, selected_sex_filter, selected_age_filter):
    # ... (Keep existing implementation using GeoPandas reprojection and passing datetime objects to DB) ...
    _db = CarpathianBearsDB(db_path)
    all_data_frames = []
    for bear in selected_bears:
        bear_data = _db.get_bear_data(bear_id=bear, start_date=start_datetime, end_date=end_datetime, season=query_season) # Pass datetime directly
        if not bear_data.empty: all_data_frames.append(bear_data)
    if not all_data_frames: return pd.DataFrame(), pd.DataFrame()
    filtered_data = pd.concat(all_data_frames, ignore_index=True).copy()
    filtered_data['timestamp'] = pd.to_datetime(filtered_data['timestamp'])
    filtered_data['x'] = pd.to_numeric(filtered_data['x'], errors='coerce'); filtered_data['y'] = pd.to_numeric(filtered_data['y'], errors='coerce')
    filtered_data.dropna(subset=['x', 'y', 'timestamp'], inplace=True)
    if 'bear_id' in filtered_data.columns: filtered_data['bear_id'] = filtered_data['bear_id'].astype(str) # Ensure string type early
    if selected_sex_filter != 'All': filtered_data = filtered_data[filtered_data['sex'] == selected_sex_filter]
    if selected_age_filter != 'All': filtered_data = filtered_data[filtered_data['age'] == selected_age_filter]
    if filtered_data.empty: return pd.DataFrame(), pd.DataFrame()
    try:
        gdf = gpd.GeoDataFrame(filtered_data, geometry=gpd.points_from_xy(filtered_data.x, filtered_data.y), crs="EPSG:2180")
        gdf_reprojected = gdf.to_crs("EPSG:4326")
        filtered_data = filtered_data.loc[gdf_reprojected.index].copy()
        filtered_data['latitude'] = gdf_reprojected.geometry.y; filtered_data['longitude'] = gdf_reprojected.geometry.x
    except ImportError: st.error("GeoPandas not installed."); return filtered_data, pd.DataFrame()
    except Exception as e: st.error(f"Reprojection error: {e}"); return filtered_data, pd.DataFrame()
    processed_data = calculate_distance_speed(filtered_data); daily_stats = calculate_daily_stats(processed_data)
    return processed_data, daily_stats

# --- Main Display Function ---
def display_carpathian_bears_section():
    st.header("Carpathian Brown Bear Movement Analysis")
    # DB Init & Sidebar Filters
    # ... (Keep existing DB init and filter definitions) ...
    if not os.path.exists(DB_PATH): st.error(f"DB not found: {DB_PATH}"); st.stop()
    try:
        db_meta = CarpathianBearsDB(DB_PATH); bears_info_df = db_meta.get_bears_list()
        if bears_info_df.empty: st.warning("No bear data found."); st.stop()
        try: available_seasons = ['All'] + sorted([s for s in db_meta.get_distinct_values('bears_tracking', 'season') if pd.notna(s)])
        except Exception: available_seasons = ['All']
        try: min_date_db, max_date_db = db_meta.get_date_range('bears_tracking', 'timestamp')
        except Exception: min_date_db, max_date_db = None, None
        del db_meta
    except Exception as e: st.error(f"DB access error: {e}"); st.stop()
    st.sidebar.header("Filters & Options")
    with st.sidebar.expander("Bear & Date Selection", expanded=True):
        bear_ids = bears_info_df['bear_id'].tolist()
        select_all_bears = st.checkbox("Select All Bears", value=True)
        if select_all_bears: selected_bears = bear_ids; st.sidebar.multiselect("Selected Bears", bear_ids, default=bear_ids, disabled=True)
        else: selected_bears = st.sidebar.multiselect("Select Bear(s)", bear_ids, default=bear_ids[0] if bear_ids else None)
        if min_date_db and max_date_db:
            start_date_filter = st.date_input("Start Date", min_date_db, min_value=min_date_db, max_value=max_date_db)
            end_date_filter = st.date_input("End Date", max_date_db, min_value=min_date_db, max_value=max_date_db)
            start_datetime = datetime.combine(start_date_filter, datetime.min.time()); end_datetime = datetime.combine(end_date_filter, datetime.max.time())
        else:
            st.warning("Using default date range.")
            start_datetime, end_datetime = datetime(2010, 1, 1), datetime.now()
            st.date_input("Start Date", start_datetime.date()); st.date_input("End Date", end_datetime.date())
    with st.sidebar.expander("Attribute Filters", expanded=False):
        selected_season = st.selectbox("Filter by Season", available_seasons, index=0)
        available_sexes = ['All'] + sorted([s for s in bears_info_df['sex'].unique() if pd.notna(s)])
        available_ages = ['All'] + sorted([a for a in bears_info_df['age'].unique() if pd.notna(a)])
        selected_sex_filter = st.selectbox("Filter by Sex", available_sexes, index=0)
        selected_age_filter = st.selectbox("Filter by Age", available_ages, index=0)
    with st.sidebar.expander("Map Options", expanded=True):
        color_by_options = ['Bear ID', 'Sex', 'Age', 'Season']; color_by = st.selectbox("Color code points by:", color_by_options, index=0)
        map_type_options = ('Individual Points', 'Movement Paths', 'Heatmap', 'Animated Path'); map_type = st.radio("Map Display Type", map_type_options, index=0)

    # Data Loading & Processing
    if not selected_bears: st.warning("Select bear(s)."); st.stop()
    query_season = selected_season if selected_season != 'All' else None
    processed_data, daily_stats_data = load_and_process_data(DB_PATH, selected_bears, start_datetime, end_datetime, query_season, selected_sex_filter, selected_age_filter)

    # Color Assignment & Legend
    # ... (Keep existing color assignment logic) ...
    color_column_name = 'dynamic_color'; legend_title = "Legend"; color_map = {}; legend_map = {}
    if not processed_data.empty:
        sex_colors = {'M': '#2196F3', 'F': '#E91E63', 'Unknown': '#808080'}; age_colors = {'A': '#9C27B0', 'J': '#00BCD4', 'Unknown': '#808080'}
        unique_seasons = sorted([s for s in processed_data['season'].unique() if pd.notna(s)])
        season_palette = px.colors.qualitative.Pastel; season_colors = {season: season_palette[i % len(season_palette)] for i, season in enumerate(unique_seasons)}; season_colors['Unknown'] = '#808080'
        unique_bears_filtered = sorted(processed_data['bear_id'].unique()); bear_palette = px.colors.qualitative.Plotly
        bear_colors = {bear: bear_palette[i % len(bear_palette)] for i, bear in enumerate(unique_bears_filtered)}
        if color_by == 'Sex': processed_data[color_column_name] = processed_data['sex'].fillna('Unknown').map(sex_colors); legend_title = "Sex"; legend_map = sex_colors
        elif color_by == 'Age': processed_data[color_column_name] = processed_data['age'].fillna('Unknown').map(age_colors); legend_title = "Age"; legend_map = age_colors
        elif color_by == 'Season': processed_data[color_column_name] = processed_data['season'].fillna('Unknown').map(season_colors); legend_title = "Season"; legend_map = {s: c for s, c in season_colors.items() if s in unique_seasons or (s == 'Unknown' and processed_data['season'].isnull().any())}
        else: processed_data[color_column_name] = processed_data['bear_id'].map(bear_colors); legend_title = "Bear ID"; legend_map = bear_colors

    # Main Content Area
    map_tab, charts_tab, stats_tab = st.tabs(["Map View", "Charts", "Statistics"])
    with map_tab:
        st.subheader("Bear Movement Map")
        if not processed_data.empty and 'latitude' in processed_data.columns:
            try: map_center = [processed_data['latitude'].mean(), processed_data['longitude'].mean()]; assert not pd.isna(map_center).any()
            except: map_center = [45.9, 25.3] # Default
            # --- Create Map (Simplified Tiles) ---
            m = folium.Map(location=map_center, zoom_start=9, tiles="CartoDB positron") # Use CartoDB positron only

            # --- Add Data Layers ---
            if map_type == 'Heatmap':
                heat_data = processed_data[['latitude', 'longitude']].dropna().values.tolist()
                if heat_data:
                    HeatMap(heat_data, radius=15).add_to(m)
                    # --- FIX: Reset map location ---
                    m.location = map_center # Force center after adding heatmap
                    # --- END FIX ---
                else: st.info("No valid points for heatmap.")
            elif map_type == 'Movement Paths':
                 bear_groups = processed_data.groupby('bear_id')
                 for bear_name, group in bear_groups:
                     group = group.sort_values('timestamp')
                     coordinates = group[['latitude', 'longitude']].dropna().values.tolist()
                     line_color = group.iloc[0][color_column_name] if not group.empty else '#808080'
                     if len(coordinates) > 1: folium.PolyLine(locations=coordinates, color=line_color, weight=2.5, opacity=0.8, tooltip=f"Path for {bear_name}").add_to(m)
            elif map_type == 'Animated Path':
                 features = get_geojson_features(processed_data.sort_values(by='timestamp'), color_column_name)
                 if features: TimestampedGeoJson({'type': 'FeatureCollection','features': features}, period='PT1H', add_last_point=True, duration='PT1M', transition_time=200, auto_play=True).add_to(m)
                 else: st.info("No valid points for animation.")
            else: # Default: Individual Points
                mc = MarkerCluster(name="Bear Points").add_to(m)
                for idx, row in processed_data.iterrows():
                    lat = row['latitude']; lng = row['longitude']; point_color = row.get(color_column_name, '#808080')
                    if not (pd.isna(lat) or pd.isna(lng)):
                        popup_html = (f"<b>Bear:</b> {row['bear_id']}<br><b>Time:</b> {row['timestamp'].strftime('%Y-%m-%d %H:%M')}<br><b>Speed:</b> {row.get('speed_m_h', 'N/A'):.1f} m/h<br><b>Season:</b> {row.get('season', 'N/A')}<br><b>Sex:</b> {row.get('sex', 'N/A')}<br><b>Age:</b> {row.get('age', 'N/A')}")
                        tooltip_text = f"Bear: {row['bear_id']} | Sex: {row['sex']} | Age: {row['age']} | Season: {row['season']} | Time: {row['timestamp'].strftime('%H:%M')}"
                        folium.CircleMarker(location=[lat, lng], radius=4, popup=folium.Popup(popup_html, max_width=250), tooltip=tooltip_text, color=point_color, fill=True, fill_color=point_color, fill_opacity=0.7, marker_id=f"point_{idx}").add_to(mc)

            if legend_map: legend_html = generate_legend_html(legend_title, legend_map); m.get_root().html.add_child(folium.Element(legend_html))
            map_key = f"map_{color_by}_{map_type}_{selected_season}_{selected_sex_filter}_{selected_age_filter}"
            map_data = display_folium_map_efficiently(m, key=map_key)
        elif not processed_data.empty and 'latitude' not in processed_data.columns: st.error("Reprojection failed.")
        else: st.warning("No data found with the current filters.")

    # --- Charts Tab ---
    with charts_tab:
        # (Keep existing charts code)
        st.subheader("Analysis Charts")
        if not processed_data.empty:
             chart_options = ["Activity by Hour", "Points per Season", "Points per Sex", "Points per Age", "Speed Distribution", "Daily Distance Distribution"]
             chart_type = st.selectbox("Select Chart Type", chart_options)
             if chart_type == "Activity by Hour":
                 hourly_activity = processed_data.groupby(processed_data['timestamp'].dt.hour).size().reset_index(name='count')
                 fig = px.bar(hourly_activity, x='timestamp', y='count', title="Activity by Hour of Day", labels={'timestamp': 'Hour', 'count': 'Number of Points'})
                 st.plotly_chart(fig, use_container_width=True)
             elif chart_type == "Points per Season":
                 season_activity = processed_data.groupby('season').size().reset_index(name='count')
                 fig = px.pie(season_activity, names='season', values='count', title="Activity Distribution by Season", color='season', color_discrete_map=season_colors)
                 st.plotly_chart(fig, use_container_width=True)
             elif chart_type == "Points per Sex":
                 sex_activity = processed_data.groupby('sex').size().reset_index(name='count')
                 fig = px.pie(sex_activity, names='sex', values='count', title="Activity Distribution by Sex", color='sex', color_discrete_map=sex_colors)
                 st.plotly_chart(fig, use_container_width=True)
             elif chart_type == "Points per Age":
                 age_activity = processed_data.groupby('age').size().reset_index(name='count')
                 fig = px.pie(age_activity, names='age', values='count', title="Activity Distribution by Age", color='age', color_discrete_map=age_colors)
                 st.plotly_chart(fig, use_container_width=True)
             elif chart_type == "Speed Distribution":
                 color_column = 'bear_id' if color_by == 'Bear ID' else color_by.lower() if color_by in ['Sex', 'Age', 'Season'] else 'bear_id'
                 color_map_plotly = legend_map if color_by != 'Bear ID' else bear_colors
                 fig_speed_hist = px.histogram(processed_data[processed_data['speed_m_h'] > 0], x='speed_m_h', color=color_column if color_column in processed_data.columns else None, title=f'Speed Distribution (colored by {color_by})', labels={'speed_m_h': 'Speed (m/h)'}, barmode='overlay', color_discrete_map=color_map_plotly)
                 fig_speed_hist.update_layout(legend_title_text=color_by); st.plotly_chart(fig_speed_hist, use_container_width=True)
             elif chart_type == "Daily Distance Distribution":
                 if not daily_stats_data.empty:
                     color_column = 'bear_id'; color_map_plotly = bear_colors
                     fig_daily_dist = px.histogram(daily_stats_data, x='daily_distance_m', color=color_column, title='Daily Distance Distribution', labels={'daily_distance_m': 'Daily Distance (m)'}, barmode='overlay', color_discrete_map=color_map_plotly)
                     fig_daily_dist.update_layout(legend_title_text="Bear ID"); st.plotly_chart(fig_daily_dist, use_container_width=True)
                 else: st.info("No daily distance data available.")
        else: st.warning("No data available for charts.")

    # --- Stats Tab ---
    with stats_tab:
        st.subheader("Movement Statistics Summary")
        if not processed_data.empty:
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Points Displayed", f"{len(processed_data):,}")
            col2.metric("Bears Displayed", processed_data['bear_id'].nunique())
            time_delta_days = (processed_data['timestamp'].max() - processed_data['timestamp'].min()).days
            col3.metric("Time Span (days)", f"{time_delta_days if time_delta_days is not None else 0}")
            describe_cols = ['bear_id', 'timestamp', 'season', 'sex', 'age', 'speed_m_h', 'distance_m']
            existing_describe_cols = [col for col in describe_cols if col in processed_data.columns]
            describe_df = processed_data[existing_describe_cols].copy()
            if 'bear_id' in describe_df.columns: describe_df['bear_id'] = describe_df['bear_id'].astype(str)
            # --- FIX: Use st.table() for describe output ---
            st.table(describe_df.describe(include='all'))
            if len(selected_bears) >= 2 and processed_data['bear_id'].nunique() >= 2:
                 st.subheader("Comparison Between Selected Bears")
                 if not daily_stats_data.empty:
                     stats_df = daily_stats_data.groupby('bear_id')['daily_distance_m'].describe()
                     stats_df = stats_df.reset_index().rename(columns={'bear_id': 'Bear ID', 'mean': 'Mean Dist', 'std': 'Std Dev', '50%': 'Median Dist', 'count': 'Days'})
                     display_cols = ['Bear ID', 'Days', 'Mean Dist', 'Median Dist', 'Std Dev', 'min', 'max']
                     existing_display_cols = [col for col in display_cols if col in stats_df.columns]
                     stats_df_display = stats_df.loc[:, existing_display_cols].copy()
                     if 'Bear ID' in stats_df_display.columns: stats_df_display['Bear ID'] = stats_df_display['Bear ID'].astype(str)
                     for col in stats_df_display.columns:
                         if pd.api.types.is_numeric_dtype(stats_df_display[col]) and col != 'Days': stats_df_display[col] = stats_df_display[col].round(1)
                     # --- FIX: Use st.table() for comparison output ---
                     st.table(stats_df_display)
                     fig_box = px.box(daily_stats_data, x='bear_id', y='daily_distance_m', color='bear_id', title="Daily Distance Distribution Comparison", labels={'daily_distance_m': 'Daily Distance (m)', 'bear_id': 'Bear ID'}, color_discrete_map=bear_colors)
                     fig_box.update_layout(showlegend=False); st.plotly_chart(fig_box, use_container_width=True)
                 else: st.info("No distance data for comparison.")
        else:
            st.warning("No data found with the current filters.")

# # Test block
# if __name__ == "__main__":
#     display_carpathian_bears_section()