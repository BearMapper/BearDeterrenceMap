"""
Carpathian Bears Movement Analysis
Interactive visualization and analysis for bear movement in the Carpathian Mountains
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import json
from wildlife_db import WildlifeDatabase
import calendar
import folium
from streamlit_folium import folium_static
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO
import base64
import math
import sqlite3

# Initialize the database connection
db = WildlifeDatabase()


def display_carpathian_bears_section():
    """Main function to display the Carpathian Bears visualization dashboard"""
    st.title("🐻 Carpathian Bears Movement Analysis")
    
    # Create tabs for different visualizations
    tabs = st.tabs([
        "🗺️ Movement Map", 
        "📊 Home Range Analysis", 
        "📈 Seasonal Patterns", 
        "📋 Data Explorer",
        "📑 About"
    ])
    
    # Get list of bears
    bears_df = db.get_bears_list()
    if bears_df.empty:
        st.error("No bear data found in the database. Please ensure data is imported correctly.")
        st.info("Run the data migration script to import Carpathian Bears data.")
        return
        
    # Get distinct seasons
    seasons = db.get_distinct_values("bears_tracking", "season")
    if not seasons:
        seasons = ["Winter", "Spring", "Summer", "Fall"]
    
    # Get date range for the entire dataset
    try:
        min_date, max_date = db.get_date_range("bears_tracking", "timestamp")
    except Exception as e:
        st.warning(f"Using default date range due to error: {e}")
        min_date = datetime(2018, 1, 1).date()
        max_date = datetime(2023, 12, 31).date()
    
    # Sidebar filters
    with st.sidebar:
        st.header("Filters")
        
        # Bear selection
        all_bears = sorted(bears_df['bear_id'].unique().tolist())
        selected_bears = st.multiselect(
            "Select Bears",
            options=all_bears,
            default=all_bears[:min(3, len(all_bears))]  # Default to first 3 bears or fewer
        )
        
        # Sex filter
        all_sexes = sorted([sex for sex in bears_df['sex'].unique() if isinstance(sex, str)])
        selected_sexes = st.multiselect(
            "Filter by Sex",
            options=all_sexes,
            default=all_sexes
        )
        
        # Age filter
        all_ages = sorted([age for age in bears_df['age'].unique() if isinstance(age, str)])
        selected_ages = st.multiselect(
            "Filter by Age Class",
            options=all_ages,
            default=all_ages
        )
        
        # Create a mapping of age codes to descriptions
        age_descriptions = {
            "A": "Adult", 
            "J": "Subadult"
        }
        
        # Season filter
        selected_seasons = st.multiselect(
            "Filter by Season",
            options=seasons,
            default=seasons
        )
        
        # Date range filter
        st.subheader("Date Range")
        date_range = st.date_input(
            "Select Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
        
        if len(date_range) == 2:
            start_date, end_date = date_range
        else:
            start_date, end_date = min_date, max_date
            
        # Apply filters button
        apply_filters = st.button("Apply Filters", type="primary")
        reset_filters = st.button("Reset Filters")
        
        if reset_filters:
            selected_bears = all_bears[:min(3, len(all_bears))]
            selected_sexes = all_sexes
            selected_ages = all_ages
            selected_seasons = seasons
            start_date, end_date = min_date, max_date
    
    # Filter bears based on selected criteria
    filtered_bears = []
    if selected_bears:
        for bear in selected_bears:
            bear_data = bears_df[bears_df['bear_id'] == bear]
            if bear_data.empty:
                continue
                
            # Check if bear meets sex and age criteria
            if selected_sexes and selected_ages:
                bear_sex = bear_data['sex'].iloc[0] if not pd.isna(bear_data['sex'].iloc[0]) else None
                bear_age = bear_data['age'].iloc[0] if not pd.isna(bear_data['age'].iloc[0]) else None
                
                if (bear_sex in selected_sexes or bear_sex is None) and (bear_age in selected_ages or bear_age is None):
                    filtered_bears.append(bear)
            else:
                filtered_bears.append(bear)
    else:
        filtered_bears = all_bears
    
    # Convert dates to datetime for filtering
    start_datetime = datetime.combine(start_date, datetime.min.time())
    end_datetime = datetime.combine(end_date, datetime.max.time())  
    # -- Movement Map Tab --
    with tabs[0]:
        st.header("Bear Movement Map")
        
        col1, col2 = st.columns([3, 1])
        
        with col2:
            st.subheader("Map Settings")
            
            map_type = st.selectbox(
                "Map Type",
                options=["Movement Paths", "Heatmap", "Daily Positions"],
                index=0
            )
            
            show_labels = st.checkbox("Show Bear Labels", value=True)
            
            if map_type == "Movement Paths":
                line_width = st.slider("Path Width", 1, 10, 3)
                show_points = st.checkbox("Show Points", value=True)
                point_size = st.slider("Point Size", 1, 20, 5) if show_points else 5
            
            elif map_type == "Heatmap":
                heat_intensity = st.slider("Heatmap Intensity", 1, 50, 10)
                heat_radius = st.slider("Heatmap Radius", 5, 50, 15)
                
            elif map_type == "Daily Positions":
                point_size = st.slider("Point Size", 1, 20, 8)
                color_by = st.selectbox(
                    "Color By",
                    options=["Bear", "Season", "Sex", "Age"],
                    index=0
                )
        
        with col1:
            with st.spinner("Loading map data..."):
                # Fetch data based on filters
                if filtered_bears:
                    # Get tracking data for selected bears
                    tracking_data = pd.DataFrame()
                    
                    for bear_id in filtered_bears:
                        bear_data = db.get_bear_data(
                            bear_id=bear_id,
                            start_date=start_datetime,
                            end_date=end_datetime,
                            season=selected_seasons[0] if len(selected_seasons) == 1 else None
                        )
                        
                        if not bear_data.empty:
                            # Filter by season if multiple seasons selected
                            if len(selected_seasons) > 1:
                                bear_data = bear_data[bear_data['season'].isin(selected_seasons)]
                                
                            tracking_data = pd.concat([tracking_data, bear_data])
                    
                    if tracking_data.empty:
                        st.warning("No data available for the selected filters.")
                    else:
                        # Create map
                        if map_type == "Movement Paths":
                            create_movement_map(tracking_data, show_labels, line_width, show_points, point_size)
                        elif map_type == "Heatmap":
                            create_heatmap(tracking_data, show_labels, heat_intensity, heat_radius)
                        elif map_type == "Daily Positions":
                            create_daily_positions_map(tracking_data, show_labels, point_size, color_by)
                else:
                    st.warning("Please select at least one bear to display data.")
    
    # -- Home Range Analysis Tab --
    with tabs[1]:
        st.header("Home Range Analysis")
        
        col1, col2 = st.columns([3, 1])
        
        with col2:
            st.subheader("Analysis Settings")
            
            hr_chart_type = st.selectbox(
                "Chart Type",
                options=["Bar Chart", "Box Plot", "Scatter Plot"],
                index=0
            )
            
            hr_compare = st.selectbox(
                "Compare By",
                options=["Individual", "Sex", "Age"],
                index=0
            )
            
            hr_metric = st.selectbox(
                "Home Range Metric",
                options=["MCP (95%)", "KDE", "Core Area (50%)"],
                index=0
            )
            
            hr_unit = st.selectbox(
                "Area Unit",
                options=["km²", "hectares"],
                index=0
            )
            
            show_seasonal = st.checkbox("Show Seasonal Variation", value=False)
        
        with col1:
            with st.spinner("Loading home range data..."):
                # Get home range data
                if filtered_bears:
                    home_range_data = pd.DataFrame()
                    
                    for bear_id in filtered_bears:
                        bear_hr = db.get_home_range_data(bear_id=bear_id)
                        if not bear_hr.empty:
                            home_range_data = pd.concat([home_range_data, bear_hr])
                    
                    if home_range_data.empty:
                        st.warning("No home range data available for the selected bears.")
                    else:
                        # Apply unit conversion if needed
                        if hr_unit == "hectares":
                            home_range_data['mcp_area'] = home_range_data['mcp_area'] * 100  # km² to hectares
                            home_range_data['core_area'] = home_range_data['core_area'] * 100
                            home_range_data['kde_area'] = home_range_data['kde_area'] * 100
                        
                        # Select metric column
                        if hr_metric == "MCP (95%)":
                            metric_col = 'mcp_area'
                            metric_label = "MCP 95% Area"
                        elif hr_metric == "KDE":
                            metric_col = 'kde_area'
                            metric_label = "KDE Area"
                        else:  # Core Area
                            metric_col = 'core_area'
                            metric_label = "Core Area (50%)"
                            
                        # Create visualization based on settings
                        if show_seasonal and hr_compare == "Individual":
                            # Get seasonal home range data
                            seasonal_data = pd.DataFrame()
                            
                            for bear_id in filtered_bears:
                                # Query seasonal MCP data from database
                                query = f"SELECT * FROM bears_seasonal_mcp WHERE bear_id = '{bear_id}'"
                                conn = None
                                try:
                                    conn = sqlite3.connect(db.db_path)  
                                    bear_seasonal = pd.read_sql(query, conn)
                                    if not bear_seasonal.empty:
                                        # Join with bears_tracking to get sex and age
                                        bear_info = tracking_data[tracking_data['bear_id'] == bear_id][['sex', 'age']].drop_duplicates().iloc[0]
                                        bear_seasonal['sex'] = bear_info['sex']
                                        bear_seasonal['age'] = bear_info['age']
                                        seasonal_data = pd.concat([seasonal_data, bear_seasonal])
                                except:
                                    # Fall back to get_seasonal_mcp_data method
                                    try:
                                        bear_seasonal = db.get_seasonal_data(bear_id=bear_id)
                                        if not bear_seasonal.empty:
                                            seasonal_data = pd.concat([seasonal_data, bear_seasonal])
                                    except:
                                        pass
                                finally:
                                    if conn: conn.close()
                            
                            if not seasonal_data.empty:
                                # Apply unit conversion if needed
                                if hr_unit == "hectares":
                                    seasonal_data['area'] = seasonal_data['area'] * 100
                                
                                # Create seasonal home range chart
                                fig = px.bar(
                                    seasonal_data,
                                    x='bear_id',
                                    y='area',
                                    color='season',
                                    title=f"Seasonal {metric_label} by Bear ID",
                                    labels={'area': f"Area ({hr_unit})", 'bear_id': 'Bear ID', 'season': 'Season'},
                                    barmode='group'
                                )
                                
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.warning("No seasonal home range data available.")
                        else:
                            # Create standard home range visualization
                            if hr_chart_type == "Bar Chart":
                                if hr_compare == "Individual":
                                    fig = px.bar(
                                        home_range_data,
                                        x='bear_id',
                                        y=metric_col,
                                        color='sex',
                                        pattern_shape='age',
                                        title=f"{metric_label} by Bear ID",
                                        labels={metric_col: f"Area ({hr_unit})", 'bear_id': 'Bear ID', 'sex': 'Sex', 'age': 'Age'}
                                    )
                                elif hr_compare == "Sex":
                                    fig = px.bar(
                                        home_range_data,
                                        x='sex',
                                        y=metric_col,
                                        color='sex',
                                        pattern_shape='age',
                                        title=f"{metric_label} by Sex",
                                        labels={metric_col: f"Area ({hr_unit})", 'sex': 'Sex', 'age': 'Age'}
                                    )
                                else:  # Age
                                    fig = px.bar(
                                        home_range_data,
                                        x='age',
                                        y=metric_col,
                                        color='sex',
                                        title=f"{metric_label} by Age Class",
                                        labels={metric_col: f"Area ({hr_unit})", 'age': 'Age Class', 'sex': 'Sex'}
                                    )
                                    
                                    # Replace age codes with descriptions
                                    new_x_labels = {k: age_descriptions.get(k, k) for k in home_range_data['age'].unique()}
                                    if new_x_labels:
                                        fig.update_layout(xaxis=dict(tickmode='array', tickvals=list(new_x_labels.keys()), ticktext=list(new_x_labels.values())))
                                
                                st.plotly_chart(fig, use_container_width=True)
                                
                            elif hr_chart_type == "Box Plot":
                                if hr_compare == "Individual":
                                    fig = px.box(
                                        home_range_data,
                                        x='bear_id',
                                        y=metric_col,
                                        color='sex',
                                        points="all",
                                        title=f"{metric_label} by Bear ID",
                                        labels={metric_col: f"Area ({hr_unit})", 'bear_id': 'Bear ID', 'sex': 'Sex'}
                                    )
                                elif hr_compare == "Sex":
                                    fig = px.box(
                                        home_range_data,
                                        x='sex',
                                        y=metric_col,
                                        color='sex',
                                        points="all",
                                        title=f"{metric_label} by Sex",
                                        labels={metric_col: f"Area ({hr_unit})", 'sex': 'Sex'}
                                    )
                                else:  # Age
                                    fig = px.box(
                                        home_range_data,
                                        x='age',
                                        y=metric_col,
                                        color='sex',
                                        points="all",
                                        title=f"{metric_label} by Age Class",
                                        labels={metric_col: f"Area ({hr_unit})", 'age': 'Age Class', 'sex': 'Sex'}
                                    )
                                    
                                    # Replace age codes with descriptions
                                    new_x_labels = {k: age_descriptions.get(k, k) for k in home_range_data['age'].unique()}
                                    if new_x_labels:
                                        fig.update_layout(xaxis=dict(tickmode='array', tickvals=list(new_x_labels.keys()), ticktext=list(new_x_labels.values())))
                                
                                st.plotly_chart(fig, use_container_width=True)
                                
                            else:  # Scatter Plot
                                # For scatter plot, we need two metrics to compare
                                second_metric = "core_area" if metric_col != "core_area" else "kde_area"
                                second_label = "Core Area (50%)" if second_metric == "core_area" else "KDE Area"
                                
                                fig = px.scatter(
                                    home_range_data,
                                    x=metric_col,
                                    y=second_metric,
                                    color='sex',
                                    symbol='age',
                                    hover_name='bear_id',
                                    size=metric_col,
                                    size_max=30,
                                    title=f"Relationship between {metric_label} and {second_label}",
                                    labels={
                                        metric_col: f"{metric_label} ({hr_unit})", 
                                        second_metric: f"{second_label} ({hr_unit})", 
                                        'sex': 'Sex', 
                                        'age': 'Age'
                                    }
                                )
                                
                                # Add regression line
                                fig.update_layout(
                                    shapes=[
                                        dict(
                                            type='line',
                                            x0=home_range_data[metric_col].min(),
                                            y0=np.poly1d(np.polyfit(home_range_data[metric_col], home_range_data[second_metric], 1))(home_range_data[metric_col].min()),
                                            x1=home_range_data[metric_col].max(),
                                            y1=np.poly1d(np.polyfit(home_range_data[metric_col], home_range_data[second_metric], 1))(home_range_data[metric_col].max()),
                                            line=dict(color="gray", width=2, dash="dash")
                                        )
                                    ]
                                )
                                
                                st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Please select at least one bear to display data.")
    
    # -- Seasonal Patterns Tab --
    with tabs[2]:
        st.header("Seasonal Movement Patterns")
        
        col1, col2 = st.columns([3, 1])
        
        with col2:
            st.subheader("Analysis Settings")
            
            seasonal_chart_type = st.selectbox(
                "Chart Type",
                options=["Area Chart", "Line Chart", "Heatmap"],
                index=0,
                key="seasonal_chart_type"
            )
            
            seasonal_metric = st.selectbox(
                "Movement Metric",
                options=["Distance Traveled", "Home Range Size", "Altitude"],
                index=0
            )
            
            seasonal_groupby = st.selectbox(
                "Group By",
                options=["Individual", "Sex", "Age"],
                index=0
            )
            
            time_resolution = st.selectbox(
                "Time Resolution",
                options=["Daily", "Weekly", "Monthly", "Seasonal"],
                index=3
            )
        
        with col1:
            with st.spinner("Loading seasonal data..."):
                # Get daily movement data for seasonal analysis
                if filtered_bears:
                    # Use db.get_daily_movement or similar method
                    daily_data = pd.DataFrame()
                    
                    for bear_id in filtered_bears:
                        # Get daily movement data from database
                        bear_daily = db.get_daily_movement(
                            bear_id=bear_id,
                            start_date=start_datetime,
                            end_date=end_datetime
                        )
                        
                        if not bear_daily.empty:
                            daily_data = pd.concat([daily_data, bear_daily])
                    
                    if daily_data.empty:
                        # Alternative: derive daily data from tracking data
                        if 'tracking_data' in locals() and not tracking_data.empty:
                            # Group tracking data by bear and day
                            tracking_data['date'] = tracking_data['timestamp'].dt.date
                            
                            daily_data = tracking_data.groupby(['bear_id', 'date', 'sex', 'age', 'season']).agg({
                                'x': ['min', 'max', 'mean'],
                                'y': ['min', 'max', 'mean'],
                                'lat': ['min', 'max', 'mean'],
                                'lng': ['min', 'max', 'mean']
                            }).reset_index()
                            
                            # Calculate daily movement metrics
                            daily_data.columns = ['_'.join(col).strip() for col in daily_data.columns.values]
                            daily_data.rename(columns={
                                'bear_id_': 'bear_id',
                                'date_': 'date',
                                'sex_': 'sex',
                                'age_': 'age',
                                'season_': 'season'
                            }, inplace=True)
                            
                            # Calculate approximate daily distance
                            daily_data['distance'] = np.sqrt(
                                (daily_data['x_max'] - daily_data['x_min'])**2 + 
                                (daily_data['y_max'] - daily_data['y_min'])**2
                            )
                    
                    if daily_data.empty:
                        st.warning("No daily movement data available for the selected bears.")
                    else:
                        # Prepare data based on time resolution
                        if 'date' in daily_data.columns:
                            try:
                                daily_data['date'] = pd.to_datetime(daily_data['date'])
                                daily_data['year'] = daily_data['date'].dt.year
                                daily_data['month'] = daily_data['date'].dt.month
                                daily_data['week'] = daily_data['date'].dt.isocalendar().week
                                
                                # Add month name
                                daily_data['month_name'] = daily_data['month'].apply(lambda x: calendar.month_name[x])
                            except:
                                pass
                        
                        # Setup grouping based on time resolution
                        if time_resolution == "Daily":
                            time_group = 'date'
                            time_label = 'Date'
                        elif time_resolution == "Weekly":
                            time_group = ['year', 'week']
                            time_label = 'Week'
                            # Create week-year label
                            daily_data['week_year'] = daily_data['year'].astype(str) + '-W' + daily_data['week'].astype(str).str.zfill(2)
                            time_group = 'week_year'
                        elif time_resolution == "Monthly":
                            time_group = ['year', 'month']
                            time_label = 'Month'
                            # Create month-year label
                            daily_data['month_year'] = daily_data['month_name'] + ' ' + daily_data['year'].astype(str)
                            time_group = 'month_year'
                        else:  # Seasonal
                            time_group = 'season'
                            time_label = 'Season'
                        
                        # Setup grouping based on selected grouping variable
                        if seasonal_groupby == "Individual":
                            group_var = 'bear_id'
                            group_label = 'Bear ID'
                        elif seasonal_groupby == "Sex":
                            group_var = 'sex'
                            group_label = 'Sex'
                        else:  # Age
                            group_var = 'age'
                            group_label = 'Age Class'
                        
                        # Prepare metric for visualization
                        if seasonal_metric == "Distance Traveled":
                            if 'distance' in daily_data.columns:
                                metric_col = 'distance'
                            else:
                                # Calculate approximate distance if not available
                                daily_data['distance'] = np.sqrt(
                                    (daily_data['x_max'] - daily_data['x_min'])**2 + 
                                    (daily_data['y_max'] - daily_data['y_min'])**2
                                )
                                metric_col = 'distance'
                            metric_label = 'Distance Traveled (m)'
                        elif seasonal_metric == "Home Range Size":
                            # Calculate daily movement area as a proxy for home range
                            daily_data['area'] = (daily_data['x_max'] - daily_data['x_min']) * (daily_data['y_max'] - daily_data['y_min'])
                            metric_col = 'area'
                            metric_label = 'Daily Range Area (m²)'
                                                
                        else:  # Altitude
                            if 'altitude' in daily_data.columns:
                                metric_col = 'altitude'
                                metric_label = 'Altitude (m)'
                            else:
                                # If altitude not available, show warning
                                st.warning("Altitude data not available. Switching to Distance Traveled.")
                                if 'distance' in daily_data.columns:
                                    metric_col = 'distance'
                                else:
                                    daily_data['distance'] = np.sqrt(
                                        (daily_data['x_max'] - daily_data['x_min'])**2 + 
                                        (daily_data['y_max'] - daily_data['y_min'])**2
                                    )
                                    metric_col = 'distance'
                                metric_label = 'Distance Traveled (m)'
                        
                        # Aggregate data for visualization
                        if time_resolution == "Seasonal":
                            # Seasonal aggregation needs special handling
                            agg_data = daily_data.groupby([group_var, time_group]).agg({
                                metric_col: 'mean'
                            }).reset_index()
                            
                            # Create visualization
                            if seasonal_chart_type == "Area Chart":
                                fig = px.area(
                                    agg_data,
                                    x=time_group,
                                    y=metric_col,
                                    color=group_var,
                                    title=f"Average {metric_label} by {time_label} and {group_label}",
                                    labels={metric_col: metric_label, time_group: time_label, group_var: group_label}
                                )
                                st.plotly_chart(fig, use_container_width=True)
                            elif seasonal_chart_type == "Line Chart":
                                fig = px.line(
                                    agg_data,
                                    x=time_group,
                                    y=metric_col,
                                    color=group_var,
                                    markers=True,
                                    title=f"Average {metric_label} by {time_label} and {group_label}",
                                    labels={metric_col: metric_label, time_group: time_label, group_var: group_label}
                                )
                                st.plotly_chart(fig, use_container_width=True)
                            else:  # Heatmap
                                # Pivot data for heatmap
                                if group_var in agg_data.columns and time_group in agg_data.columns:
                                    pivot_data = agg_data.pivot(index=group_var, columns=time_group, values=metric_col)
                                    
                                    fig = px.imshow(
                                        pivot_data,
                                        title=f"{metric_label} Heatmap by {group_label} and {time_label}",
                                        labels=dict(x=time_label, y=group_label, color=metric_label),
                                        color_continuous_scale="Viridis"
                                    )
                                    st.plotly_chart(fig, use_container_width=True)
                                else:
                                    st.warning(f"Cannot create heatmap: missing columns {group_var} or {time_group}.")
                        else:
                            # Time-based aggregation
                            try:
                                agg_data = daily_data.groupby([group_var, time_group]).agg({
                                    metric_col: 'mean'
                                }).reset_index()
                                
                                # Sort data by time
                                if time_resolution == "Monthly":
                                    # Create a proper sort order for months
                                    month_order = {month: i for i, month in enumerate(calendar.month_name[1:])}
                                    agg_data['montƒh_order'] = agg_data['month_year'].apply(
                                        lambda x: month_order.get(x.split()[0], 0) + (int(x.split()[1]) - daily_data['year'].min()) * 12
                                    )
                                    agg_data = agg_data.sort_values(['month_order', group_var])
                                    time_col = 'month_year'
                                elif time_resolution == "Weekly":
                                    agg_data = agg_data.sort_values(['week_year', group_var])
                                    time_col = 'week_year'
                                else:  # Daily
                                    agg_data = agg_data.sort_values(['date', group_var])
                                    time_col = 'date'
                                
                                # Create visualization
                                if seasonal_chart_type == "Area Chart":
                                    fig = px.area(
                                        agg_data,
                                        x=time_col,
                                        y=metric_col,
                                        color=group_var,
                                        title=f"Average {metric_label} by {time_label} and {group_label}",
                                        labels={metric_col: metric_label, time_col: time_label, group_var: group_label}
                                    )
                                    st.plotly_chart(fig, use_container_width=True)
                                elif seasonal_chart_type == "Line Chart":
                                    fig = px.line(
                                        agg_data,
                                        x=time_col,
                                        y=metric_col,
                                        color=group_var,
                                        markers=True,
                                        title=f"Average {metric_label} by {time_label} and {group_label}",
                                        labels={metric_col: metric_label, time_col: time_label, group_var: group_label}
                                    )
                                    st.plotly_chart(fig, use_container_width=True)
                                else:  # Heatmap
                                    # Pivot data for heatmap
                                    pivot_data = agg_data.pivot(index=group_var, columns=time_col, values=metric_col)
                                    
                                    fig = px.imshow(
                                        pivot_data,
                                        title=f"{metric_label} Heatmap by {group_label} and {time_label}",
                                        labels=dict(x=time_label, y=group_label, color=metric_label),
                                        color_continuous_scale="Viridis"
                                    )
                                    st.plotly_chart(fig, use_container_width=True)
                            except Exception as e:
                                st.error(f"Error creating chart: {e}")
                else:
                    st.warning("Please select at least one bear to display data.")
    
    # -- Data Explorer Tab --
    with tabs[3]:
        st.header("Data Explorer")
        
        # Create sub-tabs for different data types
        data_tabs = st.tabs([
            "Tracking Data", 
            "Home Range Data", 
            "Daily Movement", 
            "Statistics"
        ])
        
        # Tracking Data Tab
        with data_tabs[0]:
            st.subheader("Bear Tracking Data")
            
            if filtered_bears:
                # Get tracking data based on filters
                tracking_data = pd.DataFrame()
                
                for bear_id in filtered_bears:
                    bear_data = db.get_bear_data(
                        bear_id=bear_id,
                        start_date=start_datetime,
                        end_date=end_datetime,
                        season=selected_seasons[0] if len(selected_seasons) == 1 else None
                    )
                    
                    if not bear_data.empty:
                        # Filter by season if multiple seasons selected
                        if len(selected_seasons) > 1:
                            bear_data = bear_data[bear_data['season'].isin(selected_seasons)]
                            
                        tracking_data = pd.concat([tracking_data, bear_data])
                
                if tracking_data.empty:
                    st.warning("No tracking data available for the selected filters.")
                else:
                    # Display data overview
                    st.write(f"**Total records:** {len(tracking_data)}")
                    st.write(f"**Date range:** {tracking_data['timestamp'].min()} to {tracking_data['timestamp'].max()}")
                    
                    # Add download button
                    csv = tracking_data.to_csv(index=False)
                    st.download_button(
                        label="Download Data as CSV",
                        data=csv,
                        file_name="bear_tracking_data.csv",
                        mime="text/csv"
                    )
                    
                    # Display data table
                    st.dataframe(tracking_data)
            else:
                st.warning("Please select at least one bear to display data.")
        
        # Home Range Data Tab
        with data_tabs[1]:
            st.subheader("Bear Home Range Data")
            
            if filtered_bears:
                # Get home range data based on filters
                home_range_data = pd.DataFrame()
                
                for bear_id in filtered_bears:
                    bear_hr = db.get_home_range_data(bear_id=bear_id)
                    if not bear_hr.empty:
                        home_range_data = pd.concat([home_range_data, bear_hr])
                
                if home_range_data.empty:
                    st.warning("No home range data available for the selected bears.")
                else:
                    # Display data overview
                    st.write(f"**Total records:** {len(home_range_data)}")
                    
                    # Add download button
                    csv = home_range_data.to_csv(index=False)
                    st.download_button(
                        label="Download Data as CSV",
                        data=csv,
                        file_name="bear_home_range_data.csv",
                        mime="text/csv"
                    )
                    
                    # Display data table
                    st.dataframe(home_range_data)
            else:
                st.warning("Please select at least one bear to display data.")
        
        # Daily Movement Tab
        with data_tabs[2]:
            st.subheader("Bear Daily Movement Data")
            
            if filtered_bears:
                # Get daily movement data based on filters
                daily_data = pd.DataFrame()
                
                for bear_id in filtered_bears:
                    bear_daily = db.get_daily_movement(
                        bear_id=bear_id,
                        start_date=start_datetime,
                        end_date=end_datetime
                    )
                    
                    if not bear_daily.empty:
                        daily_data = pd.concat([daily_data, bear_daily])
                
                if daily_data.empty:
                    st.warning("No daily movement data available for the selected bears.")
                else:
                    # Display data overview
                    st.write(f"**Total records:** {len(daily_data)}")
                    
                    if 'date' in daily_data.columns:
                        st.write(f"**Date range:** {daily_data['date'].min()} to {daily_data['date'].max()}")
                    
                    # Add download button
                    csv = daily_data.to_csv(index=False)
                    st.download_button(
                        label="Download Data as CSV",
                        data=csv,
                        file_name="bear_daily_movement_data.csv",
                        mime="text/csv"
                    )
                    
                    # Display data table
                    st.dataframe(daily_data)
            else:
                st.warning("Please select at least one bear to display data.")
        
        # Statistics Tab
        with data_tabs[3]:
            st.subheader("Bear Movement Statistics")
            
            if filtered_bears:
                # Calculate statistics based on tracking data
                if 'tracking_data' in locals() and not tracking_data.empty:
                    # Group by bear
                    bear_stats = tracking_data.groupby('bear_id').agg({
                        'timestamp': ['min', 'max', 'count'],
                        'sex': 'first',
                        'age': 'first'
                    }).reset_index()
                    
                    # Rename columns
                    bear_stats.columns = ['_'.join(col).strip() for col in bear_stats.columns.values]
                    bear_stats.rename(columns={
                        'bear_id_': 'bear_id',
                        'timestamp_min': 'first_observed',
                        'timestamp_max': 'last_observed',
                        'timestamp_count': 'observations',
                        'sex_first': 'sex',
                        'age_first': 'age'
                    }, inplace=True)
                    
                    # Calculate study duration in days
                    bear_stats['study_days'] = (bear_stats['last_observed'] - bear_stats['first_observed']).dt.days
                    
                    # Calculate observations per day
                    bear_stats['obs_per_day'] = bear_stats['observations'] / bear_stats['study_days']
                    
                    # Display statistics table
                    st.dataframe(bear_stats)
                    
                    # Summary statistics by sex and age
                    st.subheader("Summary Statistics")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Statistics by sex
                        sex_stats = tracking_data.groupby('sex').agg({
                            'bear_id': 'nunique',
                            'timestamp': 'count'
                        }).reset_index()
                        
                        sex_stats.columns = ['Sex', 'Bears', 'Observations']
                        
                        # Create pie chart
                        fig1 = px.pie(
                            sex_stats,
                            values='Bears',
                            names='Sex',
                            title='Bears by Sex'
                        )
                        st.plotly_chart(fig1, use_container_width=True)
                    
                    with col2:
                        # Statistics by age
                        age_stats = tracking_data.groupby('age').agg({
                            'bear_id': 'nunique',
                            'timestamp': 'count'
                        }).reset_index()
                        
                        age_stats.columns = ['Age', 'Bears', 'Observations']
                        
                        # Map age codes to descriptions
                        age_stats['Age'] = age_stats['Age'].map(age_descriptions).fillna(age_stats['Age'])
                        
                        # Create pie chart
                        fig2 = px.pie(
                            age_stats,
                            values='Bears',
                            names='Age',
                            title='Bears by Age Class'
                        )
                        st.plotly_chart(fig2, use_container_width=True)
                    
                    # Seasonal distribution
                    st.subheader("Seasonal Distribution")
                    
                    # Statistics by season
                    season_stats = tracking_data.groupby('season').agg({
                        'bear_id': 'nunique',
                        'timestamp': 'count'
                    }).reset_index()
                    
                    season_stats.columns = ['Season', 'Bears', 'Observations']
                    
                    # Create bar chart
                    fig3 = px.bar(
                        season_stats,
                        x='Season',
                        y='Observations',
                        color='Bears',
                        title='Observations by Season'
                    )
                    st.plotly_chart(fig3, use_container_width=True)
                else:
                    st.warning("No tracking data available for statistical analysis.")
            else:
                st.warning("Please select at least one bear to display statistics.")
    
    # -- About Tab --
    with tabs[4]:
        st.header("About the Carpathian Bears Dataset")
        
        st.markdown("""
        ## Data Description
        
        This visualization system analyzes movement patterns of brown bears in the Carpathian Mountains. The data includes:
        
        1. **Bear Tracking Data**: GPS locations of bears over time
        2. **Home Range Data**: MCP95%, KDE and core area (KDE50%) calculations
        3. **Daily Movement**: Distance traveled and altitude data
        4. **Seasonal Patterns**: Movement patterns across different seasons
        
        ## Data Sources
        
        The data is stored in the following files:
        
        - `1_bears_RO.csv`: Main tracking data
        - `2_bears_RO_seasons.csv`: Additional seasonal tracking data
        - `3_mcphr_bears_1.csv`: MCP95% home range data
        - `4_core_area_bears_RO.csv`: Core area (KDE50%) data
        - `5_HR_kernels_bears_1.csv`: KDE home range data
        - `6_mcphrs_all_seasons.csv`: Seasonal MCP data
        - `7_dist_bears_RO.csv`: Daily displacement data
        
        ## Coordinate System
        
        Original coordinates are in EPSG:3844 (Romanian national projection) and have been transformed to WGS84 for map visualization.
        
        ## Glossary
        
        - **MCP**: Minimum Convex Polygon - A method for estimating home range size
        - **KDE**: Kernel Density Estimation - A probabilistic method for estimating home range
        - **Core Area**: Typically the 50% KDE, representing the most intensively used area
        - **GMU**: Game Management Unit
        - **Age Classes**: 
          - A: Adult
          - J: Subadult
        - **Seasons**: 
          - Winter sleep
          - Den exit and reproduction
          - Forest fruits
          - Hyperphagia
        """)
        
        # Display a sample of each dataset
        st.subheader("Data Samples")
        
        data_files = [
            ("Bear Tracking", "bears_tracking"),
            ("MCP Home Range", "bears_mcp"),
            ("Core Area", "bears_core_area"),
            ("KDE Home Range", "bears_kde"),
            ("Seasonal MCP", "bears_seasonal_mcp"),
            ("Daily Displacement", "bears_daily_displacement")
        ]
        
        for name, table in data_files:
            with st.expander(f"{name} Sample"):
                # Query a sample of each table
                conn = None
                try:
                    conn = sqlite3.connect(db.db_path)  
                    query = f"SELECT * FROM {table} LIMIT 5"
                    sample_data = pd.read_sql(query, conn)
                    st.dataframe(sample_data)
                except:
                    st.write(f"No data available for {name}")
                finally:
                    if isinstance(conn, db.db_path.__class__) and conn: 
                        conn.close()

def create_movement_map(data, show_labels=True, line_width=3, show_points=True, point_size=5):
    """Create a movement map with bear paths and points"""
    # Create a base map centered at the mean lat/lng
    center_lat = data['lat'].mean()
    center_lng = data['lng'].mean()
    
    m = folium.Map(location=[center_lat, center_lng], zoom_start=10)
    
    # Add tile layers WITH ATTRIBUTION
    folium.TileLayer('openstreetmap', attr='© OpenStreetMap contributors').add_to(m)
    folium.TileLayer('Stamen Terrain', attr='Map tiles by Stamen Design, under CC BY 3.0').add_to(m)
    folium.TileLayer('Stamen Toner', attr='Map tiles by Stamen Design, under CC BY 3.0').add_to(m)
    folium.TileLayer('CartoDB positron', attr='© CartoDB and OpenStreetMap contributors').add_to(m)
    
    # Add layer control
    folium.LayerControl().add_to(m)
    
    # Create a color map for bears
    bears = data['bear_id'].unique()
    colors = ['red', 'blue', 'green', 'purple', 'orange', 'brown', 'pink', 'gray', 'black']
    bear_colors = {bear: colors[i % len(colors)] for i, bear in enumerate(bears)}
    
    # Group data by bear
    for bear_id, bear_data in data.groupby('bear_id'):
        color = bear_colors.get(bear_id, 'blue')
        
        # Sort by timestamp to ensure correct path
        bear_data = bear_data.sort_values('timestamp')
        
        # Add path (lines)
        points = bear_data[['lat', 'lng']].values.tolist()
        if len(points) > 1:
            folium.PolyLine(
                points,
                color=color,
                weight=line_width,
                opacity=0.7,
                dash_array='5, 10' if bear_data['sex'].iloc[0] == 'F' else None,  # Dashed line for females
                tooltip=f"Bear {bear_id}"
            ).add_to(m)
        
        # Add points
        if show_points:
            for _, row in bear_data.iterrows():
                # Create popup content
                popup_content = f"""
                <b>Bear ID:</b> {row['bear_id']}<br>
                <b>Sex:</b> {row['sex']}<br>
                <b>Age:</b> {row['age']}<br>
                <b>Season:</b> {row['season']}<br>
                <b>Time:</b> {row['timestamp']}
                """
                
                folium.CircleMarker(
                    location=[row['lat'], row['lng']],
                    radius=point_size,
                    color=color,
                    fill=True,
                    fill_color=color,
                    fill_opacity=0.7,
                    popup=folium.Popup(popup_content, max_width=300)
                ).add_to(m)
        
        # Add bear labels
        if show_labels and len(bear_data) > 0:
            # Use the median point for the label to avoid outliers
            median_idx = len(bear_data) // 2
            label_point = bear_data.iloc[median_idx]
            
            folium.Marker(
                location=[label_point['lat'], label_point['lng']],
                icon=folium.DivIcon(
                    icon_size=(150, 36),
                    icon_anchor=(75, 18),
                    html=f'<div style="font-size: 12pt; color: {color}; font-weight: bold;">{bear_id}</div>'
                )
            ).add_to(m)
    
    # Display the map
    folium_static(m, width=800, height=600)

def create_heatmap(data, show_labels=True, heat_intensity=10, heat_radius=15):
    """Create a heatmap of bear locations"""
    # Create a base map centered at the mean lat/lng
    center_lat = data['lat'].mean()
    center_lng = data['lng'].mean()
    
    m = folium.Map(location=[center_lat, center_lng], zoom_start=10)
    
    # Add tile layers WITH ATTRIBUTION
    folium.TileLayer('openstreetmap', attr='© OpenStreetMap contributors').add_to(m)
    folium.TileLayer('Stamen Terrain', attr='Map tiles by Stamen Design, under CC BY 3.0').add_to(m)
    folium.TileLayer('Stamen Toner', attr='Map tiles by Stamen Design, under CC BY 3.0').add_to(m)
    folium.TileLayer('CartoDB positron', attr='© CartoDB and OpenStreetMap contributors').add_to(m)
    
    # Add layer control
    folium.LayerControl().add_to(m)
    
    # Group data by bear
    for bear_id, bear_data in data.groupby('bear_id'):
        # Create a separate heatmap for each bear
        heat_data = bear_data[['lat', 'lng']].values.tolist()
        
        try:
            from folium.plugins import HeatMap
            HeatMap(
                heat_data,
                name=f"Bear {bear_id} Heatmap",
                radius=heat_radius,
                max_zoom=13,
                gradient={0.2: 'blue', 0.5: 'lime', 0.8: 'orange', 1.0: 'red'},
                min_opacity=0.5,
                blur=15,
                max_val=heat_intensity
            ).add_to(m)
        except ImportError:
            # Fallback if HeatMap is not available
            st.warning("HeatMap plugin not available. Please install folium.plugins.")
            for point in heat_data:
                folium.CircleMarker(
                    location=point,
                    radius=5,
                    color='blue',
                    fill=True,
                    fill_opacity=0.5
                ).add_to(m)
        
        # Add bear labels
        if show_labels and len(bear_data) > 0:
            # Use the median point for the label to avoid outliers
            median_idx = len(bear_data) // 2
            label_point = bear_data.iloc[median_idx]
            
            folium.Marker(
                location=[label_point['lat'], label_point['lng']],
                icon=folium.DivIcon(
                    icon_size=(150, 36),
                    icon_anchor=(75, 18),
                    html=f'<div style="font-size: 12pt; font-weight: bold;">{bear_id}</div>'
                )
            ).add_to(m)
    
    # Display the map
    folium_static(m, width=800, height=600)

def create_daily_positions_map(data, show_labels=True, point_size=8, color_by='Bear'):
    """Create a map showing daily positions of bears"""
    # Create a base map centered at the mean lat/lng
    center_lat = data['lat'].mean()
    center_lng = data['lng'].mean()
    
    m = folium.Map(location=[center_lat, center_lng], zoom_start=10)
    
    # Add tile layers WITH ATTRIBUTION
    folium.TileLayer('openstreetmap', attr='© OpenStreetMap contributors').add_to(m)
    folium.TileLayer('Stamen Terrain', attr='Map tiles by Stamen Design, under CC BY 3.0').add_to(m)
    folium.TileLayer('Stamen Toner', attr='Map tiles by Stamen Design, under CC BY 3.0').add_to(m)
    folium.TileLayer('CartoDB positron', attr='© CartoDB and OpenStreetMap contributors').add_to(m)
    
    # Add layer control
    folium.LayerControl().add_to(m)
    
    # Create color maps
    bears = data['bear_id'].unique()
    bear_colors = {bear: f'#{hash(bear) % 0xffffff:06x}' for bear in bears}
    
    sex_colors = {'M': 'blue', 'F': 'red'}
    age_colors = {'A': 'green', 'J': 'orange'}
    season_colors = {
        'Winter sleep': 'darkblue',
        'Den exit and reproduction': 'green',
        'Forest fruits': 'orange',
        'Hyperphagia': 'brown'
    }
    
    # Group data by bear and day
    data['day'] = data['timestamp'].dt.date
    
    for (bear_id, day), day_data in data.groupby(['bear_id', 'day']):
        # Choose color based on selection
        if color_by == 'Bear':
            color = bear_colors.get(bear_id, 'blue')
        elif color_by == 'Sex':
            color = sex_colors.get(day_data['sex'].iloc[0], 'gray')
        elif color_by == 'Age':
            color = age_colors.get(day_data['age'].iloc[0], 'gray')
        else:  # Season
            color = season_colors.get(day_data['season'].iloc[0], 'gray')
        
        # Get the centroid of the day's points
        day_lat = day_data['lat'].mean()
        day_lng = day_data['lng'].mean()
        
        # Create popup content
        popup_content = f"""
        <b>Bear ID:</b> {bear_id}<br>
        <b>Date:</b> {day}<br>
        <b>Sex:</b> {day_data['sex'].iloc[0]}<br>
        <b>Age:</b> {day_data['age'].iloc[0]}<br>
        <b>Season:</b> {day_data['season'].iloc[0]}<br>
        <b>Points:</b> {len(day_data)}
        """
        
        # Add the day point
        folium.CircleMarker(
            location=[day_lat, day_lng],
            radius=point_size,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7,
            popup=folium.Popup(popup_content, max_width=300)
        ).add_to(m)
    
    # Add bear labels
    if show_labels:
        for bear_id, bear_data in data.groupby('bear_id'):
            # Use the median point for the label to avoid outliers
            median_idx = len(bear_data) // 2
            label_point = bear_data.iloc[median_idx]
            
            color = bear_colors.get(bear_id, 'blue')
            
            folium.Marker(
                location=[label_point['lat'], label_point['lng']],
                icon=folium.DivIcon(
                    icon_size=(150, 36),
                    icon_anchor=(75, 18),
                    html=f'<div style="font-size: 12pt; color: {color}; font-weight: bold;">{bear_id}</div>'
                )
            ).add_to(m)
    
    # Display the map
    folium_static(m, width=800, height=600)

# Run this if imported from map.py
if __name__ == "__main__":
    display_carpathian_bears_section()