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

# Path for Carpathian bears data
CARPATHIAN_DATA_DIR = "data/animal_data/carpathian_bears"

def load_carpathian_data():
    """
    Load the processed Carpathian bears data.
    This function assumes that data_preprocessing.py has already been run to create the necessary files.
    """
    # Check if required files exist
    processed_file = f"{CARPATHIAN_DATA_DIR}/processed/bears_processed.csv"
    if not os.path.exists(processed_file):
        st.error(f"Processed data file not found: {processed_file}")
        st.info("Please run the data_preprocessing.py script first to generate the required files.")
        return None, None, None, None
    
    try:
        # Load all relevant datasets
        bears_data = pd.read_csv(processed_file, parse_dates=['timestamp'])
        
        daily_data_file = f"{CARPATHIAN_DATA_DIR}/processed/bears_daily.csv"
        if os.path.exists(daily_data_file):
            daily_data = pd.read_csv(daily_data_file, parse_dates=['date'])
        else:
            daily_data = None
        
        hr_file = f"{CARPATHIAN_DATA_DIR}/processed/bears_home_ranges.csv"
        if os.path.exists(hr_file):
            hr_data = pd.read_csv(hr_file)
        else:
            hr_data = None
            
        distances_file = f"{CARPATHIAN_DATA_DIR}/processed/bears_distances.csv"
        if os.path.exists(distances_file):
            distances = pd.read_csv(distances_file, parse_dates=['date'])
        else:
            distances = None
            
        return bears_data, daily_data, hr_data, distances
    
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None, None, None, None

def project_to_wgs84(x, y):
    """
    Convert projected coordinates (likely EPSG:3857) to WGS84 (latitude/longitude).
    This is a simplified conversion for demonstration - in production you would use 
    a proper projection library with the correct parameters for the source projection.
    """
    # Simplified conversion centered around Romania's Carpathian Mountains
    # In a production system, replace this with proper projection code
    center_lat = 45.9
    center_lng = 25.3
    
    # Scale factors based on the data range
    x_range = 659186 - 397295
    y_range = 740429 - 415687
    
    # Scale to roughly a 2-degree area
    lng_scale = 2.0 / x_range
    lat_scale = 2.0 / y_range
    
    # Calculate offset from center
    x_offset = x - (397295 + x_range/2)
    y_offset = y - (415687 + y_range/2)
    
    # Convert to lat/lng (invert y because lat increases northward)
    lng = center_lng + x_offset * lng_scale
    lat = center_lat + y_offset * lat_scale
    
    return lat, lng

def create_carpathian_map(bears_data, filtered_data=None, show_by='bear', view_mode='points'):
    """
    Create a Folium map for Carpathian bears visualization.
    
    Args:
        bears_data: Complete bears dataset
        filtered_data: Filtered subset of data to display
        show_by: Color coding scheme ('bear', 'season', 'sex', 'age')
        view_mode: Visualization mode ('points', 'paths', 'heatmap', 'animation')
    
    Returns:
        Folium map object
    """
    # Set center of map to the middle of the data points
    if filtered_data is not None and not filtered_data.empty:
        data_to_use = filtered_data
    else:
        data_to_use = bears_data
    
    # Project a sample point to get the map center
    sample_point = data_to_use.iloc[0]
    center_lat, center_lng = project_to_wgs84(sample_point['X'], sample_point['Y'])
    
    # Create the map
    m = folium.Map(location=[center_lat, center_lng], zoom_start=9, control_scale=True)
    
    # Add different base maps with proper attribution
    folium.TileLayer(
        'OpenStreetMap',
        attr='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    ).add_to(m)
    
    folium.TileLayer(
        'Stamen Terrain',
        attr='Map tiles by <a href="http://stamen.com">Stamen Design</a>, under <a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0</a>. Data by <a href="http://openstreetmap.org">OpenStreetMap</a>, under <a href="http://www.openstreetmap.org/copyright">ODbL</a>.'
    ).add_to(m)
    
    folium.TileLayer(
        'Stamen Toner',
        attr='Map tiles by <a href="http://stamen.com">Stamen Design</a>, under <a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0</a>. Data by <a href="http://openstreetmap.org">OpenStreetMap</a>, under <a href="http://www.openstreetmap.org/copyright">ODbL</a>.'
    ).add_to(m)
    
    folium.TileLayer(
        'CartoDB positron',
        attr='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
    ).add_to(m)
    
    # Define color schemes
    bear_colors = {
        'Bear2': '#e6194B', 'Bear4': '#3cb44b', 'Bear5': '#ffe119', 
        'Bear6': '#4363d8', 'Bear8': '#f58231', 'Bear10': '#911eb4', 
        'Bear12': '#42d4f4', 'Bear13': '#f032e6', 'Bear14': '#bfef45', 
        'Bear15': '#fabebe', 'Bear16': '#469990', 'Bear17': '#e6beff', 
        'Bear18': '#9A6324'
    }
    
    season_colors = {
        'Den exit and reproduction': '#4CAF50',  # Green
        'Forest fruits': '#FFC107',             # Amber
        'Hyperphagia': '#FF5722',               # Deep Orange
        'Winter sleep': '#2196F3'               # Blue
    }
    
    sex_colors = {
        'M': '#2196F3',  # Blue for male
        'F': '#E91E63'   # Pink for female
    }
    
    age_colors = {
        'A': '#9C27B0',  # Purple for adult
        'J': '#00BCD4'   # Cyan for juvenile
    }
    
    # Function to get color based on selection
    def get_color(row):
        if show_by == 'bear':
            return bear_colors.get(row['Name'], '#000000')
        elif show_by == 'season':
            return season_colors.get(row['Season2'], '#000000')
        elif show_by == 'sex':
            return sex_colors.get(row['Sex'], '#000000')
        elif show_by == 'age':
            return age_colors.get(row['Age'], '#000000')
        return '#000000'  # Default black
    
    # Add data to map based on the view mode
    if view_mode == 'points':
        # Create a marker cluster to improve performance
        marker_cluster = MarkerCluster().add_to(m)
        
        # Add each data point
        for _, row in data_to_use.iterrows():
            lat, lng = project_to_wgs84(row['X'], row['Y'])
            
            # Create popup with details
            popup_html = f"""
            <div style="min-width: 180px; padding: 10px;">
                <h4>Bear: {row['Name']}</h4>
                <p><b>Time:</b> {row['timestamp']}</p>
                <p><b>Season:</b> {row['Season2']}</p>
                <p><b>Sex:</b> {"Male" if row['Sex'] == 'M' else "Female"}</p>
                <p><b>Age:</b> {"Adult" if row['Age'] == 'A' else "Juvenile"}</p>
            </div>
            """
            
            # Add marker with color based on selection
            color = get_color(row)
            folium.CircleMarker(
                location=[lat, lng],
                radius=5,
                popup=folium.Popup(popup_html, max_width=300),
                color='white',
                weight=1,
                fill=True,
                fill_color=color,
                fill_opacity=0.7,
                tooltip=f"{row['Name']} - {row['timestamp']}"
            ).add_to(marker_cluster)
    
    elif view_mode == 'paths':
        # Group by bear
        bear_groups = data_to_use.groupby('Name')
        
        for bear_name, group in bear_groups:
            # Sort by timestamp
            group = group.sort_values('timestamp')
            
            # Project coordinates
            coordinates = [project_to_wgs84(row['X'], row['Y']) for _, row in group.iterrows()]
            
            # Get color
            color = bear_colors.get(bear_name, '#000000')
            
            # Create popup with bear info
            first_row = group.iloc[0]
            popup_html = f"""
            <div>
                <h4>Bear: {bear_name}</h4>
                <p><b>Sex:</b> {"Male" if first_row['Sex'] == 'M' else "Female"}</p>
                <p><b>Age:</b> {"Adult" if first_row['Age'] == 'A' else "Juvenile"}</p>
                <p><b>Points:</b> {len(group)}</p>
                <p><b>Date Range:</b> {group['timestamp'].min().date()} to {group['timestamp'].max().date()}</p>
            </div>
            """
            
            # Add line
            folium.PolyLine(
                locations=coordinates,
                popup=folium.Popup(popup_html, max_width=300),
                color=color,
                weight=3,
                opacity=0.7,
                tooltip=f"Path of {bear_name}"
            ).add_to(m)
            
            # Add start and end markers
            folium.CircleMarker(
                location=coordinates[0],
                radius=8,
                color='white',
                weight=2,
                fill=True,
                fill_color=color,
                fill_opacity=0.9,
                tooltip=f"Start: {bear_name} - {group['timestamp'].min()}"
            ).add_to(m)
            
            folium.CircleMarker(
                location=coordinates[-1],
                radius=8,
                color='black',
                weight=2,
                fill=True,
                fill_color=color,
                fill_opacity=0.9,
                tooltip=f"End: {bear_name} - {group['timestamp'].max()}"
            ).add_to(m)
    
    elif view_mode == 'heatmap':
        # Project all coordinates
        heat_data = []
        for _, row in data_to_use.iterrows():
            lat, lng = project_to_wgs84(row['X'], row['Y'])
            heat_data.append([lat, lng, 1])  # weight of 1 for each point
        
        # Add heatmap
        HeatMap(heat_data, radius=15, blur=10, gradient={0.2: 'blue', 0.4: 'lime', 0.6: 'yellow', 1: 'red'}).add_to(m)
    
    elif view_mode == 'animation':
        # Create data for TimestampedGeoJson
        features = []
        
        for _, row in data_to_use.iterrows():
            lat, lng = project_to_wgs84(row['X'], row['Y'])
            time_str = row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            
            feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [lng, lat]  # GeoJSON is [lng, lat]
                },
                'properties': {
                    'time': time_str,
                    'popup': (
                        f"<b>Bear:</b> {row['Name']}<br>"
                        f"<b>Time:</b> {time_str}<br>"
                        f"<b>Season:</b> {row['Season2']}<br>"
                    ),
                    'style': {
                        'color': get_color(row),
                        'fillColor': get_color(row),
                        'fillOpacity': 0.7,
                        'weight': 1,
                        'radius': 8
                    },
                    'icon': 'circle',
                    'iconstyle': {
                        'fillColor': get_color(row),
                        'fillOpacity': 0.7,
                        'stroke': 'true',
                        'radius': 8
                    }
                }
            }
            
            features.append(feature)
        
        # Add TimestampedGeoJson layer
        TimestampedGeoJson(
            {
                'type': 'FeatureCollection',
                'features': features
            },
            period='PT1H',  # one hour
            duration='PT1M',  # transitions last one minute
            add_last_point=True,
            auto_play=False,
            loop=False,
            max_speed=10,
            loop_button=True,
            date_options='YYYY-MM-DD HH:mm:ss',
            time_slider_drag_update=True
        ).add_to(m)
    
    # Add legends
    if show_by == 'bear':
        legend_title = 'Bears'
        legend_items = [(name, color) for name, color in bear_colors.items()]
    elif show_by == 'season':
        legend_title = 'Seasons'
        legend_items = [(name, color) for name, color in season_colors.items()]
    elif show_by == 'sex':
        legend_title = 'Sex'
        legend_items = [('Male', sex_colors['M']), ('Female', sex_colors['F'])]
    elif show_by == 'age':
        legend_title = 'Age'
        legend_items = [('Adult', age_colors['A']), ('Juvenile', age_colors['J'])]
    
    # Add a legend to the map
    legend_html = f"""
    <div style="position: fixed; bottom: 50px; right: 50px; z-index: 1000; background-color: white; 
                padding: 10px; border: 2px solid grey; border-radius: 5px;">
        <p><b>{legend_title}</b></p>
        <div>
    """
    
    for name, color in legend_items:
        legend_html += f"""
        <div style="display: flex; align-items: center; margin-bottom: 5px;">
            <div style="background-color: {color}; width: 15px; height: 15px; margin-right: 5px;"></div>
            <div>{name}</div>
        </div>
        """
    
    legend_html += """
        </div>
    </div>
    """
    
    # Add the legend HTML to the map
    m.get_root().html.add_child(folium.Element(legend_html))
    
    # Add layer control
    folium.LayerControl().add_to(m)
    
    return m

def display_carpathian_filters_in_sidebar(bears_data):
    """
    Display filter controls for Carpathian bears in the sidebar.
    
    Args:
        bears_data: The complete bears dataset
        
    Returns:
        Tuple containing the filter values and date range limits
    """
    st.sidebar.header("Carpathian Bears Filters")
    
    # Extract unique values for filters
    bears = sorted(bears_data['Name'].unique())
    seasons = sorted(bears_data['Season2'].unique())
    sexes = sorted(bears_data['Sex'].unique())
    ages = sorted(bears_data['Age'].unique())
    
    # Earliest and latest dates
    min_date = bears_data['timestamp'].min().date()
    max_date = bears_data['timestamp'].max().date()
    
    # Create filter widgets - with no default selections
    with st.sidebar.expander("Bear & Date Filters", expanded=False):
        # Bear filter - no default selection
        selected_bears = st.multiselect("Select Bears", options=bears, default=None)
        
        # Date range filter
        st.write("Date Range")
        date_range = st.date_input(
            "Select date range",
            value=(min_date, max_date),  # Default to full range
            min_value=min_date, 
            max_value=max_date
        )
    
    with st.sidebar.expander("Attribute Filters", expanded=False):
        # Season filter - no default selection
        selected_seasons = st.multiselect("Select Seasons", options=seasons, default=None)
        
        # Sex filter - no default selection
        selected_sex = st.multiselect("Select Sex", options=sexes, default=None, 
                                    format_func=lambda x: "Male" if x == "M" else "Female")
        
        # Age filter - no default selection
        selected_age = st.multiselect("Select Age", options=ages, default=None,
                                    format_func=lambda x: "Adult" if x == "A" else "Juvenile")
        
        # Time of day filter
        time_of_day = st.radio(
            "Filter by time of day", 
            options=["All", "Day (6:00-18:00)", "Night (18:00-6:00)"],
            horizontal=True
        )
    
    with st.sidebar.expander("Visualization Options", expanded=True):
        # Visual encoding
        show_by = st.selectbox(
            "Color code by", 
            options=["bear", "season", "sex", "age"],
            format_func=lambda x: {
                "bear": "Bear ID", 
                "season": "Season", 
                "sex": "Sex", 
                "age": "Age"
            }.get(x, x)
        )
        
        # View mode
        view_mode = st.selectbox(
            "View mode", 
            options=["points", "paths", "heatmap", "animation"],
            format_func=lambda x: {
                "points": "Individual Points", 
                "paths": "Movement Paths", 
                "heatmap": "Density Heatmap",
                "animation": "Animated Timeline"
            }.get(x, x)
        )
    
    # Apply button to apply filters
    if st.sidebar.button("Apply Filters", use_container_width=True):
        st.session_state.carpathian_filters = (
            selected_bears, selected_seasons, selected_sex, selected_age,
            date_range, time_of_day, show_by, view_mode, min_date, max_date
        )
        st.rerun()
    
    # Reset button to clear filters
    if st.sidebar.button("Reset Filters", use_container_width=True):
        st.session_state.carpathian_filters = ([], [], [], [], (min_date, max_date), "All", "bear", "points", min_date, max_date)
        st.rerun()
    
    # Use session state if available, otherwise use current values
    if 'carpathian_filters' in st.session_state:
        return st.session_state.carpathian_filters
    else:
        return (selected_bears, selected_seasons, selected_sex, selected_age,
                date_range, time_of_day, show_by, view_mode, min_date, max_date)

def display_carpathian_bears_section():
    """
    Display the Carpathian bears visualization section in the Streamlit app.
    """
    st.title("Carpathian Bears Movement Visualization")
    st.write("""
    Explore the movement patterns of brown bears in the Carpathian Mountains. 
    This visualization allows you to analyze movement trajectories, home ranges, and seasonal patterns.
    """)
    
    # Show data preprocessing instructions
    preprocessing_expander = st.expander("Data Processing Instructions")
    with preprocessing_expander:
        st.write("""
        ### Data Preprocessing
        
        Before using this visualization for the first time, you need to run the data preprocessing script:
        
        ```python
        # Run either as a module or directly execute data_preprocessing.py
        from data_preprocessing import preprocess_carpathian_bears_data
        preprocess_carpathian_bears_data()
        ```
        
        This only needs to be done once unless you add new data.
        """)
    
    # Load the data
    bears_data, daily_data, hr_data, distances = load_carpathian_data()
    
    if bears_data is None:
        st.error("Could not load Carpathian bears data. Please run the data preprocessing script first.")
        st.info("You can run the data_preprocessing.py script to preprocess the data. See instructions above.")
        return
    
    # Display filters in sidebar
    filter_values = display_carpathian_filters_in_sidebar(bears_data)
    # Unpack all values, including min_date and max_date
    selected_bears, selected_seasons, selected_sex, selected_age, date_range, time_of_day, show_by, view_mode, min_date, max_date = filter_values
    
    # Apply filters
    filtered_data = bears_data.copy()
    
    # Filter by bear
    if selected_bears:
        filtered_data = filtered_data[filtered_data['Name'].isin(selected_bears)]
    
    # Filter by season
    if selected_seasons:
        filtered_data = filtered_data[filtered_data['Season2'].isin(selected_seasons)]
    
    # Filter by sex
    if selected_sex:
        filtered_data = filtered_data[filtered_data['Sex'].isin(selected_sex)]
    
    # Filter by age
    if selected_age:
        filtered_data = filtered_data[filtered_data['Age'].isin(selected_age)]
    
    # Filter by date
    if len(date_range) == 2:
        start_date, end_date = date_range
        filtered_data = filtered_data[
            (filtered_data['timestamp'].dt.date >= start_date) & 
            (filtered_data['timestamp'].dt.date <= end_date)
        ]
    
    # Filter by time of day
    if time_of_day == "Day (6:00-18:00)":
        filtered_data = filtered_data[filtered_data['timestamp'].dt.hour.between(6, 17)]
    elif time_of_day == "Night (18:00-6:00)":
        filtered_data = filtered_data[
            ~filtered_data['timestamp'].dt.hour.between(6, 17)
        ]
    
    # Create visualization tabs
    map_tab, stats_tab, charts_tab = st.tabs(["Map View", "Statistics", "Charts"])
    
    with map_tab:
        # Display map
        st.subheader("Bear Movement Map")
        
        # Display filter summary
        filter_summary = []
        if selected_bears:
            filter_summary.append(f"Bears: {', '.join(selected_bears)}")
        if selected_seasons:
            filter_summary.append(f"Seasons: {', '.join(selected_seasons)}")
        if selected_sex:
            sex_labels = {"M": "Male", "F": "Female"}
            filter_summary.append(f"Sex: {', '.join([sex_labels[s] for s in selected_sex])}")
        if selected_age:
            age_labels = {"A": "Adult", "J": "Juvenile"}
            filter_summary.append(f"Age: {', '.join([age_labels[a] for a in selected_age])}")
        if len(date_range) == 2 and date_range != (min_date, max_date):
            start_date, end_date = date_range
            filter_summary.append(f"Dates: {start_date} to {end_date}")
        if time_of_day != "All":
            filter_summary.append(f"Time: {time_of_day}")
            
        if filter_summary:
            st.info(f"Active filters: {' | '.join(filter_summary)}")
        else:
            st.info("No filters applied. Showing all data.")
        
        # Create and display the map
        if not filtered_data.empty:
            map_figure = create_carpathian_map(
                bears_data,
                filtered_data,
                show_by=show_by,
                view_mode=view_mode
            )
            st_folium(map_figure, width=1000, height=600)
        else:
            st.warning("No data found with the current filters. Please adjust your selection.")
    
    with stats_tab:
        # Display statistics about the filtered data
        st.subheader("Movement Statistics")
        
        if not filtered_data.empty:
            # Create multiple columns for key stats
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Points", f"{len(filtered_data):,}")
            
            with col2:
                num_days = (filtered_data['timestamp'].max() - filtered_data['timestamp'].min()).days
                st.metric("Time Span", f"{num_days} days")
            
            with col3:
                points_per_day = len(filtered_data) / max(1, num_days)
                st.metric("Points/Day", f"{points_per_day:.1f}")
            
            with col4:
                num_bears = filtered_data['Name'].nunique()
                st.metric("Bears", f"{num_bears}")
            
            # Show home range data if available
            if hr_data is not None and selected_bears:
                st.subheader("Home Range Statistics")
                
                # Filter home range data for selected bears
                filtered_hr = hr_data[hr_data['id'].isin(selected_bears)]
                
                if not filtered_hr.empty:
                    # Display in a table
                    st.dataframe(
                        filtered_hr[['id', 'area', 'Sex', 'Age', 'Stage']].rename(
                            columns={'id': 'Bear ID', 'area': 'Home Range (km²)'}
                        )
                    )
                else:
                    st.info("No home range data available for the selected bears.")
            
            # Show distance stats if available
            if distances is not None and selected_bears:
                st.subheader("Distance Statistics")
                
                # Filter distance data for selected bears
                filtered_dist = distances[distances['Name'].isin(selected_bears)]
                
                if not filtered_dist.empty:
                    # Group by bear and calculate stats
                    dist_stats = filtered_dist.groupby('Name')['dist'].agg(
                        ['count', 'mean', 'median', 'min', 'max']
                    ).reset_index()
                    
                    # Format the table
                    dist_stats = dist_stats.rename(
                        columns={
                            'Name': 'Bear ID', 
                            'count': 'Days', 
                            'mean': 'Avg Dist (m)',
                            'median': 'Median Dist (m)',
                            'min': 'Min Dist (m)',
                            'max': 'Max Dist (m)'
                        }
                    )
                    
                    # Round numeric columns
                    for col in ['Avg Dist (m)', 'Median Dist (m)', 'Min Dist (m)', 'Max Dist (m)']:
                        dist_stats[col] = dist_stats[col].round(1)
                    
                    st.dataframe(dist_stats)
                else:
                    st.info("No distance data available for the selected bears.")
        else:
            st.warning("No data found with the current filters. Please adjust your selection.")
    
    with charts_tab:
        # Display charts about the filtered data
        st.subheader("Movement Analysis Charts")
        
        if not filtered_data.empty:
            # Create chart selection
            chart_type = st.selectbox(
                "Select Chart", 
                options=[
                    "Activity by Hour", 
                    "Movement by Season",
                    "Activity Calendar Heatmap",
                    "Bear Comparison"
                ]
            )
            
            if chart_type == "Activity by Hour":
                # Group by hour of day and count
                hourly_activity = filtered_data.groupby(
                    filtered_data['timestamp'].dt.hour
                ).size().reset_index(name='count')
                
                # Create hourly activity chart
                fig = px.bar(
                    hourly_activity, 
                    x='timestamp', 
                    y='count', 
                    labels={'timestamp': 'Hour of Day', 'count': 'Number of Points'},
                    title="Activity Distribution by Hour of Day"
                )
                
                # Add day/night shading
                fig.add_vrect(
                    x0=6, x1=18,
                    fillcolor="yellow", opacity=0.1,
                    annotation_text="Daytime (6:00-18:00)",
                    annotation_position="top left"
                )
                
                fig.add_vrect(
                    x0=0, x1=6,
                    fillcolor="gray", opacity=0.1,
                    annotation_text="Nighttime",
                    annotation_position="top left"
                )
                
                fig.add_vrect(
                    x0=18, x1=24,
                    fillcolor="gray", opacity=0.1,
                    annotation=None
                )
                
                st.plotly_chart(fig, use_container_width=True)
            
            elif chart_type == "Movement by Season":
                if selected_bears and len(selected_bears) > 0:
                    # For each selected bear, show points by season
                    for bear in selected_bears:
                        bear_data = filtered_data[filtered_data['Name'] == bear]
                        
                        if not bear_data.empty:
                            # Count points by season
                            season_counts = bear_data.groupby('Season2').size().reset_index(name='count')
                            
                            # Create season distribution chart
                            fig = px.pie(
                                season_counts, 
                                names='Season2', 
                                values='count',
                                title=f"Season Distribution for {bear}",
                                color='Season2',
                                color_discrete_map={
                                    'Den exit and reproduction': '#4CAF50',
                                    'Forest fruits': '#FFC107',
                                    'Hyperphagia': '#FF5722',
                                    'Winter sleep': '#2196F3'
                                }
                            )
                            
                            st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Please select at least one bear to view seasonal movement patterns.")
            
            elif chart_type == "Activity Calendar Heatmap":
                if selected_bears and len(selected_bears) == 1:
                    bear = selected_bears[0]
                    bear_data = filtered_data[filtered_data['Name'] == bear]
                    
                    if not bear_data.empty:
                        # Group by date and count points
                        daily_activity = bear_data.groupby(
                            bear_data['timestamp'].dt.date
                        ).size().reset_index(name='count')
                        
                        # Create a complete date range
                        all_dates = pd.date_range(
                            daily_activity['timestamp'].min(),
                            daily_activity['timestamp'].max(),
                            freq='D'
                        )
                        
                        # Create a DataFrame with all dates
                        all_dates_df = pd.DataFrame({'date': all_dates})
                        all_dates_df['date'] = all_dates_df['date'].dt.date
                        
                        # Merge with actual activity
                        merged = all_dates_df.merge(
                            daily_activity, 
                            left_on='date', 
                            right_on='timestamp', 
                            how='left'
                        )
                        
                        # Fill NaN with 0
                        merged['count'] = merged['count'].fillna(0)
                        
                        # Extract month and day
                        merged['month'] = pd.to_datetime(merged['date']).dt.month
                        merged['day'] = pd.to_datetime(merged['date']).dt.day
                        merged['month_name'] = pd.to_datetime(merged['date']).dt.strftime('%b')
                        
                        # Create calendar heatmap
                        fig = px.density_heatmap(
                            merged,
                            x='day', 
                            y='month_name',
                            z='count',
                            title=f"Activity Calendar for {bear}",
                            labels={'day': 'Day of Month', 'month_name': 'Month', 'count': 'Activity (points)'}
                        )
                        
                        # Update y-axis to show months in correct order
                        fig.update_layout(
                            yaxis=dict(
                                categoryorder='array',
                                categoryarray=[
                                    'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                                    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
                                ]
                            )
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No data available for the selected bear.")
                else:
                    st.info("Please select exactly one bear to view the activity calendar.")
            
            elif chart_type == "Bear Comparison":
                if len(selected_bears) > 1:
                    # Create comparison of daily movement patterns
                    st.subheader("Daily Movement Distance Comparison")
                    
                    if distances is not None:
                        # Filter for selected bears
                        bear_distances = distances[distances['Name'].isin(selected_bears)]
                        
                        if not bear_distances.empty:
                            # Create box plot of distances
                            fig = px.box(
                                bear_distances,
                                x='Name',
                                y='dist',
                                color='Name',
                                labels={'Name': 'Bear ID', 'dist': 'Daily Distance (m)'},
                                title="Comparison of Daily Movement Distances"
                            )
                            
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # Add a statistical comparison
                            st.subheader("Statistical Summary")
                            
                            # Group by bear and calculate stats
                            stats_df = bear_distances.groupby('Name')['dist'].agg(
                                ['mean', 'median', 'std', 'min', 'max', 'count']
                            ).reset_index()
                            
                            # Format the stats
                            stats_df = stats_df.rename(
                                columns={
                                    'Name': 'Bear ID',
                                    'mean': 'Mean Distance (m)',
                                    'median': 'Median Distance (m)',
                                    'std': 'Std Dev (m)',
                                    'min': 'Min Distance (m)',
                                    'max': 'Max Distance (m)',
                                    'count': 'Days'
                                }
                            )
                            
                            # Round numeric columns
                            for col in stats_df.columns:
                                if col != 'Bear ID':
                                    stats_df[col] = stats_df[col].round(1)
                            
                            st.dataframe(stats_df)
                        else:
                            st.info("No distance data available for the selected bears.")
                    else:
                        st.info("Distance data is not available.")
                else:
                    st.info("Please select at least two bears for comparison.")
        else:
            st.warning("No data found with the current filters. Please adjust your selection.")

# This can be used for testing the component independently
if __name__ == "__main__":
    display_carpathian_bears_section()