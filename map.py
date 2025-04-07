import streamlit as st
import pandas as pd
import folium
from folium.plugins import Draw
from streamlit_folium import st_folium
import os
import datetime
import json
import uuid
import base64
from datetime import datetime as dt

# Path for saving coordinates of artificial devices
COORDINATES_CSV = "data/device_data/artificial_device_coordinates.csv"

# Path for saving marked areas of the streamlit folium polygon
POLYGON_COORDINATES_CSV = "data/areas/user_drawn_area_cities.csv"

# Ensure directories exist
os.makedirs(os.path.dirname(COORDINATES_CSV), exist_ok=True)
os.makedirs(os.path.dirname(POLYGON_COORDINATES_CSV), exist_ok=True)

# Set page configuration
st.set_page_config(page_title="Bear Deterrent Mapping System", layout="wide")

# Initialize the CSV files if they don't exist
if not os.path.exists(COORDINATES_CSV):
    pd.DataFrame(columns=["id", "timestamp", "lat", "lng"]).to_csv(COORDINATES_CSV, index=False)

if not os.path.exists(POLYGON_COORDINATES_CSV):
    pd.DataFrame(columns=["polygon_id", "timestamp", "name", "coordinates"]).to_csv(POLYGON_COORDINATES_CSV, index=False)

# Add a title
st.title("Interactive Bear Deterrent Mapping System")

# Image helper functions
@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_image_files(directory_path, start_date=None, end_date=None, include_unsuccessful=False, daily_time_filter=None):
    """
    Cache the listing of image files in a directory with datetime filtering
    
    Args:
        directory_path (str): Path to the directory containing images
        start_date (datetime, optional): Start date for filtering
        end_date (datetime, optional): End date for filtering
        include_unsuccessful (bool): Whether to include unsuccessful_parsing images
        daily_time_filter (tuple, optional): (start_hour, end_hour) for filtering by time of day
        
    Returns:
        list: List of image filenames that match the criteria
    """
    if not os.path.exists(directory_path):
        return []
    
    # Get all image files
    all_images = [f for f in os.listdir(directory_path) 
                 if f.lower().endswith(('.png', '.jpg', '.jpeg'))]
    
    # If no filtering is needed, return all images
    if start_date is None and end_date is None and daily_time_filter is None and include_unsuccessful:
        return all_images
    
    filtered_images = []
    for img in all_images:
        # Check if it's an unsuccessful parsing image
        is_unsuccessful = "unsuccessful_parsing" in img
        
        # Skip unsuccessful parsing images if not included
        if is_unsuccessful and not include_unsuccessful:
            continue
        
        # Always include unsuccessful images if they're requested
        if is_unsuccessful and include_unsuccessful:
            filtered_images.append(img)
            continue
            
        # For regular images, check date if filters are applied
        if start_date is not None or end_date is not None or daily_time_filter is not None:
            # Extract date from filename based on the pattern
            try:
                if "trail_processed" in directory_path:
                    # Format: yyyy.MM.dd.hhmm.jpeg
                    date_part = img.split('.jpeg')[0]  # Remove extension
                    img_date = dt.strptime(date_part, "%Y.%m.%d.%H%M")
                elif "device" in directory_path:
                    # Format: xxx_xxx_yyyy.mm.dd.hhmm_0.jpeg
                    date_parts = img.split('_')
                    if len(date_parts) >= 3:
                        date_str = date_parts[2]  # yyyy.mm.dd.hhmm
                        img_date = dt.strptime(date_str, "%Y.%m.%d.%H%M")
                    else:
                        # Can't parse date, skip this image
                        continue
                else:
                    # Unknown directory type, skip this image
                    continue
                
                # Check if the image passes all applicable filters
                passes_date_filter = True
                passes_daily_time_filter = True
                
                # Apply date range filter if provided
                if start_date is not None and end_date is not None:
                    passes_date_filter = img_date >= start_date and img_date <= end_date
                
                # Apply daily time filter if provided
                if daily_time_filter is not None:
                    daily_start_hour, daily_end_hour = daily_time_filter
                    img_hour = img_date.hour
                    
                    if daily_start_hour <= daily_end_hour:
                        # Normal time range (e.g., 9:00 to 17:00)
                        passes_daily_time_filter = daily_start_hour <= img_hour <= daily_end_hour
                    else:
                        # Overnight time range (e.g., 22:00 to 6:00)
                        passes_daily_time_filter = img_hour >= daily_start_hour or img_hour <= daily_end_hour
                
                # Add image if it passes all filters
                if passes_date_filter and passes_daily_time_filter:
                    filtered_images.append(img)
                    
            except (ValueError, IndexError):
                # If date parsing fails, skip this image
                continue
        else:
            # No date filters, include all regular images
            filtered_images.append(img)
    
    return filtered_images

@st.cache_data(ttl=3600)  # Cache for 1 hour
def format_device_id(raw_device_id):
    """Format device ID with consistent padding"""
    if isinstance(raw_device_id, (int, float)):
        # It's a number - format with leading zeros (6 digits)
        return f"{int(raw_device_id):06d}"
    else:
        # It's already a string
        device_id = str(raw_device_id)
        # If it's a numeric string but missing leading zeros, add them
        if device_id.isdigit() and len(device_id) < 6:
            return device_id.zfill(6)
        return device_id

# Function for displaying images in the sidebar with datetime filtering
def display_device_images_in_sidebar(deterrent_data):
    """Display images for a selected device in the sidebar with datetime filtering"""
    st.sidebar.header("View Deterrent Images")
    
    # Get device IDs for selection
    device_ids = []
    for _, row in deterrent_data.iterrows():
        raw_id = row['Directory name']
        formatted_id = format_device_id(raw_id)
        device_ids.append(formatted_id)
    
    # Set default index based on session state if available
    default_index = 0
    if "selected_deterrent" in st.session_state and st.session_state["selected_deterrent"] in device_ids:
        default_index = device_ids.index(st.session_state["selected_deterrent"])
        # Clear the session state selection after using it
        # st.session_state.pop("selected_deterrent")
    
    # Add a select box to choose a device
    selected_device = st.sidebar.selectbox(
        "Select Deterrent ID", 
        options=device_ids,
        index=default_index,
        key="deterrent_select",
        help="You can also use the 'Quick Select' buttons below the map"
    )
    
    if selected_device:
        # Device images path
        device_path = f"data/bear_pictures/{selected_device}/device/"
        # Trail images path
        trail_path = f"data/bear_pictures/{selected_device}/trail_processed/"
        
        # Date filter section
        st.sidebar.subheader("Date/Time Filter")
        
        # Year selection (start from 2024) - moved out of the if-else block so it's available to both modes
        min_year = 2024
        max_year = dt.now().year
        years = list(range(min_year, max_year + 1))
        
        # Filter mode selection
        filter_mode = st.sidebar.radio(
            "Filter Mode", 
            ["Date Range", "Daily Time Period"], 
            key="filter_mode"
        )
        
        if filter_mode == "Date Range":
            # Original date range filter
            # Create date filter inputs
            col1, col2 = st.sidebar.columns(2)
            
            with col1:
                st.write("Start Date:")
                start_year = st.selectbox("Year", years, key="start_year")
                start_month = st.selectbox("Month", range(1, 13), key="start_month")
                # Calculate max days for the selected month
                max_days = 31  # Default
                if start_month in [4, 6, 9, 11]:
                    max_days = 30
                elif start_month == 2:
                    # Simple leap year check
                    if start_year % 4 == 0 and (start_year % 100 != 0 or start_year % 400 == 0):
                        max_days = 29
                    else:
                        max_days = 28
                        
                start_day = st.selectbox("Day", range(1, max_days + 1), key="start_day")
                
            with col2:
                st.write("End Date:")
                end_year = st.selectbox("Year", years, key="end_year")
                end_month = st.selectbox("Month", range(1, 13), key="end_month")
                # Calculate max days for the selected month
                max_days = 31  # Default
                if end_month in [4, 6, 9, 11]:
                    max_days = 30
                elif end_month == 2:
                    # Simple leap year check
                    if end_year % 4 == 0 and (end_year % 100 != 0 or end_year % 400 == 0):
                        max_days = 29
                    else:
                        max_days = 28
                        
                end_day = st.selectbox("Day", range(1, max_days + 1), key="end_day")
            
            # Time filter
            st.sidebar.write("Time Range (24-hour format):")
            start_hour = st.sidebar.slider("Start Hour", 0, 23, 0, key="start_hour")
            end_hour = st.sidebar.slider("End Hour", 0, 23, 23, key="end_hour")
            
            # Create datetime objects for filtering
            try:
                start_date = dt(start_year, start_month, start_day, start_hour, 0)
                end_date = dt(end_year, end_month, end_day, end_hour, 59)
                
                # Validate date range
                if start_date > end_date:
                    st.sidebar.error("Start date cannot be after end date!")
                    use_filter = False
                else:
                    use_filter = True
                    st.sidebar.success(f"Filtering images from {start_date.strftime('%Y-%m-%d %H:%M')} to {end_date.strftime('%Y-%m-%d %H:%M')}")
            except ValueError:
                st.sidebar.error("Invalid date selected!")
                use_filter = False
        
        else:  # Daily Time Period mode
            st.sidebar.markdown("### Daily Time Period Filter")
            st.sidebar.write("Filter images for a specific time period across all dates")
            
            # Date range selection (more compact for this mode)
            col1, col2 = st.sidebar.columns(2)
            with col1:
                start_year = st.selectbox("From Year", years, key="period_start_year")
                start_month = st.selectbox("Month", range(1, 13), key="period_start_month")
            with col2:
                end_year = st.selectbox("To Year", years, key="period_end_year")
                end_month = st.selectbox("Month", range(1, 13), key="period_end_month")
            
            # Time period selection (the main focus of this mode)
            st.sidebar.markdown("#### Time of Day to Include:")
            daily_start_hour = st.sidebar.slider("Daily Start Hour", 0, 23, 21, key="daily_start_hour")
            daily_end_hour = st.sidebar.slider("Daily End Hour", 0, 23, 23, key="daily_end_hour")
            
            # For clearer feedback about the selected time range
            if daily_start_hour <= daily_end_hour:
                time_range_text = f"Every day from {daily_start_hour}:00 to {daily_end_hour}:59"
            else:
                time_range_text = f"Every night from {daily_start_hour}:00 to {daily_end_hour}:59 (overnight)"
            
            st.sidebar.info(time_range_text)
            
            # Set broad date range but will filter by time of day in image filtering function
            try:
                # First day of start month
                start_date = dt(start_year, start_month, 1, 0, 0)
                
                # Last day of end month
                if end_month == 12:
                    # December edge case
                    last_day = 31
                else:
                    # Use the first day of next month minus one day
                    next_month = end_month + 1
                    next_month_year = end_year
                    if next_month > 12:
                        next_month = 1
                        next_month_year += 1
                    last_day = (dt(next_month_year, next_month, 1) - datetime.timedelta(days=1)).day
                
                end_date = dt(end_year, end_month, last_day, 23, 59)
                
                # Store the daily hour range in session state
                st.session_state["daily_time_filter"] = (daily_start_hour, daily_end_hour)
                
                use_filter = True
                st.sidebar.success(f"Filtering images from {start_date.strftime('%Y-%m')} to {end_date.strftime('%Y-%m')} during {time_range_text}")
            except ValueError:
                st.sidebar.error("Invalid date selection!")
                use_filter = False
        
        # Option to include unsuccessful parsing images
        include_unsuccessful = st.sidebar.checkbox("Include 'unsuccessful_parsing' images", value=False)
        
        # Apply the filter or get all images
        if use_filter:
            if filter_mode == "Date Range":
                # Standard date range filtering
                device_images = get_image_files(device_path, start_date, end_date, include_unsuccessful)
                trail_images = get_image_files(trail_path, start_date, end_date, include_unsuccessful)
            else:
                # Daily time period filtering
                daily_time_filter = st.session_state.get("daily_time_filter")
                device_images = get_image_files(device_path, start_date, end_date, include_unsuccessful, daily_time_filter)
                trail_images = get_image_files(trail_path, start_date, end_date, include_unsuccessful, daily_time_filter)
        else:
            device_images = get_image_files(device_path, include_unsuccessful=include_unsuccessful)
            trail_images = get_image_files(trail_path, include_unsuccessful=include_unsuccessful)
        
        # Display reset button
        if st.sidebar.button("Reset Filters"):
            # Clear the session state for all filter values
            for key in ["start_year", "start_month", "start_day", "end_year", "end_month", "end_day", "start_hour", "end_hour"]:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()
        
        # Display counts and load buttons
        col1, col2 = st.sidebar.columns(2)
        
        with col1:
            st.button("Load Device Images", 
                      key="load_device", 
                      help=f"{len(device_images)} device images available",
                      use_container_width=True)
            
        with col2:
            st.button("Load Trail Images", 
                      key="load_trail", 
                      help=f"{len(trail_images)} trail images available",
                      use_container_width=True)
        
        # Display device images if that button was clicked
        if st.session_state.get("load_device", False):
            st.sidebar.subheader("Device Images")
            if device_images:
                st.sidebar.write(f"Found {len(device_images)} device images")
                
                # Add a toggle for image info display
                show_image_info = st.sidebar.checkbox("Show detailed image info", value=False, key="show_device_info")
                
                # Create pagination for better performance
                items_per_page = 5
                total_pages = (len(device_images) + items_per_page - 1) // items_per_page
                
                if total_pages > 1:
                    page = st.sidebar.number_input("Page", min_value=1, max_value=total_pages, value=1)
                    st.sidebar.write(f"Page {page} of {total_pages}")
                else:
                    page = 1
                
                # Calculate slice indices
                start_idx = (page - 1) * items_per_page
                end_idx = min(start_idx + items_per_page, len(device_images))
                
                # Display images with information
                for img in device_images[start_idx:end_idx]:
                    img_path = os.path.join(device_path, img)
                    
                    if show_image_info:
                        # Extract and display date information from filename
                        try:
                            # Format: xxx_xxx_yyyy.mm.dd.hhmm_0.jpeg
                            date_parts = img.split('_')
                            if len(date_parts) >= 3:
                                date_str = date_parts[2]  # yyyy.mm.dd.hhmm
                                img_date = dt.strptime(date_str, "%Y.%m.%d.%H%M")
                                date_info = img_date.strftime("%Y-%m-%d %H:%M")
                                caption = f"{img}\nDate: {date_info}"
                            else:
                                caption = img
                        except (ValueError, IndexError):
                            caption = img
                    else:
                        caption = img
                        
                    st.sidebar.image(img_path, caption=caption, use_container_width=True)
            else:
                st.sidebar.info("No device images found with the current filters")
        
        # Display trail images if that button was clicked
        if st.session_state.get("load_trail", False):
            st.sidebar.subheader("Trail Images")
            if trail_images:
                st.sidebar.write(f"Found {len(trail_images)} trail images")
                
                # Add a toggle for image info display
                show_image_info = st.sidebar.checkbox("Show detailed image info", value=False, key="show_trail_info")
                
                # Create pagination for better performance
                items_per_page = 5
                total_pages = (len(trail_images) + items_per_page - 1) // items_per_page
                
                if total_pages > 1:
                    page = st.sidebar.number_input("Page", min_value=1, max_value=total_pages, value=1, key="trail_page")
                    st.sidebar.write(f"Page {page} of {total_pages}")
                else:
                    page = 1
                
                # Calculate slice indices
                start_idx = (page - 1) * items_per_page
                end_idx = min(start_idx + items_per_page, len(trail_images))
                
                # Group images by parsing status
                successful_images = [img for img in trail_images[start_idx:end_idx] if "unsuccessful_parsing" not in img]
                unsuccessful_images = [img for img in trail_images[start_idx:end_idx] if "unsuccessful_parsing" in img]
                
                # Display successful images first
                if successful_images:
                    for img in successful_images:
                        img_path = os.path.join(trail_path, img)
                        
                        if show_image_info:
                            # Extract and display date information from filename
                            try:
                                # Format: yyyy.MM.dd.hhmm.jpeg
                                date_part = img.split('.jpeg')[0]  # Remove extension
                                img_date = dt.strptime(date_part, "%Y.%m.%d.%H%M")
                                date_info = img_date.strftime("%Y-%m-%d %H:%M")
                                caption = f"{img}\nDate: {date_info}"
                            except ValueError:
                                caption = img
                        else:
                            caption = img
                            
                        st.sidebar.image(img_path, caption=caption, use_container_width=True)
                
                # Display unsuccessful images with a warning style
                if unsuccessful_images:
                    st.sidebar.markdown("---")
                    st.sidebar.warning("Images with unsuccessful parsing:")
                    for img in unsuccessful_images:
                        img_path = os.path.join(trail_path, img)
                        st.sidebar.image(img_path, caption=img, use_container_width=True)
            else:
                st.sidebar.info("No trail images found with the current filters")

# Load all existing data
def load_existing_data():
    if os.path.exists(COORDINATES_CSV):
        return pd.read_csv(COORDINATES_CSV)
    return pd.DataFrame(columns=["id", "timestamp", "lat", "lng"])

# Load polygon data
def load_polygon_data():
    if os.path.exists(POLYGON_COORDINATES_CSV):
        df = pd.read_csv(POLYGON_COORDINATES_CSV)
        # Convert coordinates from string to list
        if not df.empty and 'coordinates' in df.columns:
            df['coordinates'] = df['coordinates'].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
        return df
    return pd.DataFrame(columns=["polygon_id", "timestamp", "name", "coordinates"])

# Load deterrent data
def load_deterrent_data():
    csv_path = "data/device_data/deterrent_devices.csv"
    if os.path.exists(csv_path):
        return pd.read_csv(csv_path)
    else:
        # Create empty DataFrame
        empty_df = pd.DataFrame(columns=["id", "lat", "lng"])
        empty_df.to_csv(csv_path, index=False)
        return empty_df

# Load data at startup
existing_markers = load_existing_data()
existing_polygons = load_polygon_data()
deterrent_data = load_deterrent_data()

# Get next available ID for markers
def get_next_id():
    if os.path.exists(COORDINATES_CSV):
        df = pd.read_csv(COORDINATES_CSV)
        if not df.empty and 'id' in df.columns:
            # Extract numeric IDs
            numeric_ids = []
            for id_val in df['id']:
                try:
                    numeric_ids.append(int(id_val))
                except (ValueError, TypeError):
                    pass
            if numeric_ids:
                return max(numeric_ids) + 1
    return 1

# Get next available ID for polygons
def get_next_polygon_id():
    if os.path.exists(POLYGON_COORDINATES_CSV):
        df = pd.read_csv(POLYGON_COORDINATES_CSV)
        if not df.empty and 'polygon_id' in df.columns:
            # Extract IDs and find max
            try:
                numeric_ids = [int(pid.split('-')[1]) for pid in df['polygon_id'] if isinstance(pid, str) and pid.startswith('poly-')]
                if numeric_ids:
                    return max(numeric_ids) + 1
            except (ValueError, IndexError):
                pass
    return 1

# Function to save coordinates from GeoJSON drawings
def save_coordinates_from_geojson(drawings):
    if not drawings:
        return [], []
    
    saved_points = []
    saved_polygons = []
    
    for drawing in drawings:
        if drawing['geometry']['type'] == 'Point':
            # Extract coordinates (GeoJSON format is [lng, lat])
            lng, lat = drawing['geometry']['coordinates']
            
            # Get next ID and format it
            next_id = get_next_id()
            marker_id = str(next_id).zfill(4)
            
            # Get timestamp
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Create new row
            new_data = {
                "id": marker_id,
                "timestamp": timestamp,
                "lat": lat,
                "lng": lng
            }
            
            # Load existing data, append new row, and save
            df = load_existing_data()
            new_row = pd.DataFrame([new_data])
            df = pd.concat([df, new_row], ignore_index=True)
            df.to_csv(COORDINATES_CSV, index=False)
            
            saved_points.append((lat, lng, marker_id))
            
        elif drawing['geometry']['type'] == 'Polygon':
            # Extract polygon coordinates (GeoJSON format is [[[lng, lat], [lng, lat], ...]])
            # Note: First and last point are the same in GeoJSON polygons
            coordinates = drawing['geometry']['coordinates'][0]
            
            # Get next polygon ID
            next_id = get_next_polygon_id()
            polygon_id = f"poly-{next_id}"
            
            # Get timestamp
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # We'll use a default name initially
            name = f"Area {next_id}"
            
            # Create new row for polygon data - store coordinates as a JSON string
            new_polygon_data = {
                "polygon_id": polygon_id,
                "timestamp": timestamp,
                "name": name,
                "coordinates": json.dumps(coordinates)
            }
            
            # Load existing polygon data, append new row, and save
            df = load_polygon_data()
            new_row = pd.DataFrame([new_polygon_data])
            df = pd.concat([df, new_row], ignore_index=True)
            df.to_csv(POLYGON_COORDINATES_CSV, index=False)
            
            saved_polygons.append((coordinates, polygon_id, name))
    
    return saved_points, saved_polygons

# Function to delete a single marker
def delete_marker(marker_id):
    if os.path.exists(COORDINATES_CSV):
        df = pd.read_csv(COORDINATES_CSV)
        # Filter out the marker with the given ID
        df = df[df['id'] != marker_id]
        df.to_csv(COORDINATES_CSV, index=False)
        return True
    return False

# Function to delete a single polygon
def delete_polygon(polygon_id):
    if os.path.exists(POLYGON_COORDINATES_CSV):
        df = pd.read_csv(POLYGON_COORDINATES_CSV)
        # Filter out the polygon with the given ID
        df = df[df['polygon_id'] != polygon_id]
        df.to_csv(POLYGON_COORDINATES_CSV, index=False)
        return True
    return False

# Function to delete all markers
def delete_all_markers():
    if os.path.exists(COORDINATES_CSV):
        # Create a new empty DataFrame with just the headers
        pd.DataFrame(columns=["id", "timestamp", "lat", "lng"]).to_csv(COORDINATES_CSV, index=False)
        return True
    return False

# Function to delete all polygons
def delete_all_polygons():
    if os.path.exists(POLYGON_COORDINATES_CSV):
        # Create a new empty DataFrame with just the headers
        pd.DataFrame(columns=["polygon_id", "timestamp", "name", "coordinates"]).to_csv(POLYGON_COORDINATES_CSV, index=False)
        return True
    return False

# Function to update polygon name
def update_polygon_name(polygon_id, new_name):
    if os.path.exists(POLYGON_COORDINATES_CSV):
        df = pd.read_csv(POLYGON_COORDINATES_CSV)
        if polygon_id in df['polygon_id'].values:
            df.loc[df['polygon_id'] == polygon_id, 'name'] = new_name
            df.to_csv(POLYGON_COORDINATES_CSV, index=False)
            return True
    return False

# Determine map center
if len(deterrent_data) > 0:
    first_point = deterrent_data.iloc[0]
    map_center = [first_point['lat'], first_point['lng']]
else:
    # Default center - Central Japan (adjusted for better view)
    map_center = [36.2048, 138.2529]

# Create map with IDs displayed on markers and polygons
def create_map():
    # Initialize map with OpenStreetMap as the default base layer
    m = folium.Map(location=map_center, zoom_start=10, control_scale=True)
    
    # Add Japanese map layers
    # 1. GSI Standard Map (set as default base map)
    gsi_standard = folium.TileLayer(
        tiles="https://cyberjapandata.gsi.go.jp/xyz/std/{z}/{x}/{y}.png",
        attr="GSI Japan",
        name="GSI Standard Map",
        overlay=False,
    )
    gsi_standard.add_to(m)
    
    # 2. GSI Terrain Map
    folium.TileLayer(
        tiles="https://cyberjapandata.gsi.go.jp/xyz/relief/{z}/{x}/{y}.png",
        attr="GSI Japan",
        name="GSI Terrain Map",
        overlay=False,
    ).add_to(m)
    
    # 3. GSI Satellite Imagery
    folium.TileLayer(
        tiles="https://cyberjapandata.gsi.go.jp/xyz/seamlessphoto/{z}/{x}/{y}.jpg",
        attr="GSI Japan",
        name="GSI Satellite Imagery",
        overlay=False,
    ).add_to(m)
    
    # 4. Japan Hazard Map
    folium.TileLayer(
        tiles="https://disaportaldata.gsi.go.jp/raster/01_flood_l2_shinsuishin_data/{z}/{x}/{y}.png",
        attr="GSI Japan Hazard Maps",
        name="Japan Flood Hazard Map",
        overlay=True,
        opacity=0.7,
    ).add_to(m)
    
    # 5. Japan Forest Map
    folium.TileLayer(
        tiles="https://cyberjapandata.gsi.go.jp/xyz/woodland/{z}/{x}/{y}.png",
        attr="GSI Japan",
        name="Japan Forest Map",
        overlay=True,
        opacity=0.7,
    ).add_to(m)
    
    # Add existing deterrent devices with streamlit buttons in expanders for selection
    for _, row in deterrent_data.iterrows():
        # Get the device ID
        raw_device_id = row['Directory name']
        device_id = format_device_id(raw_device_id)
        
        # Build paths to image directories
        device_images_path = f"data/bear_pictures/{device_id}/device/"
        trail_images_path = f"data/bear_pictures/{device_id}/trail_processed/"
        
        # Get image counts (not loading the actual images)
        device_image_count = len(get_image_files(device_images_path))
        trail_image_count = len(get_image_files(trail_images_path))
        
        # Create a simple popup with info
        popup_html = f"""
        <div style="min-width: 200px; padding: 10px;">
            <h3>Deterrent ID: {device_id}</h3>
            <p>Location: {row['lat']:.6f}, {row['lng']:.6f}</p>
            <p>Available images:</p>
            <ul>
                <li>Device Images: {device_image_count}</li>
                <li>Trail Images: {trail_image_count}</li>
            </ul>
            <p><strong>Click to view popup, then use 'Select this deterrent' button below.</strong></p>
        </div>
        """
        
        # Create the popup with the HTML content
        iframe = folium.IFrame(html=popup_html, width=250, height=220)
        popup = folium.Popup(iframe, max_width=300)
        
        # Add marker with custom popup
        folium.Marker(
            [float(row['lat']), float(row['lng'])],
            popup=popup,
            tooltip=f"Deterrent ID: {device_id} - Click for options",
            icon=folium.Icon(color="red", icon="warning-sign")
        ).add_to(m)
    
    # Add existing custom markers with IDs displayed
    existing_markers = load_existing_data()  # Fresh load
    for _, row in existing_markers.iterrows():
        marker_id = row['id']
        
        # Create a custom icon with the ID number displayed
        icon_html = f'''
            <div style="
                background-color: #3186cc;
                color: white;
                border-radius: 50%;
                text-align: center;
                line-height: 30px;
                width: 30px;
                height: 30px;
                font-weight: bold;
                font-size: 12px;
                box-shadow: 0 0 10px rgba(0,0,0,0.3);">
                {marker_id}
            </div>
        '''
        
        # Create icon
        icon = folium.DivIcon(html=icon_html)
        
        # Create popup with details
        popup_html = f"""
        <div>
            <b>ID:</b> {marker_id}<br>
            <b>Time:</b> {row['timestamp']}<br>
            <b>Location:</b> {row['lat']:.6f}, {row['lng']:.6f}<br>
        </div>
        """
        
        # Add marker with custom icon and popup
        folium.Marker(
            location=[float(row['lat']), float(row['lng'])],
            popup=folium.Popup(popup_html, max_width=300),
            icon=icon
        ).add_to(m)
    
    # Add existing polygons
    existing_polygons = load_polygon_data()  # Fresh load
    for _, row in existing_polygons.iterrows():
        polygon_id = row['polygon_id']
        polygon_name = row['name']
        
        # Parse coordinates from JSON string if needed
        if isinstance(row['coordinates'], str):
            coordinates = json.loads(row['coordinates'])
        else:
            coordinates = row['coordinates']
        
        # Convert coordinates from [lng, lat] to [lat, lng] for folium
        folium_coords = [[coord[1], coord[0]] for coord in coordinates]
        
        # Create popup with details
        popup_html = f"""
        <div>
            <b>ID:</b> {polygon_id}<br>
            <b>Name:</b> {polygon_name}<br>
            <b>Time:</b> {row['timestamp']}<br>
        </div>
        """
        
        # Add polygon to map
        folium.Polygon(
            locations=folium_coords,
            popup=folium.Popup(popup_html, max_width=300),
            color='green',
            fill=True,
            fill_color='green',
            fill_opacity=0.2,
            tooltip=f"Area: {polygon_name}"
        ).add_to(m)
    
    # Add Draw plugin with polygon enabled
    draw = Draw(
        position="topleft",
        draw_options={
            'polyline': False,
            'rectangle': True,  # Enable rectangle drawing as well
            'polygon': True,    # Enable polygon drawing
            'circle': False,
            'marker': True,
            'circlemarker': False,
        },
        edit_options={'edit': False}
    )
    draw.add_to(m)
    
    # Make sure to add the layer control AFTER all layers are added
    folium.LayerControl(position='topright', collapsed=False).add_to(m)
    
    return m

# Create tabs for different functionality
tab1, tab2 = st.tabs(["Markers", "Areas"])

with tab1:
    # Display the map
    st.subheader("Deterrent Devices Map")
    st.write("Use the marker tool (in the top-left) to place markers, then click 'Save New Markers' to save them.")
    st.write("Each blue marker shows its ID number for easy identification.")
    
    # Create the map
    m = create_map()
    map_data = st_folium(m, width=1000, height=500, key="folium_map")
    
    # Add section for quick selection of deterrents below the map
    st.subheader("Quick Select Deterrent")
    st.write("Click a deterrent on the map, then select it here to view its images:")
    
    # Create a grid of buttons for each deterrent
    cols = st.columns(5)  # 5 buttons per row
    
    for i, (_, row) in enumerate(deterrent_data.iterrows()):
        raw_device_id = row['Directory name']
        device_id = format_device_id(raw_device_id)
        
        # Determine which column to place this button in
        col_index = i % 5
        
        # Create a button in the appropriate column
        with cols[col_index]:
            if st.button(f"ID: {device_id}", key=f"select_{device_id}"):
                # Set the selected device in session state
                st.session_state["selected_deterrent"] = device_id
                st.rerun()  # Trigger a rerun to update the sidebar
    
    st.info("👉 Look for the layer control in the top-right corner of the map to switch between different Japanese maps.")
    
    # Display images in sidebar
    display_device_images_in_sidebar(deterrent_data)

    # Add buttons for saving and deleting all markers
    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("Save New Markers & Areas", key="save_markers", use_container_width=True):
            if map_data and 'all_drawings' in map_data and map_data['all_drawings']:
                # Get all drawings
                drawings = map_data['all_drawings']
                
                # Save all markers and polygons
                saved_points, saved_polygons = save_coordinates_from_geojson(drawings)
                
                # Show success message for markers
                if saved_points:
                    # Construct a nice message
                    if len(saved_points) == 1:
                        lat, lng, id = saved_points[0]
                        st.success(f"Marker added at {lat:.6f}, {lng:.6f} with ID: {id}")
                    else:
                        message = f"{len(saved_points)} markers saved:"
                        for lat, lng, id in saved_points:
                            message += f"\n• Location {lat:.6f}, {lng:.6f} with ID: {id}"
                        st.success(message)
                
                # Show success message for polygons
                if saved_polygons:
                    # Construct a nice message
                    if len(saved_polygons) == 1:
                        _, poly_id, name = saved_polygons[0]
                        st.success(f"Area '{name}' saved with ID: {poly_id}")
                    else:
                        message = f"{len(saved_polygons)} areas saved:"
                        for _, poly_id, name in saved_polygons:
                            message += f"\n• Area '{name}' with ID: {poly_id}"
                        st.success(message)
                
                if saved_points or saved_polygons:
                    # Rerun to update the map
                    st.rerun()
                else:
                    st.info("No new markers or areas to save. Use the drawing tools to place them first.")
            else:
                st.warning("No drawings found on the map. Place markers or draw areas using the drawing tools first.")

    with col2:
        if st.button("Delete All Markers", key="delete_all", use_container_width=True):
            # Confirm deletion
            if 'confirm_delete_all' not in st.session_state:
                st.session_state.confirm_delete_all = True
                st.warning("Are you sure you want to delete ALL markers? Click the button again to confirm.")
            else:
                # User confirmed, delete all markers
                if delete_all_markers():
                    st.success("All markers have been deleted.")
                    # Reset confirmation state
                    st.session_state.confirm_delete_all = False
                    # Rerun to update the map
                    st.rerun()
                else:
                    st.error("Error deleting markers.")

    # Create an interactive table with delete buttons
    st.subheader("Manage Markers")
    current_markers = load_existing_data()  # Fresh load

    if not current_markers.empty:
        # Add a counter for formatting
        row_count = len(current_markers)
        
        # Create multiple columns for better layout
        col_id, col_coords, col_time, col_delete = st.columns([1, 2, 2, 1])
        
        # Headers
        with col_id:
            st.write("**ID**")
        with col_coords:
            st.write("**Location**")
        with col_time:
            st.write("**Timestamp**")
        with col_delete:
            st.write("**Action**")
        
        # Draw a separator line
        st.markdown("---")
        
        # Display each marker in a row with a delete button
        for index, row in current_markers.iterrows():
            cols = st.columns([1, 2, 2, 1])
            
            with cols[0]:
                st.write(f"{row['id']}")
            with cols[1]:
                st.write(f"{row['lat']:.6f}, {row['lng']:.6f}")
            with cols[2]:
                st.write(f"{row['timestamp']}")
            with cols[3]:
                # Each row gets its own delete button
                if st.button("Delete", key=f"delete_{row['id']}"):
                    if delete_marker(row['id']):
                        st.success(f"Marker {row['id']} deleted")
                        # Force page refresh to update the map
                        st.rerun()
                    else:
                        st.error(f"Failed to delete marker {row['id']}")
    else:
        st.info("No custom markers have been added yet.")

    # Display existing deterrent devices
    st.subheader("Existing Deterrent Devices")
    st.dataframe(deterrent_data)