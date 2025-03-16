import streamlit as st
import pandas as pd
import folium
from folium.plugins import Draw
from streamlit_folium import st_folium
import os
import datetime

# Path for saving coordinates of artificial devices
COORDINATES_CSV = "data/device_data/artificial_device_coordinates.csv"

# Ensure directory exists
os.makedirs(os.path.dirname(COORDINATES_CSV), exist_ok=True)

# Set page configuration
st.set_page_config(page_title="Bear Deterrent Mapping System", layout="wide")

# Initialize the CSV file if it doesn't exist
if not os.path.exists(COORDINATES_CSV):
    pd.DataFrame(columns=["id", "timestamp", "lat", "lng"]).to_csv(COORDINATES_CSV, index=False)

# Add a title
st.title("Interactive Bear Deterrent Mapping System")

# Load all existing data
def load_existing_data():
    if os.path.exists(COORDINATES_CSV):
        return pd.read_csv(COORDINATES_CSV)
    return pd.DataFrame(columns=["id", "timestamp", "lat", "lng"])

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
deterrent_data = load_deterrent_data()

# Get next available ID
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

# Function to save coordinates from GeoJSON drawings
def save_coordinates_from_geojson(drawings):
    if not drawings:
        return []
    
    saved_points = []
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
    
    return saved_points

# Function to delete a single marker
def delete_marker(marker_id):
    if os.path.exists(COORDINATES_CSV):
        df = pd.read_csv(COORDINATES_CSV)
        # Filter out the marker with the given ID
        df = df[df['id'] != marker_id]
        df.to_csv(COORDINATES_CSV, index=False)
        return True
    return False

# Function to delete all markers
def delete_all_markers():
    if os.path.exists(COORDINATES_CSV):
        # Create a new empty DataFrame with just the headers
        pd.DataFrame(columns=["id", "timestamp", "lat", "lng"]).to_csv(COORDINATES_CSV, index=False)
        return True
    return False

# Determine map center
if len(deterrent_data) > 0:
    first_point = deterrent_data.iloc[0]
    map_center = [first_point['lat'], first_point['lng']]
else:
    # Default center
    map_center = [37.48, 139.96]

# Create map with IDs displayed on markers
def create_map():
    m = folium.Map(location=map_center, zoom_start=10)
    
    # Add existing deterrent devices
    for _, row in deterrent_data.iterrows():
        folium.Marker(
            [float(row['lat']), float(row['lng'])],
            popup=f"Deterrent ID: {row['id']}",
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
    
    # Add Draw plugin
    draw = Draw(
        position="topleft",
        draw_options={
            'polyline': False,
            'rectangle': False,
            'polygon': False,
            'circle': False,
            'marker': True,
            'circlemarker': False,
        },
        edit_options={'edit': False}
    )
    draw.add_to(m)
    
    return m

# Display the map
st.subheader("Deterrent Devices Map")
st.write("Use the marker tool (in the top-left) to place markers, then click 'Save New Markers' to save them.")
st.write("Each blue marker shows its ID number for easy identification.")

# Create the map
m = create_map()
map_data = st_folium(m, width=1000, height=500, key="folium_map")

# Add buttons for saving and deleting all markers
col1, col2 = st.columns([1, 1])
with col1:
    if st.button("Save New Markers", key="save_markers", use_container_width=True):
        if map_data and 'all_drawings' in map_data and map_data['all_drawings']:
            # Get all drawings
            drawings = map_data['all_drawings']
            
            # Save all markers
            saved_points = save_coordinates_from_geojson(drawings)
            
            # Show success message
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
                
                # Rerun to update the map with new markers
                st.rerun()
            else:
                st.info("No new markers to save. Use the marker tool to place markers first.")
        else:
            st.warning("No markers found on the map. Place markers using the drawing tool first.")

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

# Add export button
if st.button("Export Data"):
    export_path = "exported_data.csv"
    if os.path.exists(COORDINATES_CSV):
        pd.read_csv(COORDINATES_CSV).to_csv(export_path, index=False)
        st.success(f"Data exported to {export_path}")
        
        # Provide download link
        with open(export_path, "rb") as file:
            btn = st.download_button(
                label="Download CSV",
                data=file,
                file_name="exported_markers.csv",
                mime="text/csv"
            )
    else:
        st.error("No data to export.")