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
def get_image_files(directory_path):
    """Cache the listing of image files in a directory"""
    if not os.path.exists(directory_path):
        return []
    
    return [f for f in os.listdir(directory_path) 
            if f.lower().endswith(('.png', '.jpg', '.jpeg'))]

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

# Function for displaying images in the sidebar
def display_device_images_in_sidebar(deterrent_data):
    """Display images for a selected device in the sidebar"""
    st.sidebar.header("View Deterrent Images")
    
    # Get device IDs for selection
    device_ids = []
    for _, row in deterrent_data.iterrows():
        raw_id = row['Directory name']
        formatted_id = format_device_id(raw_id)
        device_ids.append(formatted_id)
    
    # Add a select box to choose a device
    selected_device = st.sidebar.selectbox("Select Deterrent ID", options=device_ids)
    
    if selected_device:
        # Device images
        device_path = f"data/bear_pictures/{selected_device}/device/"
        device_images = get_image_files(device_path)
        
        # Trail images
        trail_path = f"data/bear_pictures/{selected_device}/trail_processed/"
        trail_images = get_image_files(trail_path)
        
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
                for img in device_images:
                    img_path = os.path.join(device_path, img)
                    st.sidebar.image(img_path, caption=img, use_container_width=True)
            else:
                st.sidebar.info("No device images found")
        
        # Display trail images if that button was clicked
        if st.session_state.get("load_trail", False):
            st.sidebar.subheader("Trail Images")
            if trail_images:
                st.sidebar.write(f"Found {len(trail_images)} trail images")
                for img in trail_images:
                    img_path = os.path.join(trail_path, img)
                    st.sidebar.image(img_path, caption=img, use_container_width=True)
            else:
                st.sidebar.info("No trail images found")

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
    
    # Add existing deterrent devices with simple popups
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
        
        # Create a simple popup with counts and ID info
        popup_html = f"""
        <div style="min-width: 200px; padding: 10px;">
            <h3>Deterrent ID: {device_id}</h3>
            <p>Location: {row['lat']:.6f}, {row['lng']:.6f}</p>
            <p>Available images:</p>
            <ul>
                <li>Device Images: {device_image_count}</li>
                <li>Trail Images: {trail_image_count}</li>
            </ul>
            <p><strong>Click "View Images" in the sidebar to see images for this device.</strong></p>
        </div>
        """
        
        # Create the popup with the HTML content
        iframe = folium.IFrame(html=popup_html, width=250, height=200)
        popup = folium.Popup(iframe, max_width=300)
        
        # Add marker with custom popup
        folium.Marker(
            [float(row['lat']), float(row['lng'])],
            popup=popup,
            tooltip=f"Deterrent ID: {device_id}",
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
    # Display the map with a hint about layer selection
    st.subheader("Deterrent Devices Map")
    st.write("Use the marker tool (in the top-left) to place markers, then click 'Save New Markers' to save them.")
    st.write("Each blue marker shows its ID number for easy identification.")
    st.info("👉 Look for the layer control in the top-right corner of the map to switch between different Japanese maps.")

    # Create the map
    m = create_map()
    map_data = st_folium(m, width=1000, height=500, key="folium_map")
    
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

with tab2:
    # Areas/Polygons tab
    st.subheader("Manage Areas")
    st.write("Draw polygons on the map to define areas, then save them. These areas can represent regions of interest or exclusion zones.")
    
    # Load current polygons
    current_polygons = load_polygon_data()
    
    # Add button for deleting all polygons
    if st.button("Delete All Areas", key="delete_all_polygons", use_container_width=True):
        # Confirm deletion
        if 'confirm_delete_all_polygons' not in st.session_state:
            st.session_state.confirm_delete_all_polygons = True
            st.warning("Are you sure you want to delete ALL areas? Click the button again to confirm.")
        else:
            # User confirmed, delete all polygons
            if delete_all_polygons():
                st.success("All areas have been deleted.")
                # Reset confirmation state
                st.session_state.confirm_delete_all_polygons = False
                # Rerun to update the map
                st.rerun()
            else:
                st.error("Error deleting areas.")
    
    # Display existing polygons
    if not current_polygons.empty:
        st.write(f"{len(current_polygons)} areas found:")
        
        # Create multiple columns for better layout
        col_id, col_name, col_time, col_action = st.columns([1, 2, 2, 1])
        
        # Headers
        with col_id:
            st.write("**ID**")
        with col_name:
            st.write("**Name**")
        with col_time:
            st.write("**Created**")
        with col_action:
            st.write("**Actions**")
        
        # Draw a separator line
        st.markdown("---")
        
        # Display each polygon with edit/delete options
        for index, row in current_polygons.iterrows():
            cols = st.columns([1, 2, 2, 1])
            
            with cols[0]:
                st.write(f"{row['polygon_id']}")
            with cols[1]:
                # Make name editable with proper label for accessibility
                new_name = st.text_input(
                    label=f"Area name for {row['polygon_id']}", 
                    value=row['name'], 
                    key=f"name_{row['polygon_id']}",
                    label_visibility="collapsed"  # Hide the label but keep it for accessibility
                )
                if new_name != row['name']:
                    if update_polygon_name(row['polygon_id'], new_name):
                        st.success(f"Name updated")
                        # Force page refresh
                        st.rerun()
            with cols[2]:
                st.write(f"{row['timestamp']}")
            with cols[3]:
                # Each row gets its own delete button
                if st.button("Delete", key=f"delete_poly_{row['polygon_id']}"):
                    if delete_polygon(row['polygon_id']):
                        st.success(f"Area {row['polygon_id']} deleted")
                        # Force page refresh to update the map
                        st.rerun()
                    else:
                        st.error(f"Failed to delete area {row['polygon_id']}")
        
        # Draw coordinates for all polygons
        st.subheader("Area Coordinates")
        for index, row in current_polygons.iterrows():
            with st.expander(f"Coordinates for {row['name']} ({row['polygon_id']})"):
                # Parse coordinates from JSON string if needed
                if isinstance(row['coordinates'], str):
                    coordinates = json.loads(row['coordinates'])
                else:
                    coordinates = row['coordinates']
                
                # Display in a nice table format
                coords_df = pd.DataFrame(coordinates, columns=['Longitude', 'Latitude'])
                coords_df.index = coords_df.index + 1  # Start from 1 instead of 0
                coords_df.index.name = 'Corner'
                st.dataframe(coords_df)
    else:
        st.info("No areas have been defined yet. Use the polygon tool on the map to draw areas.")

# Add export button for both markers and polygons
st.subheader("Export Data")
export_type = st.radio("Choose data to export:", ["Markers", "Areas", "Both"])

if st.button("Export Selected Data"):
    if export_type == "Markers" or export_type == "Both":
        export_markers_path = "exported_markers.csv"
        if os.path.exists(COORDINATES_CSV):
            pd.read_csv(COORDINATES_CSV).to_csv(export_markers_path, index=False)
            st.success(f"Marker data exported to {export_markers_path}")
            
            # Provide download link
            with open(export_markers_path, "rb") as file:
                st.download_button(
                    label="Download Markers CSV",
                    data=file,
                    file_name="exported_markers.csv",
                    mime="text/csv",
                    key="download_markers"
                )
        else:
            st.error("No marker data to export.")
            
    if export_type == "Areas" or export_type == "Both":
        export_polygons_path = "exported_areas.csv"
        if os.path.exists(POLYGON_COORDINATES_CSV):
            pd.read_csv(POLYGON_COORDINATES_CSV).to_csv(export_polygons_path, index=False)
            st.success(f"Area data exported to {export_polygons_path}")
            
            # Provide download link
            with open(export_polygons_path, "rb") as file:
                st.download_button(
                    label="Download Areas CSV",
                    data=file,
                    file_name="exported_areas.csv",
                    mime="text/csv",
                    key="download_areas"
                )
        else:
            st.error("No area data to export.")