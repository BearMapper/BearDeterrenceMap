import streamlit as st
import pandas as pd
import folium
from folium.plugins import Draw
from streamlit_folium import st_folium
import os
import datetime
import json
from datetime import datetime as dt
from wildlife_db import WildlifeDatabase  # Import the database handler

class JapanDeterrentSystem:
    """
    Class to handle the Japan Bear Deterrent System visualization and functionality.
    Using SQLite database for data storage.
    """
    
    def __init__(self):
        """Initialize the Japan Deterrent System."""
        # Initialize the database connection
        self.db = WildlifeDatabase("wildlife_data.db")
        
        # Load data at startup
        self.existing_markers = self.load_existing_data()
        self.existing_polygons = self.load_polygon_data()
        self.deterrent_data = self.load_deterrent_data()
        
        # Determine map center
        if len(self.deterrent_data) > 0:
            first_point = self.deterrent_data.iloc[0]
            self.map_center = [first_point['lat'], first_point['lng']]
        else:
            # Default center - Central Japan (adjusted for better view)
            self.map_center = [36.2048, 138.2529]
    
    # Note the _self parameter to make it cacheable
    @st.cache_data(ttl=3600)  # Cache for 1 hour
    def get_image_files(_self, device_id, image_type="device", start_date=None, end_date=None, 
                         include_unsuccessful=False, daily_time_filter=None, limit=100, offset=0):
        """
        Get filtered images for a device from the database
        
        Args:
            device_id (str): Device ID to filter by
            image_type (str): "device" or "trail"
            start_date (datetime, optional): Start date for filtering
            end_date (datetime, optional): End date for filtering
            include_unsuccessful (bool): Whether to include unsuccessful_parsing images
            daily_time_filter (tuple, optional): (start_hour, end_hour) for filtering by time of day
            limit (int): Maximum number of results to return
            offset (int): Offset for pagination
            
        Returns:
            list: List of image paths and metadata that match the criteria
        """
        # Query database for matching images
        df = _self.db.get_images(
            device_id=device_id, 
            image_type=image_type,
            start_date=start_date,
            end_date=end_date,
            daily_time_filter=daily_time_filter,
            include_unsuccessful=include_unsuccessful,
            limit=limit,
            offset=offset
        )
        
        # Convert to list of dictionaries with image paths
        if df.empty:
            return []
            
        return df.to_dict('records')
    
    # Note the _self parameter to make it cacheable
    @st.cache_data(ttl=3600)  # Cache for 1 hour
    def format_device_id(_self, raw_device_id):
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

    def load_existing_data(self):
        """Load custom marker data from database"""
        return self.db.get_markers()

    def load_polygon_data(self):
        """Load polygon data from database"""
        return self.db.get_polygons()

    def load_deterrent_data(self):
        """Load deterrent device data from database"""
        return self.db.get_deterrent_devices()

    def get_next_id(self):
        """Get next available ID for markers"""
        return self.db.get_next_marker_id()

    def get_next_polygon_id(self):
        """Get next available ID for polygons"""
        next_id = self.db.get_next_polygon_id()
        return next_id

    def save_coordinates_from_geojson(self, drawings):
        """Save coordinates from GeoJSON drawings"""
        if not drawings:
            return [], []
        
        saved_points = []
        saved_polygons = []
        
        for drawing in drawings:
            if drawing['geometry']['type'] == 'Point':
                # Extract coordinates (GeoJSON format is [lng, lat])
                lng, lat = drawing['geometry']['coordinates']
                
                # Get next ID and format it
                next_id = self.get_next_id()
                marker_id = str(next_id).zfill(4)
                
                # Save to database
                self.db.save_marker(marker_id, lat, lng)
                
                saved_points.append((lat, lng, marker_id))
                
            elif drawing['geometry']['type'] == 'Polygon':
                # Extract polygon coordinates (GeoJSON format is [[[lng, lat], [lng, lat], ...]])
                # Note: First and last point are the same in GeoJSON polygons
                coordinates = drawing['geometry']['coordinates'][0]
                
                # Get next polygon ID
                next_id = self.get_next_polygon_id()
                polygon_id = f"poly-{next_id}"
                
                # We'll use a default name initially
                name = f"Area {next_id}"
                
                # Save to database
                self.db.save_polygon(polygon_id, name, coordinates)
                
                saved_polygons.append((coordinates, polygon_id, name))
        
        # Refresh the data
        self.existing_markers = self.load_existing_data()
        self.existing_polygons = self.load_polygon_data()
        
        return saved_points, saved_polygons

    def delete_marker(self, marker_id):
        """Delete a single marker by ID"""
        result = self.db.delete_marker(marker_id)
        if result:
            # Refresh the data
            self.existing_markers = self.load_existing_data()
        return result

    def delete_polygon(self, polygon_id):
        """Delete a single polygon by ID"""
        result = self.db.delete_polygon(polygon_id)
        if result:
            # Refresh the data
            self.existing_polygons = self.load_polygon_data()
        return result

    def delete_all_markers(self):
        """Delete all markers"""
        result = self.db.delete_all_markers()
        if result:
            # Refresh the data
            self.existing_markers = self.load_existing_data()
        return result

    def delete_all_polygons(self):
        """Delete all polygons"""
        result = self.db.delete_all_polygons()
        if result:
            # Refresh the data
            self.existing_polygons = self.load_polygon_data()
        return result

    def update_polygon_name(self, polygon_id, new_name):
        """Update polygon name"""
        result = self.db.update_polygon_name(polygon_id, new_name)
        if result:
            # Refresh the data
            self.existing_polygons = self.load_polygon_data()
        return result

    def create_map(self):
        """Create a Folium map with all markers, deterrents, and polygons"""
        # Initialize map with OpenStreetMap as the default base layer
        m = folium.Map(location=self.map_center, zoom_start=10, control_scale=True)
        
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
        for _, row in self.deterrent_data.iterrows():
            # Get the device ID
            raw_device_id = row.get('directory_name', row.get('id', ''))
            device_id = self.format_device_id(raw_device_id)
            
            # Get image counts from the database
            device_image_count = self.db.get_image_count(device_id, "device")
            trail_image_count = self.db.get_image_count(device_id, "trail")
            
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
        for _, row in self.existing_markers.iterrows():
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
        for _, row in self.existing_polygons.iterrows():
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
                'rectangle': True,
                'polygon': True,
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

    # In class JapanDeterrentSystem within japan_deterrents.py:

    def display_device_images_in_sidebar(self):
        """Display images for a selected device in the sidebar with datetime filtering"""
        st.sidebar.header("Japan Deterrent Images")

        # Get device IDs for selection
        device_ids = []
        if not self.deterrent_data.empty:
            id_col = 'id' if 'id' in self.deterrent_data.columns else 'directory_name'
            if id_col in self.deterrent_data.columns:
                for raw_id in self.deterrent_data[id_col]:
                    formatted_id = self.format_device_id(raw_id)
                    if formatted_id not in device_ids:
                         device_ids.append(formatted_id)
                device_ids.sort()
            else:
                st.sidebar.warning(f"Could not find ID column ('{id_col}') in deterrent data.")

        device_options = ["None"] + device_ids

        # Store previous device selection to detect changes
        if 'prev_selected_device' not in st.session_state:
            st.session_state.prev_selected_device = "None"

        selected_device = st.sidebar.selectbox(
            "Select Deterrent ID",
            options=device_options,
            index=0 # Default to "None"
            # Consider adding a key if needed elsewhere: key="selected_device_key"
        )

        # --- Reset image display flags if device changes ---
        if selected_device != st.session_state.prev_selected_device:
            st.session_state.show_device_images = False
            st.session_state.show_trail_images = False
            # Potentially reset page numbers too
            if 'device_page' in st.session_state: del st.session_state['device_page']
            if 'trail_page' in st.session_state: del st.session_state['trail_page']
            st.session_state.prev_selected_device = selected_device
            # We need to rerun ONLY if a device *was* selected and is now different,
            # otherwise selecting 'None' initially would cause an unnecessary rerun.
            # However, Streamlit handles reruns on widget changes anyway.

        if selected_device and selected_device != "None":
            # Initialize filter variables
            use_filter = False
            start_date = None
            end_date = None
            filter_mode = "Date Range" # Default filter mode
            daily_time_filter = None

            with st.sidebar.expander("Date/Time Filters", expanded=False):
                # --- Date Filter Section START ---
                min_year = 2024
                now = datetime.datetime.now()
                max_year = now.year
                years = list(range(min_year, max_year + 1))

                try:
                    default_end_year_index = years.index(now.year)
                except ValueError:
                    default_end_year_index = len(years) - 1

                default_end_month_index = now.month - 1
                default_end_day_index = now.day - 1

                filter_mode = st.radio("Filter Mode", ["Date Range", "Daily Time Period"], key="filter_mode")

                if filter_mode == "Date Range":
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("Start Date:")
                        start_year = st.selectbox("Year", years, key="start_year", index=0)
                        start_month = st.selectbox("Month", range(1, 13), key="start_month", index=0)
                        # Improve day selection safety
                        start_max_days = pd.Timestamp(start_year, start_month, 1).days_in_month
                        start_day_options = range(1, start_max_days + 1)
                        start_day_index = 0 # Default to 1st
                        start_day = st.selectbox("Day", start_day_options, key="start_day", index=start_day_index)

                    with col2:
                        st.write("End Date:")
                        end_year = st.selectbox("Year", years, key="end_year", index=default_end_year_index)
                        end_month = st.selectbox("Month", range(1, 13), key="end_month", index=default_end_month_index)
                        # Improve day selection safety
                        end_max_days = pd.Timestamp(end_year, end_month, 1).days_in_month
                        end_day_options = range(1, end_max_days + 1)
                        # Ensure default index is valid
                        safe_end_day_index = min(default_end_day_index, end_max_days - 1)
                        end_day = st.selectbox("Day", end_day_options, key="end_day", index=safe_end_day_index)

                    st.write("Time Range (24-hour format):")
                    start_hour = st.slider("Start Hour", 0, 23, 0, key="start_hour")
                    end_hour = st.slider("End Hour", 0, 23, 23, key="end_hour")

                    try:
                        start_date = dt(start_year, start_month, start_day, start_hour, 0)
                        end_date = dt(end_year, end_month, end_day, end_hour, 59)
                        if start_date > end_date: st.error("Start date cannot be after end date!"); use_filter = False
                        else: use_filter = True; st.success(f"Filtering: {start_date.strftime('%Y-%m-%d %H:%M')} to {end_date.strftime('%Y-%m-%d %H:%M')}")
                    except ValueError: st.error("Invalid date selected!"); use_filter = False

                else: # Daily Time Period mode
                    st.markdown("### Daily Time Period Filter")
                    # ... (keep logic for daily filter) ...
                    col1, col2 = st.columns(2)
                    with col1:
                        period_start_year = st.selectbox("From Year", years, key="period_start_year", index=0)
                        period_start_month = st.selectbox("Month", range(1, 13), key="period_start_month", index=0)
                    with col2:
                        period_end_year = st.selectbox("To Year", years, key="period_end_year", index=default_end_year_index)
                        period_end_month = st.selectbox("Month", range(1, 13), key="period_end_month", index=default_end_month_index)

                    st.markdown("#### Time of Day to Include:")
                    daily_start_hour = st.slider("Daily Start Hour", 0, 23, 21, key="daily_start_hour")
                    daily_end_hour = st.slider("Daily End Hour", 0, 23, 23, key="daily_end_hour")

                    if daily_start_hour <= daily_end_hour: time_range_text = f"Every day from {daily_start_hour}:00 to {daily_end_hour}:59"
                    else: time_range_text = f"Every night from {daily_start_hour}:00 to {daily_end_hour}:59 (overnight)"
                    st.info(time_range_text)
                    try:
                        # Use the full year/month range selected for the period
                        start_date = dt(period_start_year, period_start_month, 1, 0, 0)
                        period_end_max_days = pd.Timestamp(period_end_year, period_end_month, 1).days_in_month
                        end_date = dt(period_end_year, period_end_month, period_end_max_days, 23, 59)

                        daily_time_filter = (daily_start_hour, daily_end_hour) # Set the filter tuple
                        use_filter = True
                        st.success(f"Filtering: {start_date.strftime('%Y-%m')} to {end_date.strftime('%Y-%m')} during {time_range_text}")
                    except ValueError: st.error("Invalid date selection!"); use_filter = False
                 # --- Date Filter Section END ---

                include_unsuccessful = st.checkbox("Include 'unsuccessful_parsing' images", value=False)
                if st.button("Reset Filters"):
                    st.session_state.show_device_images = False # Hide images on reset
                    st.session_state.show_trail_images = False
                    # Clear specific filter keys if needed
                    # ...
                    st.rerun()

            # Define filter parameters based on UI state
            filter_start_date = start_date if use_filter else None
            filter_end_date = end_date if use_filter else None
            filter_daily_time = daily_time_filter if use_filter and filter_mode == "Daily Time Period" else None

            # Get image lists (potentially filtered) - runs on every interaction, but cached
            device_image_list = self.get_image_files(selected_device, image_type="device", start_date=filter_start_date, end_date=filter_end_date, include_unsuccessful=include_unsuccessful, daily_time_filter=filter_daily_time)
            trail_image_list = self.get_image_files(selected_device, image_type="trail", start_date=filter_start_date, end_date=filter_end_date, include_unsuccessful=include_unsuccessful, daily_time_filter=filter_daily_time)

            # --- Button Logic using Session State ---
            col1, col2 = st.sidebar.columns(2)
            with col1:
                if st.button("Load Device Images", key="load_device_btn", help=f"{len(device_image_list)} device images available", use_container_width=True):
                    st.session_state.show_device_images = True
                    st.session_state.show_trail_images = False
                    # Reset page number when button clicked
                    st.session_state.device_page = 1
            with col2:
                 if st.button("Load Trail Images", key="load_trail_btn", help=f"{len(trail_image_list)} trail images available", use_container_width=True):
                    st.session_state.show_trail_images = True
                    st.session_state.show_device_images = False
                     # Reset page number when button clicked
                    st.session_state.trail_page = 1
            # --- End Button Logic ---

            # --- Display Logic using Session State ---
            if st.session_state.get("show_device_images", False):
                st.sidebar.subheader("Device Images")
                if device_image_list:
                    st.sidebar.write(f"Found {len(device_image_list)} device images")
                    show_image_info = st.sidebar.checkbox("Show detailed image info", value=False, key="show_device_info_cb") # Unique key
                    items_per_page = 5
                    total_pages = (len(device_image_list) + items_per_page - 1) // items_per_page

                    # Use session state for page number
                    if 'device_page' not in st.session_state:
                        st.session_state.device_page = 1

                    page = 1 # Default if only one page
                    if total_pages > 1:
                        # Get page number from session state
                        page = st.sidebar.number_input("Page", min_value=1, max_value=total_pages,
                                                       key="device_page", # Use session state key here
                                                       help="Select page number")
                        st.sidebar.write(f"Page {st.session_state.device_page} of {total_pages}")

                    start_idx = (st.session_state.device_page - 1) * items_per_page # Use state variable
                    end_idx = min(start_idx + items_per_page, len(device_image_list))

                    for img_data in device_image_list[start_idx:end_idx]:
                        img_path = img_data['image_path']; filename = img_data['filename']
                        caption = filename
                        if show_image_info and img_data.get('timestamp'): # Use .get for safety
                            try: img_date = dt.fromisoformat(img_data['timestamp']); date_info = img_date.strftime("%Y-%m-%d %H:%M"); caption = f"{filename}\nDate: {date_info}"
                            except (ValueError, TypeError): pass
                        if os.path.exists(img_path):
                             st.sidebar.image(img_path, caption=caption, use_container_width=True)
                        else:
                             st.sidebar.warning(f"Img not found: {img_path}") # Shorter warning
                else:
                    st.sidebar.info("No device images found with the current filters")

            if st.session_state.get("show_trail_images", False):
                st.sidebar.subheader("Trail Images")
                if trail_image_list:
                    st.sidebar.write(f"Found {len(trail_image_list)} trail images")
                    show_image_info = st.sidebar.checkbox("Show detailed image info", value=False, key="show_trail_info_cb") # Unique key
                    items_per_page = 5
                    total_pages = (len(trail_image_list) + items_per_page - 1) // items_per_page

                    # Use session state for page number
                    if 'trail_page' not in st.session_state:
                        st.session_state.trail_page = 1

                    page = 1 # Default if only one page
                    if total_pages > 1:
                         # Get page number from session state
                        page = st.sidebar.number_input("Page", min_value=1, max_value=total_pages,
                                                       key="trail_page", # Use session state key
                                                       help="Select page number")
                        st.sidebar.write(f"Page {st.session_state.trail_page} of {total_pages}")

                    start_idx = (st.session_state.trail_page - 1) * items_per_page # Use state variable
                    end_idx = min(start_idx + items_per_page, len(trail_image_list))

                    # Use .get() for safer dictionary access
                    successful_images = [img for img in trail_image_list[start_idx:end_idx] if img.get('parsed_successfully', False)]
                    unsuccessful_images = [img for img in trail_image_list[start_idx:end_idx] if not img.get('parsed_successfully', False)]

                    if successful_images:
                        for img_data in successful_images:
                            img_path = img_data['image_path']; filename = img_data['filename']
                            caption = filename
                            if show_image_info and img_data.get('timestamp'):
                                try: img_date = dt.fromisoformat(img_data['timestamp']); date_info = img_date.strftime("%Y-%m-%d %H:%M"); caption = f"{filename}\nDate: {date_info}"
                                except (ValueError, TypeError): pass
                            if os.path.exists(img_path):
                                st.sidebar.image(img_path, caption=caption, use_container_width=True)
                            else:
                                st.sidebar.warning(f"Img not found: {img_path}")

                    if unsuccessful_images:
                        st.sidebar.markdown("---"); st.sidebar.warning("Images with unsuccessful parsing:")
                        for img_data in unsuccessful_images:
                            img_path = img_data['image_path']; filename = img_data['filename']
                            if os.path.exists(img_path):
                                st.sidebar.image(img_path, caption=filename, use_container_width=True)
                            else:
                                st.sidebar.warning(f"Img not found: {img_path}")

                else:
                    st.sidebar.info("No trail images found with the current filters")
             # --- End Display Logic ---

        else:
            st.sidebar.info("Select a deterrent ID to view images")
            # Clear flags when no device is selected
            st.session_state.show_device_images = False
            st.session_state.show_trail_images = False

    # display_japan_deterrent_section and other methods remain the same...
    # ...

    def display_japan_deterrent_section(self):
        """Display the Japan Deterrent System section in the Streamlit app."""
        # Add a title
        st.title("Interactive Bear Deterrent Mapping System")
        
        # Create tabs for different functionality
        tab1, tab2 = st.tabs(["Markers", "Areas"])
        
        with tab1:
            # Display the map with a hint about layer selection
            st.subheader("Deterrent Devices Map")
            st.write("Use the marker tool (in the top-left) to place markers, then click 'Save New Markers' to save them.")
            st.write("Each blue marker shows its ID number for easy identification.")
            st.info("👉 Look for the layer control in the top-right corner of the map to switch between different Japanese maps.")

            # Create the map
            m = self.create_map()
            map_data = st_folium(m, width=1000, height=500, key="folium_map")
            
            # Display images in sidebar
            self.display_device_images_in_sidebar()

            # Add buttons for saving and deleting all markers
            col1, col2 = st.columns([1, 1])
            with col1:
                if st.button("Save New Markers & Areas", key="save_markers", use_container_width=True):
                    if map_data and 'all_drawings' in map_data and map_data['all_drawings']:
                        # Get all drawings
                        drawings = map_data['all_drawings']
                        
                        # Save all markers and polygons
                        saved_points, saved_polygons = self.save_coordinates_from_geojson(drawings)
                        
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
                        if self.delete_all_markers():
                            st.success("All markers have been deleted.")
                            # Reset confirmation state
                            st.session_state.confirm_delete_all = False
                            # Rerun to update the map
                            st.rerun()
                        else:
                            st.error("Error deleting markers.")

            # Create an interactive table with delete buttons
            st.subheader("Manage Markers")
            current_markers = self.load_existing_data()  # Fresh load

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
                            if self.delete_marker(row['id']):
                                st.success(f"Marker {row['id']} deleted")
                                # Force page refresh to update the map
                                st.rerun()
                            else:
                                st.error(f"Failed to delete marker {row['id']}")
            else:
                st.info("No custom markers have been added yet.")

            # Display existing deterrent devices
            st.subheader("Existing Deterrent Devices")
            st.dataframe(self.deterrent_data)
        
        with tab2:
            # Areas tab
            st.subheader("Areas Management")
            
            # Display existing polygons in a table
            polygon_data = self.load_polygon_data()
            
            if not polygon_data.empty:
                st.write(f"Found {len(polygon_data)} defined areas")
                
                # Create a table for polygon management
                cols = st.columns([1, 2, 2, 1])
                
                # Headers
                with cols[0]:
                    st.write("**ID**")
                with cols[1]:
                    st.write("**Name**")
                with cols[2]:
                    st.write("**Created**")
                with cols[3]:
                    st.write("**Action**")
                
                # Draw a separator line
                st.markdown("---")
                
                # Display each polygon in a row with edit/delete buttons
                for index, row in polygon_data.iterrows():
                    col1, col2, col3, col4 = st.columns([1, 2, 2, 1])
                    
                    with col1:
                        st.write(f"{row['polygon_id']}")
                    with col2:
                        # Allow editing the name with a text input
                        new_name = st.text_input(
                            "Name", 
                            value=row['name'], 
                            key=f"name_{row['polygon_id']}",
                            label_visibility="collapsed"
                        )
                        # Update if name changed
                        if new_name != row['name']:
                            if self.update_polygon_name(row['polygon_id'], new_name):
                                st.success(f"Name updated to '{new_name}'")
                                st.rerun()
                    with col3:
                        st.write(f"{row['timestamp']}")
                    with col4:
                        if st.button("Delete", key=f"delete_poly_{row['polygon_id']}"):
                            if self.delete_polygon(row['polygon_id']):
                                st.success(f"Area {row['polygon_id']} deleted")
                                st.rerun()
                            else:
                                st.error(f"Failed to delete area {row['polygon_id']}")
                
                # Add a button to delete all polygons
                if st.button("Delete All Areas", key="delete_all_polys"):
                    # Confirm deletion
                    if 'confirm_delete_all_polygons' not in st.session_state:
                        st.session_state.confirm_delete_all_polygons = True
                        st.warning("Are you sure you want to delete ALL areas? Click the button again to confirm.")
                    else:
                        # User confirmed, delete all polygons
                        if self.delete_all_polygons():
                            st.success("All areas have been deleted.")
                            # Reset confirmation state
                            st.session_state.confirm_delete_all_polygons = False
                            # Rerun to update the display
                            st.rerun()
                        else:
                            st.error("Error deleting areas.")
            else:
                st.info("No areas have been defined yet. Use the polygon or rectangle drawing tools on the map to create areas.")

# This can be used for testing the component independently
if __name__ == "__main__":
    deterrent_system = JapanDeterrentSystem()
    deterrent_system.display_japan_deterrent_section()