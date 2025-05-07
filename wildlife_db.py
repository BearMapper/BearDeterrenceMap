# wildlife_db.py
import sqlite3
import pandas as pd
import os
import json
from datetime import datetime
import shutil
import numpy as np
from pyproj import Transformer

class WildlifeDatabase:
    def __init__(self, db_path="wildlife_data.db"):
        self.db_path = db_path
        # Initialize the coordinate transformer
        self.transformer = Transformer.from_crs("EPSG:3844", "EPSG:4326", always_xy=True)

    def get_distinct_values(self, table_name, column_name):
        """Get distinct values from a specific column in a table"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            query = f"SELECT DISTINCT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL"
            cursor = conn.cursor()
            cursor.execute(query)
            distinct_values = [item[0] for item in cursor.fetchall()]
            return distinct_values
        except sqlite3.Error as e:
            print(f"Database error in get_distinct_values: {e}")
            return []
        finally:
            if conn: conn.close()
    
    def fix_timestamp_issues(self):
        """Fix any issues with timestamps in the database"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check for NULL timestamps
            cursor.execute("SELECT COUNT(*) FROM bears_tracking WHERE timestamp IS NULL")
            null_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM bears_tracking WHERE timestamp IS NOT NULL")
            non_null_count = cursor.fetchone()[0]
            
            print(f"Timestamp status: {non_null_count} with timestamp, {null_count} without timestamp")
            
            if null_count > 0 and non_null_count == 0:
                print("All timestamps are NULL. Creating artificial timestamps...")
                
                # Get the bears and their record counts
                cursor.execute("SELECT bear_id, COUNT(*) FROM bears_tracking GROUP BY bear_id")
                bear_counts = cursor.fetchall()
                
                base_date = datetime(2018, 1, 1)  # Start with a reasonable date
                updated_count = 0
                
                for bear_id, count in bear_counts:
                    print(f"Processing {count} records for bear {bear_id}...")
                    
                    # Get all records for this bear
                    cursor.execute("SELECT id FROM bears_tracking WHERE bear_id = ? ORDER BY id", (bear_id,))
                    record_ids = [row[0] for row in cursor.fetchall()]
                    
                    # Create evenly spread timestamps
                    for i, record_id in enumerate(record_ids):
                        # Create a timestamp spaced 1 hour apart for each record
                        timestamp = base_date + pd.Timedelta(hours=i)
                        
                        cursor.execute(
                            "UPDATE bears_tracking SET timestamp = ? WHERE id = ?",
                            (timestamp.isoformat(), record_id)
                        )
                        updated_count += 1
                    
                    # Advance base date by 6 months for the next bear
                    base_date = base_date + pd.Timedelta(days=180)
                
                # Commit changes
                conn.commit()
                print(f"Created artificial timestamps for {updated_count} records.")
                
                return True
            else:
                print("Timestamp data appears to be OK.")
                return False
                
        except Exception as e:
            print(f"Error fixing timestamps: {e}")
            if conn: conn.rollback()
            return False
        finally:
            if conn: conn.close()
    

    def initialize_db(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create original tables
            cursor.execute('CREATE TABLE IF NOT EXISTS deterrent_devices (id TEXT PRIMARY KEY, directory_name TEXT, lat REAL, lng REAL)')
            cursor.execute('CREATE TABLE IF NOT EXISTS markers (id TEXT PRIMARY KEY, timestamp TEXT, lat REAL, lng REAL)')
            cursor.execute('CREATE TABLE IF NOT EXISTS polygons (polygon_id TEXT PRIMARY KEY, timestamp TEXT, name TEXT, coordinates TEXT)')
            cursor.execute('CREATE TABLE IF NOT EXISTS image_metadata (id INTEGER PRIMARY KEY AUTOINCREMENT, device_id TEXT, image_path TEXT, image_type TEXT, filename TEXT, timestamp TEXT, parsed_successfully BOOLEAN, FOREIGN KEY (device_id) REFERENCES deterrent_devices(id))')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_image_device ON image_metadata(device_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_image_timestamp ON image_metadata(timestamp)')
            
            # Create new tables for Carpathian bears data
            # Use DROP TABLE to ensure we rebuild the table with the correct schema
            cursor.execute("DROP TABLE IF EXISTS bears_tracking")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS bears_tracking (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bear_id TEXT,
                    timestamp TEXT,
                    x REAL,
                    y REAL,
                    lat REAL,
                    lng REAL,
                    season_code TEXT,
                    season TEXT,
                    sex TEXT,
                    age TEXT,
                    is_daytime BOOLEAN
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS bears_mcp (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    bear_id TEXT,
                    area REAL,
                    sex TEXT,
                    age TEXT,
                    num_gmu INTEGER,
                    stage TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS bears_core_area (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bear_id TEXT,
                    sex TEXT,
                    age TEXT,
                    area REAL
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS bears_kde (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bear_id TEXT,
                    area REAL,
                    sex TEXT,
                    age TEXT,
                    num_gmu INTEGER,
                    stage TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS bears_seasonal_mcp (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bear_id TEXT,
                    area REAL,
                    season TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS bears_daily_displacement (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bear_id TEXT,
                    x REAL,
                    y REAL,
                    lat REAL,
                    lng REAL,
                    date TEXT,
                    distance REAL,
                    season TEXT,
                    altitude REAL
                )
            ''')
            
            conn.commit()
        except Exception as e:
            print(f"!!! Error during initialize_db: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

    # Keep all existing methods for deterrent devices, markers, polygons, etc.
    # [Original methods would be preserved here]

    # --- Deterrent Devices Methods ---
    def import_deterrent_devices(self, csv_path="data/device_data/deterrent_devices.csv"):
        if not os.path.exists(csv_path): print(f"CSV file not found: {csv_path}"); return False
        try:
            df = pd.read_csv(csv_path, dtype={'Directory name': str})
            print(f"Read CSV with columns: {df.columns.tolist()}"); num_csv_records = len(df)
            print(f"Found {num_csv_records} deterrent devices in CSV")
        except Exception as e: print(f"Error reading CSV {csv_path}: {e}"); return False
        conn = None
        try:
            conn = sqlite3.connect(self.db_path); cursor = conn.cursor()
            print("Dropping deterrent_devices table (if exists)..."); cursor.execute("DROP TABLE IF EXISTS deterrent_devices")
            print("Creating deterrent_devices table..."); cursor.execute('''CREATE TABLE deterrent_devices (id TEXT PRIMARY KEY, directory_name TEXT, lat REAL, lng REAL)''')
            print("Table 'deterrent_devices' created.")
            successful_inserts = 0; failed_inserts = 0
            print("Starting data insertion...")
            for _, row in df.iterrows():
                try:
                    if 'Directory name' not in row or pd.isna(row['Directory name']) or 'lat' not in row or 'lng' not in row: failed_inserts += 1; continue
                    directory_name_raw = str(row['Directory name']).strip(); id_cleaned = directory_name_raw
                    if '.' in id_cleaned:
                        try:
                           id_float = float(id_cleaned)
                           if id_float == int(id_float): id_cleaned = str(int(id_float))
                        except ValueError: pass
                    cursor.execute("INSERT INTO deterrent_devices (id, directory_name, lat, lng) VALUES (?, ?, ?, ?)", (id_cleaned, id_cleaned, row['lat'], row['lng']))
                    successful_inserts += 1
                except sqlite3.IntegrityError: print(f"Record with ID {id_cleaned} already exists (IntegrityError), skipping"); failed_inserts += 1
                except Exception as e: print(f"Error inserting record for '{id_cleaned}': {e}"); failed_inserts += 1
            print(f"Insertion loop finished: {successful_inserts} successful attempts, {failed_inserts} failed/skipped.")
            print("Attempting to commit insertions..."); conn.commit(); print("Commit successful.")
            cursor.execute("SELECT COUNT(*) FROM deterrent_devices"); post_commit_count = cursor.fetchone()[0]
            print(f"Verification Query: Found {post_commit_count} records in table immediately after commit.")
            if post_commit_count == successful_inserts: print(f"Table rebuilt successfully with {post_commit_count} records."); return True
            else: print(f"!!! Mismatch: Expected {successful_inserts} records, but found {post_commit_count} after commit!"); return False
        except Exception as e: print(f"!!! Error during deterrent devices import process: {e}"); return False
        finally:
            if conn: print("Closing database connection for import."); conn.close()

    def get_deterrent_devices(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql("SELECT * FROM deterrent_devices ORDER BY id", conn, dtype={'id': str, 'directory_name': str})
        except Exception as e: print(f"!!! Error reading deterrent devices from DB: {e}"); df = pd.DataFrame()
        finally:
             if conn: conn.close()
        return df

    # --- Markers Methods ---
    def import_markers(self, csv_path="data/device_data/artificial_device_coordinates.csv"):
        if not os.path.exists(csv_path): print(f"Markers CSV file not found: {csv_path}. Skipping markers import."); return True
        try: df = pd.read_csv(csv_path, dtype={'id': str})
        except Exception as e: print(f"Error reading markers CSV {csv_path}: {e}"); return False
        conn = None
        try:
            conn = sqlite3.connect(self.db_path); cursor = conn.cursor()
            print(f"Importing markers from {csv_path}. Existing markers will be kept (duplicates skipped).")
            inserted_count = 0; skipped_count = 0
            for _, row in df.iterrows():
                 marker_id = str(row['id']) if pd.notna(row['id']) else None;
                 if not marker_id: skipped_count += 1; continue
                 try:
                    if 'timestamp' not in row or 'lat' not in row or 'lng' not in row: skipped_count += 1; continue
                    cursor.execute("INSERT OR IGNORE INTO markers (id, timestamp, lat, lng) VALUES (?, ?, ?, ?)",(marker_id, row['timestamp'], row['lat'], row['lng']))
                    if cursor.rowcount > 0: inserted_count += 1
                    else: skipped_count += 1
                 except Exception as e: print(f"Error inserting marker {marker_id}: {e}"); skipped_count += 1
            conn.commit()
            print(f"Attempted marker import: {inserted_count} new markers inserted, {skipped_count} skipped (duplicates/errors).")
            return True
        except Exception as e: print(f"!!! Error during markers import process: {e}"); return False
        finally:
            if conn: conn.close()
    
    def get_markers(self):
        conn = None
        try: conn = sqlite3.connect(self.db_path); df = pd.read_sql("SELECT * FROM markers ORDER BY id", conn, dtype={'id': str})
        except Exception as e: print(f"!!! Error reading markers from DB: {e}"); df = pd.DataFrame()
        finally:
             if conn: conn.close()
        return df
        
    def save_marker(self, marker_id, lat, lng):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path); timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S"); marker_id_str = str(marker_id)
            conn.execute("INSERT INTO markers (id, timestamp, lat, lng) VALUES (?, ?, ?, ?)",(marker_id_str, timestamp, lat, lng)); conn.commit(); return True
        except sqlite3.IntegrityError: return False
        except Exception as e: print(f"Error saving marker {marker_id_str}: {e}"); return False
        finally:
            if conn: conn.close()
            
    def delete_marker(self, marker_id):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path); cursor = conn.cursor(); marker_id_str = str(marker_id)
            cursor.execute("DELETE FROM markers WHERE id = ?", (marker_id_str,)); deleted_rows = cursor.rowcount; conn.commit(); return deleted_rows > 0
        except Exception as e: print(f"Error deleting marker {marker_id}: {e}"); return False
        finally:
            if conn: conn.close()
            
    def delete_all_markers(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path); cursor = conn.cursor()
            cursor.execute("DELETE FROM markers"); deleted_rows = cursor.rowcount; conn.commit(); print(f"Deleted {deleted_rows} markers."); return True
        except Exception as e: print(f"Error deleting all markers: {e}"); return False
        finally:
            if conn: conn.close()
            
    def get_next_marker_id(self):
        conn = None; ids = []
        try:
            conn = sqlite3.connect(self.db_path); cursor = conn.cursor(); cursor.execute("SELECT id FROM markers"); ids = cursor.fetchall()
        except Exception as e: print(f"Error getting next marker ID: {e}")
        finally:
            if conn: conn.close()
        numeric_ids = [];
        for id_tuple in ids:
            try: numeric_ids.append(int(id_tuple[0]))
            except (ValueError, TypeError, IndexError): pass
        return max(numeric_ids, default=0) + 1

    # --- Polygons Methods ---
    def import_polygons(self, csv_path="data/areas/user_drawn_area_cities.csv"):
        if not os.path.exists(csv_path): print(f"Polygons CSV file not found: {csv_path}. Skipping polygons import."); return True
        try: df = pd.read_csv(csv_path, dtype={'polygon_id': str})
        except Exception as e: print(f"Error reading polygons CSV {csv_path}: {e}"); return False
        conn = None
        try:
            conn = sqlite3.connect(self.db_path); cursor = conn.cursor()
            print(f"Importing polygons from {csv_path}. Existing polygons will be kept (duplicates skipped).")
            inserted_count = 0; skipped_count = 0
            for _, row in df.iterrows():
                 polygon_id = str(row['polygon_id']) if pd.notna(row['polygon_id']) else None
                 if not polygon_id: skipped_count += 1; continue
                 try:
                     if 'timestamp' not in row or 'name' not in row or 'coordinates' not in row: skipped_count += 1; continue
                     cursor.execute("INSERT OR IGNORE INTO polygons (polygon_id, timestamp, name, coordinates) VALUES (?, ?, ?, ?)",(polygon_id, row['timestamp'], row['name'], row['coordinates']))
                     if cursor.rowcount > 0: inserted_count += 1
                     else: skipped_count += 1
                 except Exception as e: print(f"Error inserting polygon {polygon_id}: {e}"); skipped_count += 1
            conn.commit()
            print(f"Attempted polygon import: {inserted_count} new polygons inserted, {skipped_count} skipped (duplicates/errors).")
            return True
        except Exception as e: print(f"!!! Error during polygons import process: {e}"); return False
        finally:
             if conn: conn.close()
             
    def get_polygons(self):
        conn = None
        try: conn = sqlite3.connect(self.db_path); df = pd.read_sql("SELECT * FROM polygons ORDER BY polygon_id", conn, dtype={'polygon_id': str, 'name': str})
        except Exception as e: print(f"!!! Error reading polygons from DB: {e}"); df = pd.DataFrame()
        finally:
             if conn: conn.close()
        if not df.empty and 'coordinates' in df.columns:
            def safe_json_loads(x):
                if isinstance(x, str):
                    try: return json.loads(x)
                    except json.JSONDecodeError: return None
                return x
            df['coordinates'] = df['coordinates'].apply(safe_json_loads)
        return df
        
    def save_polygon(self, polygon_id, name, coordinates):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path); timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S"); polygon_id_str = str(polygon_id); name_str = str(name)
            if not isinstance(coordinates, str): coordinates_str = json.dumps(coordinates)
            else: coordinates_str = coordinates
            conn.execute("INSERT INTO polygons (polygon_id, timestamp, name, coordinates) VALUES (?, ?, ?, ?)",(polygon_id_str, timestamp, name_str, coordinates_str)); conn.commit(); return True
        except sqlite3.IntegrityError: return False
        except Exception as e: print(f"Error saving polygon {polygon_id_str}: {e}"); return False
        finally:
            if conn: conn.close()
            
    def update_polygon_name(self, polygon_id, new_name):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path); cursor = conn.cursor(); polygon_id_str = str(polygon_id); new_name_str = str(new_name)
            cursor.execute("UPDATE polygons SET name = ? WHERE polygon_id = ?", (new_name_str, polygon_id_str)); updated_rows = cursor.rowcount; conn.commit(); return updated_rows > 0
        except Exception as e: print(f"Error updating polygon name for {polygon_id}: {e}"); return False
        finally:
            if conn: conn.close()
            
    def delete_polygon(self, polygon_id):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path); cursor = conn.cursor(); polygon_id_str = str(polygon_id)
            cursor.execute("DELETE FROM polygons WHERE polygon_id = ?", (polygon_id_str,)); deleted_rows = cursor.rowcount; conn.commit(); return deleted_rows > 0
        except Exception as e: print(f"Error deleting polygon {polygon_id}: {e}"); return False
        finally:
            if conn: conn.close()
            
    def delete_all_polygons(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path); cursor = conn.cursor()
            cursor.execute("DELETE FROM polygons"); deleted_rows = cursor.rowcount; conn.commit(); print(f"Deleted {deleted_rows} polygons."); return True
        except Exception as e: print(f"Error deleting all polygons: {e}"); return False
        finally:
            if conn: conn.close()
            
    def get_next_polygon_id(self):
        conn = None; ids = []
        try:
            conn = sqlite3.connect(self.db_path); cursor = conn.cursor(); cursor.execute("SELECT polygon_id FROM polygons"); ids = cursor.fetchall()
        except Exception as e: print(f"Error getting next polygon ID: {e}")
        finally:
            if conn: conn.close()
        numeric_ids = []
        for id_tuple in ids:
            try:
                if isinstance(id_tuple[0], str) and id_tuple[0].startswith('poly-'):
                     numeric_part = id_tuple[0].split('-')[-1]; numeric_ids.append(int(numeric_part))
            except (ValueError, IndexError, TypeError): pass
        return max(numeric_ids, default=0) + 1

    # --- Image Metadata Methods ---
    def index_image_files(self, base_folder="data/bear_pictures", reindex=False):
        """Scan directory and index image files, checking ONLY device and trail_processed."""
        start_time = datetime.now()
        conn = None
        images_indexed = 0
        folders_processed = 0
        skipped_files = 0
        total_files_found = 0

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            if reindex:
                print("Reindexing: Clearing existing image metadata...")
                cursor.execute("DELETE FROM image_metadata")
                conn.commit()
                print("Existing image metadata cleared.")

            if not os.path.isdir(base_folder):
                 print(f"Error: Base image folder '{base_folder}' not found.")
                 return 0

            print(f"Starting image scan in '{base_folder}'...")
            try:
                device_dirs = os.listdir(base_folder)
            except OSError as e_list_base:
                print(f"!!! OS error listing base directory {base_folder}: {e_list_base}")
                return 0

            print(f"Found {len(device_dirs)} items in base folder.")

            # --- Start loop for each item in base_folder ---
            for device_dir in device_dirs:
                device_path = os.path.join(base_folder, device_dir)
                if not os.path.isdir(device_path):
                    print(f"  Skipping non-directory item: {device_dir}")
                    continue

                folders_processed += 1
                print(f"\nProcessing Device Folder: {device_path}")
                device_id_raw = str(device_dir).strip()
                device_id_cleaned = device_id_raw
                # Clean ID
                if '.' in device_id_cleaned:
                    try:
                       id_float = float(device_id_cleaned)
                       if id_float == int(id_float): device_id_cleaned = str(int(id_float))
                    except ValueError: pass
                print(f"  Device ID (Cleaned): {device_id_cleaned}")

                # Function to process images in a subfolder
                def process_image_folder(image_folder_path, image_type):
                    nonlocal images_indexed, skipped_files, total_files_found
                    print(f"  Scanning Subfolder: {image_folder_path} (Type: {image_type})")
                    if os.path.isdir(image_folder_path):
                        files_processed_in_folder = 0
                        try:
                            files_in_folder = os.listdir(image_folder_path)
                            print(f"    Found {len(files_in_folder)} items.")
                            for img_file in files_in_folder:
                                total_files_found += 1
                                file_path = os.path.join(image_folder_path, img_file)
                                if os.path.isfile(file_path) and img_file.lower().endswith(('.jpg', '.jpeg', '.png')):
                                    files_processed_in_folder += 1
                                    is_unsuccessful = "unsuccessful_parsing" in img_file and image_type == "trail_processed"
                                    parsed_successfully = not is_unsuccessful
                                    storage_image_type = "trail" if image_type == "trail_processed" else "device"
                                    timestamp = self._extract_timestamp_from_filename(img_file, image_type)
                                    img_path = file_path.replace("\\", "/")
                                    try:
                                        cursor.execute(
                                            """INSERT INTO image_metadata
                                               (device_id, image_path, image_type, filename, timestamp, parsed_successfully)
                                               VALUES (?, ?, ?, ?, ?, ?)""",
                                            (device_id_cleaned, img_path, storage_image_type, img_file, timestamp, parsed_successfully)
                                        )
                                        images_indexed += 1
                                    except Exception as e_insert:
                                         print(f"      !!! Error inserting metadata for {img_path}: {e_insert}")
                                         skipped_files += 1
                                else:
                                     skipped_files += 1
                            print(f"    Finished processing {files_processed_in_folder} potential images in this subfolder.")
                        except OSError as e_os:
                            print(f"    !!! OS error reading folder {image_folder_path}: {e_os}")
                            skipped_files += 1
                    else:
                         print(f"    Subfolder not found or not a directory: {image_folder_path}")

                # Process image folders
                process_image_folder(os.path.join(device_path, "device"), "device")
                process_image_folder(os.path.join(device_path, "trail_processed"), "trail_processed")

            # Commit after processing ALL device folders
            print("\nCommitting all indexed image metadata...")
            conn.commit()
            print("Image metadata commit successful.")

        except Exception as e:
             print(f"!!! Error during image indexing main process: {e}")
             if conn: conn.rollback()
        finally:
             if conn:
                 print("Closing database connection for image indexing.")
                 conn.close()

        elapsed_time = (datetime.now() - start_time).total_seconds()
        print(f"\n--- Image Indexing Summary ---")
        print(f"Processed {folders_processed} potential device folders.")
        print(f"Encountered {total_files_found} total file system items in relevant subfolders.")
        print(f"Successfully indexed {images_indexed} images.")
        print(f"Skipped {skipped_files} non-image files or files causing errors.")
        print(f"Indexing completed in {elapsed_time:.2f} seconds.")
        print(f"------------------------------")
        return images_indexed

    def _extract_timestamp_from_filename(self, filename, image_type):
        # Extract timestamp from filename
        logic_type = "trail" if image_type == "trail_processed" else image_type
        try:
            if logic_type == "trail":
                if "unsuccessful_parsing" in filename: return None
                base_name = os.path.splitext(filename)[0]; parts = base_name.split('.'); date_part = base_name
                if len(parts) >= 4 and all(p.isdigit() for p in parts[-4:]): date_part = ".".join(parts[-4:])
                dt_obj = datetime.strptime(date_part, "%Y.%m.%d.%H%M"); return dt_obj.isoformat()
            elif logic_type == "device":
                parts = filename.split('_')
                if len(parts) >= 3:
                    date_str = parts[2]; date_str_cleaned = "".join(filter(lambda c: c.isdigit() or c == '.', date_str))
                    dt_obj = datetime.strptime(date_str_cleaned, "%Y.%m.%d.%H%M"); return dt_obj.isoformat()
        except (ValueError, IndexError, TypeError): pass
        return None

    def get_images(self, device_id, image_type=None, start_date=None, end_date=None, daily_time_filter=None, include_unsuccessful=False, limit=100, offset=0):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path); device_id_str = str(device_id); query = "SELECT * FROM image_metadata WHERE device_id = ?"; params = [device_id_str]
            if image_type: query += " AND image_type = ?"; params.append(image_type)
            if not include_unsuccessful: query += " AND parsed_successfully = 1"
            if start_date and end_date:
                start_iso = start_date.isoformat() if isinstance(start_date, datetime) else start_date; end_iso = end_date.isoformat() if isinstance(end_date, datetime) else end_date
                query += " AND timestamp >= ? AND timestamp <= ?"; params.extend([start_iso, end_iso])
            if daily_time_filter:
                start_hour, end_hour = daily_time_filter; hour_extract = "CAST(strftime('%H', timestamp) AS INTEGER)"
                if start_hour <= end_hour: query += f" AND {hour_extract} BETWEEN ? AND ?"; params.extend([start_hour, end_hour])
                else: query += f" AND ({hour_extract} >= ? OR {hour_extract} <= ?)"; params.extend([start_hour, end_hour])
            query += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"; params.extend([limit, offset])
            df = pd.read_sql(query, conn, params=params, dtype={'device_id': str})
        except Exception as e: print(f"!!! Error executing get_images query: {e}"); df = pd.DataFrame()
        finally:
             if conn: conn.close()
        return df

    def get_image_count(self, device_id, image_type=None, include_unsuccessful=False):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path); cursor = conn.cursor()
            if device_id is not None: device_id_str = str(device_id); query = "SELECT COUNT(*) FROM image_metadata WHERE device_id = ?"; params = [device_id_str]
            else: query = "SELECT COUNT(*) FROM image_metadata WHERE 1=1"; params = []
            if image_type: query += " AND image_type = ?"; params.append(image_type)
            if not include_unsuccessful: query += " AND parsed_successfully = 1"
            cursor.execute(query, params); count = cursor.fetchone()[0]
        except Exception as e: print(f"Error getting image count: {e}"); count = 0
        finally:
            if conn: conn.close()
        return count

    # --- New Carpathian Bears Data Methods ---
    
    def _transform_coordinates(self, x, y):
        """Transform coordinates from EPSG:3844 to WGS84"""
        if pd.isna(x) or pd.isna(y):
            return None, None
        try:
            lng, lat = self.transformer.transform(x, y)
            return lat, lng
        except Exception as e:
            print(f"Error transforming coordinates ({x}, {y}): {e}")
            return None, None
    
    def import_bears_data(self, csv_path="data/animal_data/carpathian_bears/1_bears_RO.csv"):
        """Import Carpathian Bears tracking data with properly formatted timestamps"""
        if not os.path.exists(csv_path):
            print(f"Bears CSV file not found: {csv_path}")
            return False
            
        try:
            # Read CSV file, handling potential encoding or format issues
            df = pd.read_csv(csv_path, dtype={
                'X': float, 
                'Y': float, 
                'Name': str, 
                'Season': str, 
                'Season2': str, 
                'Sex': str, 
                'age': str
            })
            
            # Check for timestamp column (both uppercase and lowercase)
            timestamp_col = None
            if 'Timestamp' in df.columns:
                timestamp_col = 'Timestamp'
            elif 'timestamp' in df.columns:
                timestamp_col = 'timestamp'
                
            if timestamp_col:
                # Make sure to properly parse the timestamp format from the CSV
                df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')
                print(f"Sample timestamp from CSV: {df[timestamp_col].iloc[0] if not df.empty else 'No data'}")
                df.dropna(subset=[timestamp_col], inplace=True)
            else:
                print(f"WARNING: No timestamp column found in {csv_path}")
            
            print(f"Read {len(df)} bear tracking records from {csv_path}")
        except Exception as e:
            print(f"Error reading bears CSV {csv_path}: {e}")
            return False
            
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Clear existing data
            print("Clearing existing bears_tracking data...")
            cursor.execute("DELETE FROM bears_tracking")
            conn.commit()
            
            # Insert new data
            count = 0
            failed_count = 0
            
            for _, row in df.iterrows():
                try:
                    # Transform coordinates from EPSG:3844 to WGS84
                    lat, lng = self._transform_coordinates(row.get('X'), row.get('Y'))
                    
                    # Prepare timestamp - ENSURE ISO FORMAT STRING
                    ts_iso = None
                    if timestamp_col and pd.notna(row.get(timestamp_col)):
                        ts_iso = row[timestamp_col].isoformat()
                    
                    # Determine if it's daytime (between 6 AM and 8 PM)
                    is_daytime = False
                    if timestamp_col and pd.notna(row.get(timestamp_col)):
                        try:
                            hour = row[timestamp_col].hour
                            is_daytime = 6 <= hour < 20
                        except:
                            pass
                            
                    cursor.execute(
                        """INSERT INTO bears_tracking 
                        (bear_id, timestamp, x, y, lat, lng, season_code, season, sex, age, is_daytime) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            str(row.get('Name', '')).strip() if pd.notna(row.get('Name')) else None,
                            ts_iso,  # This should be a proper ISO formatted timestamp string
                            row.get('X'),
                            row.get('Y'),
                            lat,
                            lng,
                            str(row.get('Season', '')).strip() if pd.notna(row.get('Season')) else None,
                            str(row.get('Season2', '')).strip() if pd.notna(row.get('Season2')) else None,
                            str(row.get('Sex', '')).strip() if pd.notna(row.get('Sex')) else None,
                            str(row.get('age', '')).strip() if pd.notna(row.get('age')) else None,
                            is_daytime
                        )
                    )
                    count += 1
                    if count % 1000 == 0:
                        print(f"Processed {count} records...")
                except Exception as e:
                    print(f"Error inserting bear tracking record: {e}")
                    failed_count += 1
                    continue
            
            # Commit changes
            conn.commit()
            print(f"Imported {count} bear tracking records, failed {failed_count}")
            
            # Verify timestamps
            cursor.execute("SELECT COUNT(*) FROM bears_tracking WHERE timestamp IS NULL")
            null_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM bears_tracking WHERE timestamp IS NOT NULL")
            non_null_count = cursor.fetchone()[0]
            print(f"Timestamp status: {non_null_count} with timestamp, {null_count} without timestamp")
            
            return count > 0
        except Exception as e:
            print(f"!!! Error during bears tracking import process: {e}")
            if conn: conn.rollback()
            return False
        finally:
            if conn: conn.close()
    
    def import_bears_seasonal_data(self, csv_path="data/animal_data/carpathian_bears/2_bears_RO_seasons.csv"):
        """Import additional seasonal data for bears with properly formatted timestamps"""
        if not os.path.exists(csv_path):
            print(f"Bears seasonal CSV file not found: {csv_path}")
            return False
            
        try:
            # Read CSV file
            df = pd.read_csv(csv_path, dtype={
                'X': float, 
                'Y': float, 
                'Name': str, 
                'Season': str, 
                'Season2': str, 
                'Sex': str, 
                'age': str
            })
            
            # Check for timestamp column (both uppercase and lowercase)
            timestamp_col = None
            if 'Timestamp' in df.columns:
                timestamp_col = 'Timestamp'
            elif 'timestamp' in df.columns:
                timestamp_col = 'timestamp'
                
            if timestamp_col:
                # Make sure to properly parse the timestamp format from the CSV
                df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')
                print(f"Sample timestamp from seasonal CSV: {df[timestamp_col].iloc[0] if not df.empty else 'No data'}")
                df.dropna(subset=[timestamp_col], inplace=True)
            else:
                print(f"WARNING: No timestamp column found in {csv_path}")
                
            print(f"Read {len(df)} bear seasonal records from {csv_path}")
        except Exception as e:
            print(f"Error reading bears seasonal CSV {csv_path}: {e}")
            return False
            
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Insert seasonal data
            count = 0
            failed_count = 0
            
            for _, row in df.iterrows():
                try:
                    # Transform coordinates
                    lat, lng = self._transform_coordinates(row.get('X'), row.get('Y'))
                    
                    # Prepare timestamp - ENSURE ISO FORMAT STRING
                    ts_iso = None
                    if timestamp_col and pd.notna(row.get(timestamp_col)):
                        ts_iso = row[timestamp_col].isoformat()
                    
                    # Determine if it's daytime (between 6 AM and 8 PM)
                    is_daytime = False
                    if timestamp_col and pd.notna(row.get(timestamp_col)):
                        try:
                            hour = row[timestamp_col].hour
                            is_daytime = 6 <= hour < 20
                        except:
                            pass
                            
                    cursor.execute(
                        """INSERT INTO bears_tracking 
                        (bear_id, timestamp, x, y, lat, lng, season_code, season, sex, age, is_daytime) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            str(row.get('Name', '')).strip() if pd.notna(row.get('Name')) else None,
                            ts_iso,  # This should be a proper ISO formatted timestamp string
                            row.get('X'),
                            row.get('Y'),
                            lat,
                            lng,
                            str(row.get('Season', '')).strip() if pd.notna(row.get('Season')) else None,
                            str(row.get('Season2', '')).strip() if pd.notna(row.get('Season2')) else None,
                            str(row.get('Sex', '')).strip() if pd.notna(row.get('Sex')) else None,
                            str(row.get('age', '')).strip() if pd.notna(row.get('age')) else None,
                            is_daytime
                        )
                    )
                    count += 1
                    if count % 1000 == 0:
                        print(f"Processed {count} seasonal records...")
                except Exception as e:
                    print(f"Error inserting bear seasonal record: {e}")
                    failed_count += 1
                    continue
            
            conn.commit()
            print(f"Imported {count} bear seasonal records, failed {failed_count}")
            
            # Verify timestamps
            cursor.execute("SELECT COUNT(*) FROM bears_tracking WHERE timestamp IS NULL")
            null_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM bears_tracking WHERE timestamp IS NOT NULL")
            non_null_count = cursor.fetchone()[0]
            print(f"Timestamp status after seasonal import: {non_null_count} with timestamp, {null_count} without timestamp")
            
            return count > 0
        except Exception as e:
            print(f"!!! Error during bears seasonal import process: {e}")
            if conn: conn.rollback()
            return False
        finally:
            if conn: conn.close()
    
    def import_mcp_data(self, csv_path="data/animal_data/carpathian_bears/3_mcphr_bears_1.csv"):
        """Import MCP (Minimum Convex Polygon) data for bears"""
        if not os.path.exists(csv_path):
            print(f"MCP CSV file not found: {csv_path}")
            return False
            
        try:
            df = pd.read_csv(csv_path, dtype={
                'id': str,
                'area': float,
                'Sex': str,
                'age': str,
                'No_GMU': int,
                'Stage': str
            })
            print(f"Read {len(df)} MCP records from {csv_path}")
        except Exception as e:
            print(f"Error reading MCP CSV {csv_path}: {e}")
            return False
            
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Clear existing data
            print("Clearing existing bears_mcp data...")
            cursor.execute("DELETE FROM bears_mcp")
            conn.commit()
            
            # Insert new data
            count = 0
            failed_count = 0
            
            for _, row in df.iterrows():
                try:
                    cursor.execute(
                        """INSERT INTO bears_mcp 
                           (bear_id, area, sex, age, num_gmu, stage) 
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (
                            str(row.get('id', '')).strip() if pd.notna(row.get('id')) else None,
                            row.get('area'),
                            str(row.get('Sex', '')).strip() if pd.notna(row.get('Sex')) else None,
                            str(row.get('age', '')).strip() if pd.notna(row.get('age')) else None,
                            row.get('No_GMU'),
                            str(row.get('Stage', '')).strip() if pd.notna(row.get('Stage')) else None
                        )
                    )
                    count += 1
                except Exception as e:
                    print(f"Error inserting MCP record: {e}")
                    failed_count += 1
                    continue
            
            conn.commit()
            print(f"Imported {count} MCP records, failed {failed_count}")
            return count > 0
        except Exception as e:
            print(f"!!! Error during MCP import process: {e}")
            if conn: conn.rollback()
            return False
        finally:
            if conn: conn.close()
    
    def import_core_area_data(self, csv_path="data/animal_data/carpathian_bears/4_core_area_bears_RO.csv"):
        """Import core area data for bears"""
        if not os.path.exists(csv_path):
            print(f"Core area CSV file not found: {csv_path}")
            return False
            
        try:
            df = pd.read_csv(csv_path, dtype={
                'id': str,
                'Sex': str,
                'age': str,
                'area': float
            })
            print(f"Read {len(df)} core area records from {csv_path}")
        except Exception as e:
            print(f"Error reading core area CSV {csv_path}: {e}")
            return False
            
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Clear existing data
            print("Clearing existing bears_core_area data...")
            cursor.execute("DELETE FROM bears_core_area")
            conn.commit()
            
            # Insert new data
            count = 0
            failed_count = 0
            
            for _, row in df.iterrows():
                try:
                    cursor.execute(
                        """INSERT INTO bears_core_area 
                           (bear_id, sex, age, area) 
                           VALUES (?, ?, ?, ?)""",
                        (
                            str(row.get('id', '')).strip() if pd.notna(row.get('id')) else None,
                            str(row.get('Sex', '')).strip() if pd.notna(row.get('Sex')) else None,
                            str(row.get('age', '')).strip() if pd.notna(row.get('age')) else None,
                            row.get('area')
                        )
                    )
                    count += 1
                except Exception as e:
                    print(f"Error inserting core area record: {e}")
                    failed_count += 1
                    continue
            
            conn.commit()
            print(f"Imported {count} core area records, failed {failed_count}")
            return count > 0
        except Exception as e:
            print(f"!!! Error during core area import process: {e}")
            if conn: conn.rollback()
            return False
        finally:
            if conn: conn.close()
    
    def import_kde_data(self, csv_path="data/animal_data/carpathian_bears/5_HR_kernels_bears_1.csv"):
        """Import KDE (Kernel Density Estimation) data for bears"""
        if not os.path.exists(csv_path):
            print(f"KDE CSV file not found: {csv_path}")
            return False
            
        try:
            df = pd.read_csv(csv_path, dtype={
                'id': str,
                'area': float,
                'Sex': str,
                'age': str,
                'No_GMU': int,
                'Stage': str
            })
            print(f"Read {len(df)} KDE records from {csv_path}")
        except Exception as e:
            print(f"Error reading KDE CSV {csv_path}: {e}")
            return False
            
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Clear existing data
            print("Clearing existing bears_kde data...")
            cursor.execute("DELETE FROM bears_kde")
            conn.commit()
            
            # Insert new data
            count = 0
            failed_count = 0
            
            for _, row in df.iterrows():
                try:
                    cursor.execute(
                        """INSERT INTO bears_kde 
                           (bear_id, area, sex, age, num_gmu, stage) 
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (
                            str(row.get('id', '')).strip() if pd.notna(row.get('id')) else None,
                            row.get('area'),
                            str(row.get('Sex', '')).strip() if pd.notna(row.get('Sex')) else None,
                            str(row.get('age', '')).strip() if pd.notna(row.get('age')) else None,
                            row.get('No_GMU'),
                            str(row.get('Stage', '')).strip() if pd.notna(row.get('Stage')) else None
                        )
                    )
                    count += 1
                except Exception as e:
                    print(f"Error inserting KDE record: {e}")
                    failed_count += 1
                    continue
            
            conn.commit()
            print(f"Imported {count} KDE records, failed {failed_count}")
            return count > 0
        except Exception as e:
            print(f"!!! Error during KDE import process: {e}")
            if conn: conn.rollback()
            return False
        finally:
            if conn: conn.close()
    
    def import_seasonal_mcp_data(self, csv_path="data/animal_data/carpathian_bears/6_mcphrs_all_seasons.csv"):
        """Import seasonal MCP data for bears"""
        if not os.path.exists(csv_path):
            print(f"Seasonal MCP CSV file not found: {csv_path}")
            return False
            
        try:
            df = pd.read_csv(csv_path, dtype={
                'id': str,
                'area': float,
                'Season': str
            })
            print(f"Read {len(df)} seasonal MCP records from {csv_path}")
        except Exception as e:
            print(f"Error reading seasonal MCP CSV {csv_path}: {e}")
            return False
            
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Clear existing data
            print("Clearing existing bears_seasonal_mcp data...")
            cursor.execute("DELETE FROM bears_seasonal_mcp")
            conn.commit()
            
            # Insert new data
            count = 0
            failed_count = 0
            
            for _, row in df.iterrows():
                try:
                    cursor.execute(
                        """INSERT INTO bears_seasonal_mcp 
                           (bear_id, area, season) 
                           VALUES (?, ?, ?)""",
                        (
                            str(row.get('id', '')).strip() if pd.notna(row.get('id')) else None,
                            row.get('area'),
                            str(row.get('Season', '')).strip() if pd.notna(row.get('Season')) else None
                        )
                    )
                    count += 1
                except Exception as e:
                    print(f"Error inserting seasonal MCP record: {e}")
                    failed_count += 1
                    continue
            
            conn.commit()
            print(f"Imported {count} seasonal MCP records, failed {failed_count}")
            return count > 0
        except Exception as e:
            print(f"!!! Error during seasonal MCP import process: {e}")
            if conn: conn.rollback()
            return False
        finally:
            if conn: conn.close()
    
    def import_daily_displacement_data(self, csv_path="data/animal_data/carpathian_bears/7_dist_bears_RO.csv"):
        """Import daily displacement data for bears"""
        if not os.path.exists(csv_path):
            print(f"Daily displacement CSV file not found: {csv_path}")
            return False
            
        try:
            df = pd.read_csv(csv_path, dtype={
                'id': int,
                'X': float,
                'Y': float,
                'Date': str,
                'dist': float,
                'Season': str,
                'Name': str,
                'alt': float
            })
            
            # Convert date column
            if 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
                df.dropna(subset=['Date'], inplace=True)
                
            print(f"Read {len(df)} daily displacement records from {csv_path}")
        except Exception as e:
            print(f"Error reading daily displacement CSV {csv_path}: {e}")
            return False
            
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Clear existing data
            print("Clearing existing bears_daily_displacement data...")
            cursor.execute("DELETE FROM bears_daily_displacement")
            conn.commit()
            
            # Insert new data
            count = 0
            failed_count = 0
            
            for _, row in df.iterrows():
                try:
                    # Transform coordinates
                    lat, lng = self._transform_coordinates(row.get('X'), row.get('Y'))
                    
                    # Prepare date
                    date_iso = row['Date'].isoformat() if pd.notna(row.get('Date')) else None
                    
                    cursor.execute(
                        """INSERT INTO bears_daily_displacement 
                           (bear_id, x, y, lat, lng, date, distance, season, altitude) 
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            str(row.get('Name', '')).strip() if pd.notna(row.get('Name')) else None,
                            row.get('X'),
                            row.get('Y'),
                            lat,
                            lng,
                            date_iso,
                            row.get('dist'),
                            str(row.get('Season', '')).strip() if pd.notna(row.get('Season')) else None,
                            row.get('alt')
                        )
                    )
                    count += 1
                    if count % 1000 == 0:
                        print(f"Processed {count} daily displacement records...")
                except Exception as e:
                    print(f"Error inserting daily displacement record: {e}")
                    failed_count += 1
                    continue
            
            conn.commit()
            print(f"Imported {count} daily displacement records, failed {failed_count}")
            return count > 0
        except Exception as e:
            print(f"!!! Error during daily displacement import process: {e}")
            if conn: conn.rollback()
            return False
        finally:
            if conn: conn.close()
    
    # --- Query methods for Carpathian bears data ---
    
    def get_bears_list(self):
        """Get a list of all bears in the database with basic information"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            query = """
            SELECT DISTINCT 
                bear_id, 
                sex,
                age,
                COUNT(*) as point_count
            FROM bears_tracking
            WHERE bear_id IS NOT NULL
            GROUP BY bear_id, sex, age
            ORDER BY bear_id
            """
            df = pd.read_sql(query, conn)
        except Exception as e:
            print(f"!!! Error getting bears list: {e}")
            df = pd.DataFrame()
        finally:
            if conn: conn.close()
        return df
    
    def get_seasonal_data(self, bear_id=None):
        """Get seasonal movement data for bears"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            query = """
            SELECT 
                bear_id,
                season,
                sex,
                age,
                COUNT(*) as point_count,
                AVG(x) as avg_x,
                AVG(y) as avg_y,
                MIN(x) as min_x,
                MAX(x) as max_x,
                MIN(y) as min_y,
                MAX(y) as max_y,
                MIN(lat) as min_lat,
                MAX(lat) as max_lat,
                MIN(lng) as min_lng,
                MAX(lng) as max_lng,
                COUNT(DISTINCT date(timestamp)) as day_count
            FROM bears_tracking
            """
            params = []
            
            if bear_id:
                query += " WHERE bear_id = ? AND bear_id IS NOT NULL"
                params.append(bear_id)
            else:
                query += " WHERE bear_id IS NOT NULL"
                
            query += " GROUP BY bear_id, season, sex, age ORDER BY bear_id, season"
            
            df = pd.read_sql(query, conn, params=params)
        except Exception as e:
            print(f"!!! Error getting seasonal data: {e}")
            df = pd.DataFrame()
        finally:
            if conn: conn.close()
        return df
    
    def get_home_range_data(self, bear_id=None):
        """Get home range data for bears"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            query = """
            SELECT 
                m.bear_id,
                m.sex,
                m.age,
                m.area as mcp_area,
                c.area as core_area,
                k.area as kde_area,
                m.stage
            FROM 
                bears_mcp m
            LEFT JOIN 
                bears_core_area c ON m.bear_id = c.bear_id
            LEFT JOIN 
                bears_kde k ON m.bear_id = k.bear_id
            """
            params = []
            
            if bear_id:
                query += " WHERE m.bear_id = ?"
                params.append(bear_id)
                
            query += " ORDER BY m.bear_id"
            
            df = pd.read_sql(query, conn, params=params)
        except Exception as e:
            print(f"!!! Error getting home range data: {e}")
            df = pd.DataFrame()
        finally:
            if conn: conn.close()
        return df
    
    def get_daily_movement(self, bear_id=None, start_date=None, end_date=None):
        """Get daily movement data for bears"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            query = """
            SELECT 
                bear_id,
                date,
                x,
                y,
                lat,
                lng,
                distance,
                season,
                altitude
            FROM bears_daily_displacement
            """
            params = []
            
            where_clauses = []
            if bear_id:
                where_clauses.append("bear_id = ?")
                params.append(bear_id)
                
            if start_date:
                start_iso = start_date.isoformat() if isinstance(start_date, datetime) else start_date
                where_clauses.append("date >= ?")
                params.append(start_iso)
                
            if end_date:
                end_iso = end_date.isoformat() if isinstance(end_date, datetime) else end_date
                where_clauses.append("date <= ?")
                params.append(end_iso)
                
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
                
            query += " ORDER BY bear_id, date"
            
            df = pd.read_sql(query, conn, params=params)
            
            # Convert date strings to datetime
            if not df.empty and 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                
        except Exception as e:
            print(f"!!! Error getting daily movement data: {e}")
            df = pd.DataFrame()
        finally:
            if conn: conn.close()
        return df
    
    def get_bear_data(self, bear_id=None, start_date=None, end_date=None, season=None):
        """Get tracking data for one or all bears with filtering options"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Build query with parameters
            query = "SELECT * FROM bears_tracking WHERE 1=1"
            params = []
            
            if bear_id:
                query += " AND bear_id = ?"
                params.append(bear_id)
                
            if start_date:
                start_iso = start_date.isoformat() if isinstance(start_date, datetime) else start_date
                query += " AND timestamp >= ?"
                params.append(start_iso)
                
            if end_date:
                end_iso = end_date.isoformat() if isinstance(end_date, datetime) else end_date
                query += " AND timestamp <= ?"
                params.append(end_iso)
                
            if season:
                query += " AND season = ?"
                params.append(season)
                
            query += " ORDER BY bear_id, timestamp"
            
            df = pd.read_sql(query, conn, params=params)
            
            # Convert timestamp strings to datetime
            if not df.empty and 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                
        except Exception as e:
            print(f"!!! Error getting bear data: {e}")
            df = pd.DataFrame()
        finally:
            if conn: conn.close()
        return df
    
    def get_date_range(self, table_name, date_column):
        """Get the minimum and maximum date from a specific column in a table"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            query = f"SELECT MIN({date_column}), MAX({date_column}) FROM {table_name} WHERE {date_column} IS NOT NULL"
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            
            # Improved error handling and reporting
            if result and result[0] and result[1]:
                try:
                    min_date = pd.to_datetime(result[0]).date()
                    max_date = pd.to_datetime(result[1]).date()
                    return min_date, max_date
                except Exception as e:
                    print(f"Error parsing dates from database: {e}")
                    print(f"Raw date values from database: {result[0]} to {result[1]}")
                    # Return default dates if parsing fails
                    return datetime(2018, 1, 1).date(), datetime(2023, 12, 31).date()
            else:
                print(f"No valid date range found in database for {table_name}.{date_column}")
                # Return default dates if no data found
                return datetime(2018, 1, 1).date(), datetime(2023, 12, 31).date()
        except sqlite3.Error as e:
            print(f"Database error in get_date_range: {e}")
            return datetime(2018, 1, 1).date(), datetime(2023, 12, 31).date()
        except Exception as e:
            print(f"Error processing dates in get_date_range: {e}")
            return datetime(2018, 1, 1).date(), datetime(2023, 12, 31).date()
        finally:
            if conn: conn.close()

def fix_timestamp_issues(self):
    """Fix any issues with timestamps in the database"""
    conn = None
    try:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check for NULL timestamps
        cursor.execute("SELECT COUNT(*) FROM bears_tracking WHERE timestamp IS NULL")
        null_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM bears_tracking WHERE timestamp IS NOT NULL")
        non_null_count = cursor.fetchone()[0]
        
        print(f"Timestamp status: {non_null_count} with timestamp, {null_count} without timestamp")
        
        if null_count > 0 and non_null_count == 0:
            print("All timestamps are NULL. Creating artificial timestamps...")
            
            # Get the bears and their record counts
            cursor.execute("SELECT bear_id, COUNT(*) FROM bears_tracking GROUP BY bear_id")
            bear_counts = cursor.fetchall()
            
            base_date = datetime(2018, 1, 1)  # Start with a reasonable date
            updated_count = 0
            
            for bear_id, count in bear_counts:
                print(f"Processing {count} records for bear {bear_id}...")
                
                # Get all records for this bear
                cursor.execute("SELECT id FROM bears_tracking WHERE bear_id = ? ORDER BY id", (bear_id,))
                record_ids = [row[0] for row in cursor.fetchall()]
                
                # Create evenly spread timestamps
                for i, record_id in enumerate(record_ids):
                    # Create a timestamp spaced 1 hour apart for each record
                    timestamp = base_date + pd.Timedelta(hours=i)
                    
                    cursor.execute(
                        "UPDATE bears_tracking SET timestamp = ? WHERE id = ?",
                        (timestamp.isoformat(), record_id)
                    )
                    updated_count += 1
                
                # Advance base date by 6 months for the next bear
                base_date = base_date + pd.Timedelta(days=180)
            
            # Commit changes
            conn.commit()
            print(f"Created artificial timestamps for {updated_count} records.")
            
            return True
        else:
            print("Timestamp data appears to be OK.")
            return False
            
    except Exception as e:
        print(f"Error fixing timestamps: {e}")
        if conn: conn.rollback()
        return False
    finally:
        if conn: conn.close()

# Data migration function for Carpathian Bears
def migrate_carpathian_bears_data():
    """Migrate all Carpathian Bears data to SQLite database"""
    print("--- Starting Carpathian Bears Data Migration ---")
    db = WildlifeDatabase()
    db.initialize_db()  # Ensure tables exist
    
    bears_csv = "data/animal_data/carpathian_bears/1_bears_RO.csv"
    seasonal_csv = "data/animal_data/carpathian_bears/2_bears_RO_seasons.csv"
    mcp_csv = "data/animal_data/carpathian_bears/3_mcphr_bears_1.csv"
    core_area_csv = "data/animal_data/carpathian_bears/4_core_area_bears_RO.csv"
    kde_csv = "data/animal_data/carpathian_bears/5_HR_kernels_bears_1.csv"
    seasonal_mcp_csv = "data/animal_data/carpathian_bears/6_mcphrs_all_seasons.csv"
    daily_displacement_csv = "data/animal_data/carpathian_bears/7_dist_bears_RO.csv"
    
    print("\n[1/7] Importing bears tracking data...")
    if db.import_bears_data(bears_csv): 
        print("✓ Bears tracking data import step finished.")
    else: 
        print("✗ Bears tracking data import step failed.")
    
    print("\n[2/7] Importing bears seasonal data...")
    if db.import_bears_seasonal_data(seasonal_csv): 
        print("✓ Bears seasonal data import step finished.")
    else: 
        print("✗ Bears seasonal data import step failed.")
    
    print("\n[3/7] Importing MCP data...")
    if db.import_mcp_data(mcp_csv): 
        print("✓ MCP data import step finished.")
    else: 
        print("✗ MCP data import step failed.")
    
    print("\n[4/7] Importing core area data...")
    if db.import_core_area_data(core_area_csv): 
        print("✓ Core area data import step finished.")
    else: 
        print("✗ Core area data import step failed.")
    
    print("\n[5/7] Importing KDE data...")
    if db.import_kde_data(kde_csv): 
        print("✓ KDE data import step finished.")
    else: 
        print("✗ KDE data import step failed.")
    
    print("\n[6/7] Importing seasonal MCP data...")
    if db.import_seasonal_mcp_data(seasonal_mcp_csv): 
        print("✓ Seasonal MCP data import step finished.")
    else: 
        print("✗ Seasonal MCP data import step failed.")
    
    print("\n[7/7] Importing daily displacement data...")
    if db.import_daily_displacement_data(daily_displacement_csv): 
        print("✓ Daily displacement data import step finished.")
    else: 
        print("✗ Daily displacement data import step failed.")
    
    print("\n--- Carpathian Bears Migration Complete ---")
    
    # Print final counts
    final_conn = None
    try:
        final_conn = sqlite3.connect(db.db_path)
        tracking_count = final_conn.execute("SELECT COUNT(*) FROM bears_tracking").fetchone()[0]
        print(f"  Bears Tracking Records: {tracking_count}")
        mcp_count = final_conn.execute("SELECT COUNT(*) FROM bears_mcp").fetchone()[0]
        print(f"  MCP Records: {mcp_count}")
        core_area_count = final_conn.execute("SELECT COUNT(*) FROM bears_core_area").fetchone()[0]
        print(f"  Core Area Records: {core_area_count}")
        kde_count = final_conn.execute("SELECT COUNT(*) FROM bears_kde").fetchone()[0]
        print(f"  KDE Records: {kde_count}")
        seasonal_mcp_count = final_conn.execute("SELECT COUNT(*) FROM bears_seasonal_mcp").fetchone()[0]
        print(f"  Seasonal MCP Records: {seasonal_mcp_count}")
        displacement_count = final_conn.execute("SELECT COUNT(*) FROM bears_daily_displacement").fetchone()[0]
        print(f"  Daily Displacement Records: {displacement_count}")
    except Exception as e_check:
        print(f"Error checking final counts directly: {e_check}")
    finally:
        print("\nChecking for timestamp issues...")
        db.fix_timestamp_issues()
        if final_conn: final_conn.close()
    

# Complete migration function
def migrate_csv_to_sqlite():
    """Migrate all CSV data to SQLite database"""
    print("--- Starting Complete Data Migration ---")
    db = WildlifeDatabase()
    db.initialize_db()  # Ensure tables exist
    
    # Original data files
    deterrent_csv = "data/device_data/deterrent_devices.csv"
    markers_csv = "data/device_data/artificial_device_coordinates.csv"
    polygons_csv = "data/areas/user_drawn_area_cities.csv"
    image_base_folder = "data/bear_pictures"
    
    print("\n[1/5] Importing deterrent devices...")
    if db.import_deterrent_devices(deterrent_csv): print("✓ Deterrent devices import step finished.")
    else: print("✗ Deterrent devices import step failed.")
    
    print("\n[2/5] Importing custom markers...")
    if db.import_markers(markers_csv): print("✓ Markers import step finished.")
    else: print("✗ Markers import step failed.")
    
    print("\n[3/5] Importing areas/polygons...")
    if db.import_polygons(polygons_csv): print("✓ Polygons import step finished.")
    else: print("✗ Polygons import step failed.")
    
    print("\n[4/5] Indexing image files...")
    img_count = db.index_image_files(image_base_folder, reindex=True)
    if img_count >= 0: print(f"✓ Image indexing step finished ({img_count} images indexed).")
    else: print("✗ Image indexing step failed.")
    
    print("\n[5/5] Importing Carpathian bears data...")
    migrate_carpathian_bears_data()
    
    print("\n--- Complete Migration Finished ---")

# Run migration if executed directly
if __name__ == "__main__":
    migrate_csv_to_sqlite()
   