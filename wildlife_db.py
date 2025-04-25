# wildlife_db.py (Corrected indentation for process_image_folder calls)
import sqlite3
import pandas as pd
import os
import json
from datetime import datetime
import shutil

class WildlifeDatabase:
    # __init__ and initialize_db remain the same
    def __init__(self, db_path="wildlife_data.db"):
        self.db_path = db_path
        # self.initialize_db() # Called explicitly before migration now

    def initialize_db(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            # Create tables (abbreviated for clarity - use full from previous)
            cursor.execute('CREATE TABLE IF NOT EXISTS deterrent_devices (id TEXT PRIMARY KEY, directory_name TEXT, lat REAL, lng REAL)')
            cursor.execute('CREATE TABLE IF NOT EXISTS markers (id TEXT PRIMARY KEY, timestamp TEXT, lat REAL, lng REAL)')
            cursor.execute('CREATE TABLE IF NOT EXISTS polygons (polygon_id TEXT PRIMARY KEY, timestamp TEXT, name TEXT, coordinates TEXT)')
            cursor.execute('CREATE TABLE IF NOT EXISTS image_metadata (id INTEGER PRIMARY KEY AUTOINCREMENT, device_id TEXT, image_path TEXT, image_type TEXT, filename TEXT, timestamp TEXT, parsed_successfully BOOLEAN, FOREIGN KEY (device_id) REFERENCES deterrent_devices(id))')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_image_device ON image_metadata(device_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_image_timestamp ON image_metadata(timestamp)')
            cursor.execute('CREATE TABLE IF NOT EXISTS bears_tracking (id INTEGER PRIMARY KEY AUTOINCREMENT, bear_id TEXT, timestamp TEXT, x REAL, y REAL, lat REAL, lng REAL, season TEXT, sex TEXT, age TEXT, is_daytime BOOLEAN)')
            conn.commit()
        except Exception as e:
            print(f"!!! Error during initialize_db: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

    # --- Deterrent Devices Methods ---
    # (Keep the version that worked correctly)
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
    # (Keep previous version)
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
    # (Keep previous version)
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

                # *** CORRECTED INDENTATION: Calls are now INSIDE the loop ***
                process_image_folder(os.path.join(device_path, "device"), "device")
                process_image_folder(os.path.join(device_path, "trail_processed"), "trail_processed") # Check processed

            # --- End of loop for device_dir ---

            # Commit after processing ALL device folders
            print("\nCommitting all indexed image metadata...")
            conn.commit()
            print("Image metadata commit successful.")

        except Exception as e:
             print(f"!!! Error during image indexing main process: {e}")
             if conn: conn.rollback() # Rollback on error during the main loop/setup
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
        # (Keep same as previous version)
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

    # get_images, get_image_count remain the same
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

    # --- Carpathian Bears Data Methods ---
    # (Keep previous version)
    def import_bears_tracking_data(self, csv_path="data/animal_data/carpathian_bears/processed/bears_processed.csv"):
        if not os.path.exists(csv_path): print(f"CSV file not found: {csv_path}"); return False
        conn = None
        try:
             df = pd.read_csv(csv_path, dtype={'Name': str, 'Season2': str, 'Sex': str, 'Age': str, 'timestamp': 'object'})
             df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce'); df.dropna(subset=['timestamp'], inplace=True)
        except Exception as e: print(f"Error reading bears tracking CSV {csv_path}: {e}"); return False
        try:
            conn = sqlite3.connect(self.db_path); cursor = conn.cursor()
            print("Clearing existing bears_tracking data..."); cursor.execute("DELETE FROM bears_tracking"); conn.commit()
            count = 0; failed_count = 0
            for _, row in df.iterrows():
                try:
                    ts_iso = row['timestamp'].isoformat() if pd.notna(row['timestamp']) else None
                    cursor.execute( """INSERT INTO bears_tracking (bear_id, timestamp, x, y, lat, lng, season, sex, age, is_daytime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (str(row.get('Name', '')) if pd.notna(row.get('Name')) else None, ts_iso, row.get('X', None), row.get('Y', None), row.get('lat', None), row.get('lng', None), str(row.get('Season2', '')) if pd.notna(row.get('Season2')) else None, str(row.get('Sex', '')) if pd.notna(row.get('Sex')) else None, str(row.get('Age', '')) if pd.notna(row.get('Age')) else None, bool(row.get('is_daytime', False))) )
                    count += 1
                except Exception as e: failed_count += 1; continue
            conn.commit()
            print(f"Imported {count} bear tracking records, failed {failed_count}")
            return count > 0
        except Exception as e: print(f"!!! Error during bears tracking import process: {e}"); return False
        finally:
             if conn: conn.close()
    def get_bear_movement_by_season(self, bear_id=None):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path); query = """ SELECT bear_id, season, sex, age, COUNT(*) as point_count, AVG(x) as avg_x, AVG(y) as avg_y, MIN(x) as min_x, MAX(x) as max_x, MIN(y) as min_y, MAX(y) as max_y FROM bears_tracking """; params = []
            if bear_id is not None: bear_id_str = str(bear_id); query += " WHERE bear_id = ?"; params.append(bear_id_str)
            query += " GROUP BY bear_id, season, sex, age ORDER BY bear_id, season"
            df = pd.read_sql(query, conn, params=params, dtype={'bear_id': str, 'season': str, 'sex': str, 'age': str})
        except Exception as e: print(f"!!! Error getting bear movement by season: {e}"); df = pd.DataFrame()
        finally:
             if conn: conn.close()
        return df

# Data migration function (keep as is)
def migrate_csv_to_sqlite():
    """Migrate all CSV data to SQLite database"""
    print("--- Starting Data Migration ---")
    db = WildlifeDatabase()
    db.initialize_db() # Ensure tables exist

    deterrent_csv = "data/device_data/deterrent_devices.csv"
    markers_csv = "data/device_data/artificial_device_coordinates.csv"
    polygons_csv = "data/areas/user_drawn_area_cities.csv"
    image_base_folder = "data/bear_pictures"
    bears_csv = "data/animal_data/carpathian_bears/processed/bears_processed.csv"

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
    if db.import_bears_tracking_data(bears_csv): print("✓ Bears tracking data import step finished.")
    else: print("✗ Bears tracking data import step failed.")

    print("\n--- Migration Complete ---")
    print("Final counts (read directly from DB):")
    final_conn = None
    try:
        final_conn = sqlite3.connect(db.db_path)
        deterrent_count = final_conn.execute("SELECT COUNT(*) FROM deterrent_devices").fetchone()[0]
        print(f"  Deterrent Devices: {deterrent_count}")
        marker_count = final_conn.execute("SELECT COUNT(*) FROM markers").fetchone()[0]
        print(f"  Markers: {marker_count}")
        polygon_count = final_conn.execute("SELECT COUNT(*) FROM polygons").fetchone()[0]
        print(f"  Polygons: {polygon_count}")
        img_meta_count = final_conn.execute("SELECT COUNT(*) FROM image_metadata").fetchone()[0]
        print(f"  Image Metadata Records: {img_meta_count}")
        bears_count = final_conn.execute("SELECT COUNT(*) FROM bears_tracking").fetchone()[0]
        print(f"  Bears Tracking Records: {bears_count}")
    except Exception as e_check:
        print(f"Error checking final counts directly: {e_check}")
    finally:
        if final_conn: final_conn.close()

# Run migration if executed directly (optional - can use migrate_data.py)
# if __name__ == "__main__":
#      migrate_csv_to_sqlite()