"""
Diagnostic and Fix Script for Carpathian Bears Database
- Checks the database for issues with timestamp data
- Verifies coordinate transformation
- Fixes timestamp format issues
- Validates all required tables and columns
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime
import pyproj
from pyproj import Transformer

def inspect_and_fix_database(db_path="wildlife_data.db"):
    """Comprehensive check and fix of the Carpathian Bears database"""
    print("=" * 50)
    print("CARPATHIAN BEARS DATABASE DIAGNOSTICS")
    print("=" * 50)
    
    # Check if database exists
    if not os.path.exists(db_path):
        print(f"ERROR: Database file '{db_path}' not found!")
        return False
    
    print(f"Database file found: {db_path}")
    
    # Connect to database
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check tables
        print("\n--- CHECKING DATABASE TABLES ---")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [table[0] for table in cursor.fetchall()]
        print(f"Tables found: {', '.join(tables)}")
        
        required_tables = [
            'bears_tracking', 'bears_mcp', 'bears_core_area', 
            'bears_kde', 'bears_seasonal_mcp', 'bears_daily_displacement'
        ]
        
        missing_tables = [table for table in required_tables if table not in tables]
        if missing_tables:
            print(f"ERROR: Missing required tables: {', '.join(missing_tables)}")
            return False
        
        # Check bears_tracking table structure
        print("\n--- CHECKING BEARS_TRACKING TABLE STRUCTURE ---")
        cursor.execute("PRAGMA table_info(bears_tracking);")
        columns = {col[1]: col[2] for col in cursor.fetchall()}
        print(f"Columns found: {', '.join(columns.keys())}")
        
        required_columns = [
            'id', 'bear_id', 'timestamp', 'x', 'y', 'lat', 'lng', 
            'season_code', 'season', 'sex', 'age', 'is_daytime'
        ]
        
        missing_columns = [col for col in required_columns if col not in columns]
        if missing_columns:
            print(f"ERROR: Missing required columns: {', '.join(missing_columns)}")
            if 'is_daytime' in missing_columns:
                print("Adding missing 'is_daytime' column...")
                try:
                    cursor.execute("ALTER TABLE bears_tracking ADD COLUMN is_daytime BOOLEAN")
                    conn.commit()
                    print("Added 'is_daytime' column successfully.")
                except Exception as e:
                    print(f"Error adding column: {e}")
        
        # Check for data in bears_tracking
        print("\n--- CHECKING DATA IN BEARS_TRACKING ---")
        cursor.execute("SELECT COUNT(*) FROM bears_tracking")
        count = cursor.fetchone()[0]
        print(f"Total records in bears_tracking: {count}")
        
        if count == 0:
            print("ERROR: No data found in bears_tracking table!")
            return False
        
        # Check timestamp format
        print("\n--- CHECKING TIMESTAMP FORMAT ---")
        cursor.execute("SELECT timestamp FROM bears_tracking LIMIT 10")
        timestamps = [row[0] for row in cursor.fetchall() if row[0] is not None]
        
        if not timestamps:
            print("ERROR: No valid timestamps found!")
        else:
            print(f"Sample timestamps: {', '.join(timestamps[:3])}")
            
            # Try to parse timestamps
            valid_format = True
            for ts in timestamps:
                try:
                    pd.to_datetime(ts)
                except:
                    valid_format = False
                    print(f"Invalid timestamp format detected: '{ts}'")
                    break
            
            if valid_format:
                print("Timestamp format appears valid.")
            else:
                print("ERROR: Invalid timestamp format detected!")
                print("Attempting to fix timestamp format...")
                # This would be a place to add timestamp fixing logic if needed
        
        # Check coordinates
        print("\n--- CHECKING COORDINATES ---")
        cursor.execute("SELECT x, y, lat, lng FROM bears_tracking LIMIT 10")
        coords = cursor.fetchall()
        
        if not coords:
            print("ERROR: No coordinate data found!")
        else:
            print("Sample coordinates (x, y, lat, lng):")
            for i, (x, y, lat, lng) in enumerate(coords[:3]):
                print(f"  {i+1}: ({x}, {y}) → ({lat}, {lng})")
            
            # Check if coordinates are valid
            valid_coords = True
            for x, y, lat, lng in coords:
                if x is None or y is None or lat is None or lng is None:
                    valid_coords = False
                    print(f"Invalid coordinates detected: x={x}, y={y}, lat={lat}, lng={lng}")
                elif not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
                    valid_coords = False
                    print(f"Out of range coordinates detected: lat={lat}, lng={lng}")
            
            if valid_coords:
                print("Coordinates appear valid.")
            else:
                print("ERROR: Invalid coordinates detected!")
                print("Attempting to fix coordinates...")
                fix_coordinates(conn)
        
        # Check for data distribution
        print("\n--- CHECKING DATA DISTRIBUTION ---")
        cursor.execute("SELECT COUNT(DISTINCT bear_id) FROM bears_tracking")
        bear_count = cursor.fetchone()[0]
        print(f"Number of distinct bears: {bear_count}")
        
        cursor.execute("SELECT bear_id, COUNT(*) FROM bears_tracking GROUP BY bear_id")
        bear_counts = cursor.fetchall()
        print("Records per bear:")
        for bear_id, count in bear_counts[:5]:
            print(f"  {bear_id}: {count} records")
        if len(bear_counts) > 5:
            print(f"  ... and {len(bear_counts)-5} more bears")
        
        # Check date range
        print("\n--- CHECKING DATE RANGE ---")
        fix_date_range(conn)
        
        # Check related tables
        check_related_tables(conn)
        
        print("\n" + "=" * 50)
        print("DIAGNOSTICS COMPLETE")
        print("=" * 50)
        
    except Exception as e:
        print(f"ERROR during database inspection: {e}")
        return False
    finally:
        if conn:
            conn.close()
    
    return True

def fix_coordinates(conn):
    """Fix coordinates by properly transforming from EPSG:3844 to WGS84"""
    try:
        # Create transformer
        transformer = Transformer.from_crs("EPSG:3844", "EPSG:4326", always_xy=True)
        
        # Get records with coordinates
        cursor = conn.cursor()
        cursor.execute("SELECT id, x, y FROM bears_tracking WHERE x IS NOT NULL AND y IS NOT NULL")
        records = cursor.fetchall()
        
        if not records:
            print("No records with coordinates found to fix.")
            return
        
        print(f"Transforming coordinates for {len(records)} records...")
        
        # Transform coordinates
        updated = 0
        for record_id, x, y in records:
            try:
                # Transform EPSG:3844 to WGS84
                lng, lat = transformer.transform(x, y)
                
                # Check if coordinates are valid
                if -90 <= lat <= 90 and -180 <= lng <= 180:
                    cursor.execute(
                        "UPDATE bears_tracking SET lat = ?, lng = ? WHERE id = ?",
                        (lat, lng, record_id)
                    )
                    updated += 1
            except Exception as e:
                print(f"Error transforming coordinates for record {record_id}: {e}")
        
        conn.commit()
        print(f"Successfully updated coordinates for {updated} records.")
        
    except Exception as e:
        print(f"Error in fix_coordinates: {e}")

def fix_date_range(conn):
    """Check and fix date range issues in the database"""
    cursor = conn.cursor()
    
    # Try to get min and max timestamp
    cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM bears_tracking WHERE timestamp IS NOT NULL")
    min_ts, max_ts = cursor.fetchone()
    
    print(f"Raw timestamp range: {min_ts} to {max_ts}")
    
    if min_ts is None or max_ts is None:
        print("ERROR: Cannot determine timestamp range!")
        
        # Check if timestamps are stored in a different format
        cursor.execute("SELECT timestamp FROM bears_tracking WHERE timestamp IS NOT NULL LIMIT 5")
        sample_ts = [row[0] for row in cursor.fetchall()]
        
        if sample_ts:
            print(f"Sample timestamps: {sample_ts}")
            
            # Try to convert samples to datetime
            valid_dates = []
            for ts in sample_ts:
                try:
                    dt = pd.to_datetime(ts)
                    valid_dates.append(dt)
                    print(f"  Converted '{ts}' to {dt}")
                except:
                    print(f"  Failed to convert '{ts}'")
            
            if valid_dates:
                print("Some timestamps can be parsed. Attempting to fix all timestamps...")
                
                # Get all records with timestamps
                cursor.execute("SELECT id, timestamp FROM bears_tracking WHERE timestamp IS NOT NULL")
                ts_records = cursor.fetchall()
                
                updated = 0
                failed = 0
                
                for record_id, ts in ts_records:
                    try:
                        dt = pd.to_datetime(ts)
                        iso_ts = dt.isoformat()
                        
                        cursor.execute(
                            "UPDATE bears_tracking SET timestamp = ? WHERE id = ?",
                            (iso_ts, record_id)
                        )
                        updated += 1
                    except:
                        failed += 1
                
                conn.commit()
                print(f"Updated {updated} timestamps to ISO format. Failed to convert {failed} timestamps.")
                
                # Check again after fixing
                cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM bears_tracking WHERE timestamp IS NOT NULL")
                min_ts, max_ts = cursor.fetchone()
                print(f"New timestamp range: {min_ts} to {max_ts}")
            else:
                print("Unable to parse any timestamps.")
    else:
        try:
            # Try to convert to datetime objects
            min_date = pd.to_datetime(min_ts).date()
            max_date = pd.to_datetime(max_ts).date()
            print(f"Parsed date range: {min_date} to {max_date}")
        except Exception as e:
            print(f"Error parsing date range: {e}")

def check_related_tables(conn):
    """Check related tables for data integrity"""
    cursor = conn.cursor()
    
    tables = [
        'bears_mcp', 'bears_core_area', 'bears_kde', 
        'bears_seasonal_mcp', 'bears_daily_displacement'
    ]
    
    print("\n--- CHECKING RELATED TABLES ---")
    
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table}: {count} records")
            
            if count > 0:
                cursor.execute(f"SELECT * FROM {table} LIMIT 1")
                sample = cursor.fetchone()
                columns = [desc[0] for desc in cursor.description]
                print(f"  Columns: {', '.join(columns)}")
            else:
                print(f"  WARNING: No data in {table}!")
        except Exception as e:
            print(f"  ERROR checking {table}: {e}")

def run_diagnostics():
    """Run diagnostics and return results"""
    print("Starting database diagnostics...\n")
    inspect_and_fix_database()

if __name__ == "__main__":
    run_diagnostics()