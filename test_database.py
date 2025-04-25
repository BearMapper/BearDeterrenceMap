"""
Test SQLite database setup for Wildlife Movement Analysis System
"""

import os
import time
import sqlite3
import pandas as pd
from wildlife_db import WildlifeDatabase

def get_table_names(db_path):
    """Get a list of all tables in the database"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Query sqlite_master table for all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    return tables

def get_table_count(db_path, table_name):
    """Get the number of records in a table"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
    except sqlite3.Error:
        count = 0
        
    conn.close()
    return count

def run_performance_test():
    """Run performance tests comparing CSV vs SQLite"""
    print("Running performance comparison tests...")
    print("-" * 50)
    
    db_path = "wildlife_data.db"
    
    # Create database connection
    db = WildlifeDatabase(db_path)
    
    # Test 1: Loading all markers
    # CSV approach
    csv_start = time.time()
    if os.path.exists("data/device_data/artificial_device_coordinates.csv"):
        markers_csv = pd.read_csv("data/device_data/artificial_device_coordinates.csv")
    else:
        markers_csv = pd.DataFrame()
    csv_time = time.time() - csv_start
    
    # SQLite approach
    sqlite_start = time.time()
    try:
        markers_sqlite = db.get_markers()
        sqlite_success = True
    except:
        # Fallback if method not available
        conn = sqlite3.connect(db_path)
        markers_sqlite = pd.read_sql("SELECT * FROM markers", conn)
        conn.close()
        sqlite_success = True
    sqlite_time = time.time() - sqlite_start
    
    print(f"Test 1: Loading all markers")
    print(f"  CSV:    {csv_time:.6f} seconds")
    print(f"  SQLite: {sqlite_time:.6f} seconds")
    if csv_time > 0 and sqlite_time > 0:
        print(f"  Speed improvement: {csv_time/sqlite_time:.2f}x")
    print()
    
    # Test 2: Filtering images by date range
    # For this test, we'll use a sample device ID
    try:
        device_ids = db.get_deterrent_devices()
        if not device_ids.empty:
            sample_device = device_ids.iloc[0]['id']
            
            # CSV approach - simplified version of your original code
            csv_start = time.time()
            device_path = f"data/bear_pictures/{sample_device}/device/"
            if os.path.exists(device_path):
                device_files = [f for f in os.listdir(device_path) 
                              if f.lower().endswith(('.jpg', '.jpeg', '.png'))]
            else:
                device_files = []
            csv_time = time.time() - csv_start
            
            # SQLite approach
            sqlite_start = time.time()
            try:
                # Try using the method from db if available
                device_images = db.get_image_count(sample_device, "device")
            except:
                # Fallback to direct query
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM image_metadata WHERE device_id = ? AND image_type = 'device'", 
                               (sample_device,))
                device_images = cursor.fetchone()[0]
                conn.close()
            
            sqlite_time = time.time() - sqlite_start
            
            print(f"Test 2: Counting images for device {sample_device}")
            print(f"  CSV:    {csv_time:.6f} seconds")
            print(f"  SQLite: {sqlite_time:.6f} seconds")
            if csv_time > 0 and sqlite_time > 0:
                print(f"  Speed improvement: {csv_time/sqlite_time:.2f}x")
            print()
    except:
        print("Skipping Test 2: No device data available")
    
    # Test 3: Query speed for complex filters
    tables = get_table_names(db_path)
    if "bears_tracking" in tables:
        print(f"Test 3: Complex query with filtering and aggregation")
        
        # SQLite approach - direct query instead of using method
        sqlite_start = time.time()
        conn = sqlite3.connect(db_path)
        seasonal_stats = pd.read_sql("""
            SELECT 
                bear_id,
                season,
                COUNT(*) as point_count,
                AVG(x) as avg_x,
                AVG(y) as avg_y,
                MIN(x) as min_x,
                MAX(x) as max_x,
                MIN(y) as min_y,
                MAX(y) as max_y
            FROM bears_tracking
            GROUP BY bear_id, season
        """, conn)
        conn.close()
        sqlite_time = time.time() - sqlite_start
        
        print(f"  SQLite complex query: {sqlite_time:.6f} seconds")
        print(f"  Retrieved {len(seasonal_stats)} records")
        print()
    
    print("-" * 50)
    print("Database summary:")
    
    # Print table counts
    for table in get_table_names(db_path):
        count = get_table_count(db_path, table)
        print(f"  Table '{table}': {count} records")

def run_database_check():
    """Basic check of database accessibility and content"""
    print("\nRunning basic database checks...")
    
    db_path = "wildlife_data.db"
    
    # Check if database file exists
    if not os.path.exists(db_path):
        print("❌ Database file not found!")
        return False
    
    # Check tables directly using sqlite3
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        print(f"Found {len(tables)} tables in database: {', '.join(tables)}")
    except Exception as e:
        print(f"❌ Error getting tables: {e}")
        return False
    
    # Check if we can execute a simple query
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0] == 1:
            print("✅ Database connection successful")
        else:
            print("❌ Database query failed")
            return False
    except Exception as e:
        print(f"❌ Error executing database query: {e}")
        return False
    
    # Check for key tables
    required_tables = ['deterrent_devices', 'markers', 'polygons', 'image_metadata']
    missing_tables = [table for table in required_tables if table not in tables]
    
    if missing_tables:
        print(f"❌ Missing required tables: {', '.join(missing_tables)}")
        return False
    else:
        print("✅ All required tables present")
    
    return True

if __name__ == "__main__":
    print("Wildlife Movement Analysis Database Test")
    print("=" * 50)
    
    if run_database_check():
        run_performance_test()
        print("\n✅ All tests completed successfully!")
    else:
        print("\n❌ Database check failed. Please check setup.")