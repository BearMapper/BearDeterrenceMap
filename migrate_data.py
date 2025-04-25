# migrate_data.py (Modified test_database function)

"""
Migrate data from CSV files to SQLite database

This script will:
1. Create a new SQLite database
2. Import data from all CSV files
3. Scan and index image files
"""

# Keep existing imports
from wildlife_db import WildlifeDatabase
import os
import pandas as pd
import time
import sqlite3 # Add direct sqlite3 import for testing

def migrate_all_data():
    """Migrate all data to SQLite with progress reporting"""
    print("Starting migration to SQLite...")

    # Create database instance for migration
    db = WildlifeDatabase("wildlife_data.db")

    # Define paths (makes it easier to manage)
    # *** IMPORTANT: Verify these paths are correct for your setup ***
    deterrent_csv = "data/device_data/deterrent_devices.csv"
    markers_csv = "data/device_data/artificial_device_coordinates.csv"
    polygons_csv = "data/areas/user_drawn_area_cities.csv"
    image_base_folder = "data/bear_pictures"
    bears_csv = "data/animal_data/carpathian_bears/processed/bears_processed.csv"

    # Step 1: Import deterrent devices
    print("\n[1/5] Importing deterrent devices...")
    # Pass the correct csv path to the function
    if db.import_deterrent_devices(csv_path=deterrent_csv):
        print("✓ Deterrent devices import step finished.")
    else:
        print("✗ Deterrent devices import step failed.")

    # Step 2: Import markers
    print("\n[2/5] Importing custom markers...")
    # Pass the correct csv path to the function
    if db.import_markers(csv_path=markers_csv):
         print("✓ Markers import step finished.")
    else:
         print("✗ Markers import step failed.")


    # Step 3: Import polygons
    print("\n[3/5] Importing areas/polygons...")
    # Pass the correct csv path to the function
    if db.import_polygons(csv_path=polygons_csv):
         print("✓ Polygons import step finished.")
    else:
         print("✗ Polygons import step failed.")


    # Step 4: Index image files
    print("\n[4/5] Indexing image files (this may take a while)...")
    # Pass the correct base folder and reindex flag
    img_count = db.index_image_files(base_folder=image_base_folder, reindex=True)
    if img_count >= 0:
         print(f"✓ Image indexing step finished ({img_count} images indexed).")
    else:
         print("✗ Image indexing step failed.")


    # Step 5: Import Carpathian bears data
    print("\n[5/5] Importing Carpathian bears data...")
    # Pass the correct csv path to the function
    if db.import_bears_tracking_data(csv_path=bears_csv):
         print("✓ Bears tracking data import step finished.")
    else:
         print("✗ Bears tracking data import step failed.")

    print("\nMigration complete! Database saved as wildlife_data.db")
    print("\nYou can now update your application to use the database.")


def test_database():
    """Run basic tests to verify database functionality"""
    print("\nRunning database tests...")
    db_path = "wildlife_data.db" # Define db path for direct connection
    db = WildlifeDatabase(db_path) # Instance for using class methods

    # --- Direct Check for Deterrent Devices ---
    direct_conn = None # Initialize connection variable
    try:
        print(f"--- Directly checking {db_path} ---")
        direct_conn = sqlite3.connect(db_path)
        cursor = direct_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM deterrent_devices")
        direct_count = cursor.fetchone()[0]
        print(f"Direct Query Result: Found {direct_count} records in 'deterrent_devices' table.")
        # Optional: Select a few records to see their format
        if direct_count > 0:
            cursor.execute("SELECT id, directory_name, lat, lng FROM deterrent_devices LIMIT 3")
            sample_records = cursor.fetchall()
            print("Direct Query Sample:")
            for record in sample_records:
                print(f"  {record}")
    except Exception as e_direct:
        print(f"Direct Query Error: Failed to query deterrent_devices table directly: {e_direct}")
    finally:
        if direct_conn:
            direct_conn.close() # Ensure connection is closed
    print("--- End Direct Check ---")

    # Test 1: Check deterrent devices (using the class method again for comparison)
    deterrents = db.get_deterrent_devices()
    print(f"Class Method Result: Found {len(deterrents)} deterrent devices")

    # Test 2: Check markers
    markers = db.get_markers()
    print(f"Found {len(markers)} custom markers")

    # Test 3: Check polygons
    polygons = db.get_polygons()
    print(f"Found {len(polygons)} areas/polygons")

    # Test 4: Check image counts
    # Need to handle case where deterrents DataFrame is empty
    if not deterrents.empty:
        # Use try-except in case the 'id' column doesn't exist as expected
        try:
            sample_device = deterrents['id'].iloc[0]
            device_images = db.get_image_count(sample_device, "device")
            trail_images = db.get_image_count(sample_device, "trail")
            print(f"Sample device {sample_device}: {device_images} device images, {trail_images} trail images")
        except (KeyError, IndexError) as e_sample:
            print(f"Could not get sample device ID for image count check: {e_sample}")
    elif direct_count > 0: # Check if direct count found records even if class method didn't
         print("Skipping sample image count check as class method returned 0 devices (though direct check found some).")
    else:
        print("Skipping sample image count check as no deterrent devices were found.")


    # Test 5: Check bears data
    try:
        # Check if bears data exists before calling unique()
        bear_data_df = db.get_bear_movement_by_season()
        if not bear_data_df.empty and 'bear_id' in bear_data_df.columns:
             unique_bears = bear_data_df['bear_id'].nunique()
             print(f"Found movement data for {unique_bears} bears")
        else:
             print("No bear movement data found or 'bear_id' column missing.")
    except Exception as e:
        print(f"Error testing bear data: {e}")

    print("\nTests complete")


if __name__ == "__main__":
    # Check if user wants to migrate or test
    action = input("Choose action: [m]igrate data, [t]est database, or [b]oth: ").lower()

    if action.startswith('m') or action.startswith('b'):
        migrate_all_data() # Runs migration

    # Test phase should always run after migration if 'b' is chosen
    if action.startswith('t') or action.startswith('b'):
        test_database()    # Runs tests