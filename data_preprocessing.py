import polars as pl
import pandas as pd
import os
from data_acquisition import get_jaguar_data
import pytesseract
from PIL import Image, ImageFilter
import re
from datetime import datetime
import shutil
import numpy as np

def clean_device_locations():
    # Read the Excel file
    excel_file = "data/device_data/Location Data.xlsx"
    df = pl.read_excel(excel_file)

    # Display the first few rows to see the structure
    print("First few rows of the data:")
    print(df.head())

    #create dataframe with excel coordinates 
    if 'Latitude and longitude' and 'Directory name' in df.columns:
        coordinates = df.select(['Latitude and longitude', 'Directory name'])
        print("\nCoordinates:")
        print(coordinates)

    #removes null values
    coordinates_cleaned = coordinates.filter(pl.col("Latitude and longitude").is_not_null())
    print(coordinates_cleaned)

    #splits Latitude and longitude
    coordinates_split = coordinates_cleaned.with_columns([
        pl.col("Directory name"),
        pl.col("Latitude and longitude").str.split(", ").list.get(0).alias("lat"),
        pl.col("Latitude and longitude").str.split(", ").list.get(1).alias("lng")
    ]).drop("Latitude and longitude")

    coordinates_split.write_csv("data/device_data/deterrent_devices.csv", separator=",")

def extract_text_from_image(image_path):
    """
    Extracts text from the central horizontal strip of the bottom 1/16th
    of an image, applying sharpening.

    Args:
        image_path (str): The path to the image file.

    Returns:
        str: The extracted text, or None if an error occurs.
    """
    try:
        img = Image.open(image_path)
        width, height = img.size

        # --- Crop the bottom portion (1/16th of height) ---
        bottom_height = height // 16
        lower_left_bottom = (0, height - bottom_height)
        upper_right_bottom = (width, height)
        bottom_cropped_img = img.crop((lower_left_bottom[0], lower_left_bottom[1],
                                         upper_right_bottom[0], upper_right_bottom[1]))

        # --- Crop the central horizontal strip (width 900 to 1450) from the bottom cropped image ---
        bottom_cropped_width, bottom_cropped_height = bottom_cropped_img.size
        left = 900
        right = 1450
        upper = 0
        lower = bottom_cropped_height

        # Ensure the coordinates are within the image bounds of the bottom cropped image
        if left < 0:
            left = 0
        if right > bottom_cropped_width:
            right = bottom_cropped_width

        central_cropped_img = bottom_cropped_img.crop((left, upper, right, lower))

        # Increase sharpness
        sharpened_img = central_cropped_img.filter(ImageFilter.SHARPEN)

        # Tell Tesseract to expect digits, '/', ':', and space (you can uncomment this if needed)
        # custom_config = r'--oem 3 --psm 6 -c tessedit_char_whitelist=0123456789/: '
        text = pytesseract.image_to_string(sharpened_img).strip()
        return text
    except FileNotFoundError:
        print(f"Error: Image not found at {image_path}")
        return None
    except pytesseract.TesseractNotFoundError:
        print(f"Error: Tesseract is not installed or not in your system's PATH.")
        print("Please make sure Tesseract is installed and configured correctly.")
        return None
    except Exception as e:
        print(f"Error occurred while processing {image_path}: {e}")
        return None

def process_trail_folders(base_folder):
    """
    Traverses the base folder and extracts text from images in "trail" subfolders.
    If the extracted text contains the pattern<\ctrl3348>/MM/DD HH:MM:SS and the date is in the past,
    it copies the original image to a "trail_processed" folder with a new filename,
    correcting "90" in the year to "20".
    If extraction or parsing fails, it still copies the image with "unsuccessful_parsing" in the name.

    Args:
        base_folder (str): The path to the main folder (e.g., "bear_pictures").
    """
    for root, dirs, files in os.walk(base_folder):
        if os.path.basename(root) == "trail":
            print(f"Processing folder: {root}")
            trail_processed_folder = os.path.join(os.path.dirname(root), "trail_processed")
            os.makedirs(trail_processed_folder, exist_ok=True)

            for file in files:
                if file.lower().endswith(('.png', '.jpg', '.jpeg')):
                    image_path = os.path.join(root, file)
                    print(f"  --- Processing image: {file} ---")
                    extracted_text = extract_text_from_image(image_path)
                    print(f"  Extracted text: '{extracted_text}'")
                    parsing_successful = False

                    if extracted_text:
                        print(f"  Checking for date pattern in: '{extracted_text}'")
                        date_match = re.search(r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", extracted_text)
                        if date_match:
                            date_str = date_match.group(1)
                            print(f"  Found date pattern: '{date_str}'")

                            # Correct "90" year to "20"
                            if date_str.startswith("90"):
                                print(f"  Correcting year from '90' to '20' in: '{date_str}'")
                                date_str = "20" + date_str[2:]
                                print(f"  Corrected date string: '{date_str}'")

                            try:
                                print(f"  Attempting to parse date: '{date_str}'")
                                extracted_date = datetime.strptime(date_str, "%Y/%m/%d %H:%M:%S")
                                now = datetime(2025, 4, 4, 7, 50, 15) # Using the current time

                                if extracted_date < now:
                                    new_filename_past = extracted_date.strftime("%Y.%m.%d.%H%M") + os.path.splitext(file)[1].lower()
                                    destination_path_past = os.path.join(trail_processed_folder, new_filename_past)
                                    shutil.copy2(image_path, destination_path_past)
                                    print(f"  Past date '{date_str}' found. Copied to {new_filename_past}")
                                    parsing_successful = True
                                else:
                                    print(f"  Date '{date_str}' is not in the past.")
                                    parsing_successful = True # Consider parsing successful even if not in the past
                            except ValueError:
                                print(f"  Error parsing date '{date_str}'.")
                        else:
                            print(f"  Could not find pattern 'YYYY/MM/DD HH:MM:SS' in extracted text.")
                    else:
                        print(f"  Text extraction failed for this image.")

                    if not parsing_successful:
                        original_name, ext = os.path.splitext(file)
                        new_filename_unsuccessful = f"{original_name}_unsuccessful_parsing{ext.lower()}"
                        destination_path_unsuccessful = os.path.join(trail_processed_folder, new_filename_unsuccessful)
                        shutil.copy2(image_path, destination_path_unsuccessful)
                        print(f"  Copying {file} to {new_filename_unsuccessful} due to unsuccessful parsing.")
                    print("  --- Finished processing image ---")


def clean_jaguar_data():
    df = get_jaguar_data()
    df = df.rename({
        "event-id": "id",
        "location-long": "lng",
        "location-lat": "lat"
    })
    df.write_csv("data/animal_data/jaguar_rescue.csv", separator = ",")

#clean_device_locations()
#clean_jaguar_data()

#base_directory = 'data/bear_pictures'
#process_trail_folders(base_directory)

#########################
# Carpathian Bears Data Preprocessing Functions
#########################

def preprocess_carpathian_bears_data():
    """
    Preprocess raw Carpathian bears data for visualization.
    This function only needs to be run once when new data is added.
    
    It performs the following operations:
    1. Converts raw GPS data to standardized format
    2. Creates aggregated datasets for quick visualization
    3. Calculates movement metrics (speed, distance)
    4. Processes home range data
    """
    print("Starting Carpathian bears data preprocessing...")
    
    # Create output directory if it doesn't exist
    output_dir = "data/animal_data/carpathian_bears/processed"
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Process main tracking data
        process_bears_tracking_data(output_dir)
        
        # Process home range data
        process_bears_home_range_data(output_dir)
        
        # Process distance data
        process_bears_distance_data(output_dir)
        
        # Create interpolated paths
        create_bears_interpolated_paths(output_dir)
        
        print("Carpathian bears data preprocessing completed successfully!")
        return True
    except Exception as e:
        print(f"Error during Carpathian bears data preprocessing: {e}")
        return False

def process_bears_tracking_data(output_dir):
    """Process and transform the main bear tracking data"""
    print("Processing bears tracking data...")
    
    # Load bears data
    bears_file = "data/animal_data/carpathian_bears/1_bears_RO.csv"
    if not os.path.exists(bears_file):
        print(f"Error: {bears_file} not found")
        return False
    
    bears_data = pd.read_csv(bears_file)
    
    # Convert timestamps to datetime
    bears_data['timestamp'] = pd.to_datetime(bears_data['timestamp'])
    
    # Add derived fields
    bears_data['date'] = bears_data['timestamp'].dt.date
    bears_data['hour'] = bears_data['timestamp'].dt.hour
    bears_data['month'] = bears_data['timestamp'].dt.month
    bears_data['year'] = bears_data['timestamp'].dt.year
    bears_data['day_of_year'] = bears_data['timestamp'].dt.dayofyear
    bears_data['is_daytime'] = bears_data['hour'].between(6, 18)
    
    # Save processed data
    bears_data.to_csv(f"{output_dir}/bears_processed.csv", index=False)
    print(f"Saved processed bears data to {output_dir}/bears_processed.csv")
    
    # Create daily aggregations
    create_bears_daily_aggregations(bears_data, output_dir)
    
    # Create monthly aggregations
    create_bears_monthly_aggregations(bears_data, output_dir)
    
    return True

def create_bears_daily_aggregations(bears_data, output_dir):
    """Create daily movement aggregations for each bear"""
    print("Creating daily bear movement aggregations...")
    
    # Group by bear and date
    daily_groups = []
    
    # Get unique bears and dates
    bears = bears_data['Name'].unique()
    dates = pd.to_datetime(bears_data['timestamp']).dt.date.unique()
    
    for bear in bears:
        bear_data = bears_data[bears_data['Name'] == bear]
        
        for date in dates:
            date_data = bear_data[pd.to_datetime(bear_data['timestamp']).dt.date == date]
            
            if len(date_data) > 0:
                # Calculate statistics
                record = {
                    'bear_id': bear,
                    'date': date,
                    'num_points': len(date_data),
                    'centroid_x': date_data['X'].mean(),
                    'centroid_y': date_data['Y'].mean(),
                    'min_x': date_data['X'].min(),
                    'max_x': date_data['X'].max(),
                    'min_y': date_data['Y'].min(),
                    'max_y': date_data['Y'].max(),
                    'sex': date_data['Sex'].iloc[0],
                    'age': date_data['Age'].iloc[0],
                    'season': date_data['Season2'].iloc[0]
                }
                
                # Calculate movement distance if there are enough points
                if len(date_data) > 1:
                    # Sort by timestamp
                    sorted_points = date_data.sort_values('timestamp')
                    
                    # Calculate distances between consecutive points
                    distances = []
                    for i in range(1, len(sorted_points)):
                        prev = sorted_points.iloc[i-1]
                        curr = sorted_points.iloc[i]
                        
                        # Euclidean distance
                        dist = np.sqrt((curr['X'] - prev['X'])**2 + (curr['Y'] - prev['Y'])**2)
                        distances.append(dist)
                    
                    record['total_distance'] = sum(distances)
                    record['avg_speed'] = sum(distances) / len(distances) if distances else 0
                else:
                    record['total_distance'] = 0
                    record['avg_speed'] = 0
                
                daily_groups.append(record)
    
    # Convert to DataFrame
    daily_df = pd.DataFrame(daily_groups)
    
    # Save to CSV
    daily_df.to_csv(f"{output_dir}/bears_daily.csv", index=False)
    print(f"Saved daily bear movements to {output_dir}/bears_daily.csv")

def create_bears_monthly_aggregations(bears_data, output_dir):
    """Create monthly movement aggregations for each bear"""
    print("Creating monthly bear movement aggregations...")
    
    # Add year-month column
    bears_data['year_month'] = bears_data['timestamp'].dt.strftime('%Y-%m')
    
    # Group by bear and year-month
    monthly_stats = []
    
    for bear in bears_data['Name'].unique():
        bear_data = bears_data[bears_data['Name'] == bear]
        
        for year_month in bear_data['year_month'].unique():
            month_data = bear_data[bear_data['year_month'] == year_month]
            
            # Calculate statistics
            stats = {
                'bear_id': bear,
                'year_month': year_month,
                'num_points': len(month_data),
                'num_days': len(month_data['date'].unique()),
                'most_common_season': month_data['Season2'].mode()[0] if not month_data['Season2'].empty else None,
                'sex': month_data['Sex'].iloc[0],
                'age': month_data['Age'].iloc[0]
            }
            
            monthly_stats.append(stats)
    
    # Convert to DataFrame
    monthly_df = pd.DataFrame(monthly_stats)
    
    # Save to CSV
    monthly_df.to_csv(f"{output_dir}/bears_monthly.csv", index=False)
    print(f"Saved monthly bear statistics to {output_dir}/bears_monthly.csv")

def process_bears_home_range_data(output_dir):
    """Process home range data for visualization"""
    print("Processing bears home range data...")
    
    # Process HR kernels data
    hr_file = "data/animal_data/carpathian_bears/5_HR_kernels_bears_1.csv"
    if os.path.exists(hr_file):
        hr_data = pd.read_csv(hr_file)
        
        # Save processed version
        hr_data.to_csv(f"{output_dir}/bears_home_ranges.csv", index=False)
        print(f"Saved home range data to {output_dir}/bears_home_ranges.csv")
    else:
        print(f"Warning: {hr_file} not found")
    
    # Process seasonal home range data
    seasonal_hr_file = "data/animal_data/carpathian_bears/6_mcphrs_all_seasons.csv"
    if os.path.exists(seasonal_hr_file):
        seasonal_hr_data = pd.read_csv(seasonal_hr_file)
        
        # Map season codes to names for clarity
        season_map = {
            'I': 'Winter sleep',
            'II': 'Den exit and reproduction',
            'III': 'Forest fruits',
            'IV': 'Hyperphagia'
        }
        
        # Add season name column if 'Season' column exists
        if 'Season' in seasonal_hr_data.columns:
            seasonal_hr_data['SeasonName'] = seasonal_hr_data['Season'].map(
                lambda x: season_map.get(str(x), str(x))
            )
        
        # Save processed version
        seasonal_hr_data.to_csv(f"{output_dir}/bears_seasonal_home_ranges.csv", index=False)
        print(f"Saved seasonal home range data to {output_dir}/bears_seasonal_home_ranges.csv")
    else:
        print(f"Warning: {seasonal_hr_file} not found")
    
    # Create a summary dataset combining both
    if os.path.exists(hr_file) and os.path.exists(seasonal_hr_file):
        create_home_range_summary(hr_data, seasonal_hr_data, output_dir)

def create_home_range_summary(hr_data, seasonal_hr_data, output_dir):
    """Create a summary dataset with home range statistics"""
    print("Creating home range summary...")
    
    # Map season codes to names
    season_map = {
        'I': 'Winter sleep',
        'II': 'Den exit and reproduction',
        'III': 'Forest fruits',
        'IV': 'Hyperphagia'
    }
    
    # Initialize results list
    summary = []
    
    # Process each bear
    for _, bear_row in hr_data.iterrows():
        if pd.isna(bear_row['id']):
            continue
            
        bear_id = bear_row['id']
        
        # Get seasonal data for this bear
        bear_seasons = seasonal_hr_data[seasonal_hr_data['id'] == bear_id]
        
        # Initialize record with bear info
        record = {
            'bear_id': bear_id,
            'sex': bear_row['Sex'],
            'age': bear_row['Age'],
            'home_range_area': bear_row['area'],
            'stage': bear_row.get('Stage', 'Unknown'),
            'Winter sleep': 0,
            'Den exit and reproduction': 0,
            'Forest fruits': 0,
            'Hyperphagia': 0
        }
        
        # Add seasonal areas
        for _, season_row in bear_seasons.iterrows():
            season_code = season_row['Season']
            season_name = season_map.get(str(season_code), str(season_code))
            record[season_name] = season_row['Area']
        
        # Find largest season
        max_season = None
        max_area = -1
        
        for season_name in season_map.values():
            if record[season_name] > max_area:
                max_area = record[season_name]
                max_season = season_name
        
        record['largest_season'] = max_season
        record['largest_season_area'] = max_area
        
        summary.append(record)
    
    # Convert to DataFrame and save
    summary_df = pd.DataFrame(summary)
    summary_df.to_csv(f"{output_dir}/bears_home_range_summary.csv", index=False)
    print(f"Saved home range summary to {output_dir}/bears_home_range_summary.csv")

def process_bears_distance_data(output_dir):
    """Process distance data for visualization"""
    print("Processing bears distance data...")
    
    # Load distance data
    dist_file = "data/animal_data/carpathian_bears/7_dist_bears_RO.csv"
    if not os.path.exists(dist_file):
        print(f"Warning: {dist_file} not found")
        return
    
    dist_data = pd.read_csv(dist_file)
    
    # Convert date strings to datetime
    dist_data['date'] = pd.to_datetime(dist_data['date'])
    
    # Map season codes to names
    season_map = {
        'I': 'Winter sleep',
        'II': 'Den exit and reproduction',
        'III': 'Forest fruits',
        'IV': 'Hyperphagia'
    }
    
    # Add season name column
    dist_data['SeasonName'] = dist_data['Season'].map(
        lambda x: season_map.get(str(x), str(x))
    )
    
    # Save processed data
    dist_data.to_csv(f"{output_dir}/bears_distances.csv", index=False)
    print(f"Saved distance data to {output_dir}/bears_distances.csv")
    
    # Create aggregated statistics
    # Group by bear and season
    bear_season_stats = []
    
    for bear in dist_data['Name'].unique():
        bear_data = dist_data[dist_data['Name'] == bear]
        
        for season in bear_data['Season'].unique():
            season_data = bear_data[bear_data['Season'] == season]
            
            # Calculate statistics
            stats = {
                'bear_id': bear,
                'season': season,
                'season_name': season_map.get(str(season), str(season)),
                'count': len(season_data),
                'total_distance': season_data['dist'].sum(),
                'avg_distance': season_data['dist'].mean(),
                'min_distance': season_data['dist'].min(),
                'max_distance': season_data['dist'].max(),
                'avg_altitude': season_data['alt'].mean() if 'alt' in season_data.columns else None
            }
            
            bear_season_stats.append(stats)
    
    # Convert to DataFrame
    stats_df = pd.DataFrame(bear_season_stats)
    
    # Save to CSV
    stats_df.to_csv(f"{output_dir}/bears_distance_stats.csv", index=False)
    print(f"Saved distance statistics to {output_dir}/bears_distance_stats.csv")

def create_bears_interpolated_paths(output_dir):
    """
    Create interpolated paths between sparse GPS points for smoother visualization
    """
    print("Creating interpolated paths...")
    
    # Load processed tracking data
    processed_file = f"{output_dir}/bears_processed.csv"
    if not os.path.exists(processed_file):
        print(f"Error: {processed_file} not found. Run tracking data processing first.")
        return
    
    processed_data = pd.read_csv(processed_file, parse_dates=['timestamp'])
    
    # Group data by bear
    interpolated_data = []
    
    for bear in processed_data['Name'].unique():
        # Get data for this bear
        bear_data = processed_data[processed_data['Name'] == bear].copy()
        
        # Sort by timestamp
        bear_data = bear_data.sort_values('timestamp')
        
        # Iterate through points
        for i in range(len(bear_data) - 1):
            current = bear_data.iloc[i]
            next_point = bear_data.iloc[i + 1]
            
            # Add the current point as non-interpolated
            interpolated_data.append({
                'bear_id': bear,
                'original_id': current['id'],
                'x': current['X'],
                'y': current['Y'],
                'timestamp': current['timestamp'],
                'is_interpolated': False,
                'season': current['Season2'],
                'sex': current['Sex'],
                'age': current['Age']
            })
            
            # Calculate time difference in hours
            time_diff = (next_point['timestamp'] - current['timestamp']).total_seconds() / 3600
            
            # If gap is large (>2 hours) but not too large (<24 hours), add interpolated points
            if 2 < time_diff < 24:
                steps = int(time_diff)  # One point per hour
                
                for step in range(1, steps):
                    fraction = step / steps
                    
                    # Linear interpolation
                    interp_x = current['X'] + (next_point['X'] - current['X']) * fraction
                    interp_y = current['Y'] + (next_point['Y'] - current['Y']) * fraction
                    interp_time = current['timestamp'] + pd.Timedelta(hours=step)
                    
                    interpolated_data.append({
                        'bear_id': bear,
                        'original_id': f"{current['id']}_interp_{step}",
                        'x': interp_x,
                        'y': interp_y,
                        'timestamp': interp_time,
                        'is_interpolated': True,
                        'season': current['Season2'],
                        'sex': current['Sex'],
                        'age': current['Age']
                    })
        
        # Add the final point
        final_point = bear_data.iloc[-1]
        interpolated_data.append({
            'bear_id': bear,
            'original_id': final_point['id'],
            'x': final_point['X'],
            'y': final_point['Y'],
            'timestamp': final_point['timestamp'],
            'is_interpolated': False,
            'season': final_point['Season2'],
            'sex': final_point['Sex'],
            'age': final_point['Age']
        })
    
    # Convert to DataFrame
    interp_df = pd.DataFrame(interpolated_data)
    
    # Save to CSV
    interp_df.to_csv(f"{output_dir}/bears_interpolated_paths.csv", index=False)
    print(f"Saved interpolated paths to {output_dir}/bears_interpolated_paths.csv")

# Main execution
if __name__ == "__main__":
    # Uncomment which preprocessing functions to run
    # clean_device_locations()
    # clean_jaguar_data()
    # base_directory = 'data/bear_pictures'
    # process_trail_folders(base_directory)
    
    # Process Carpathian bears data
    preprocess_carpathian_bears_data()


