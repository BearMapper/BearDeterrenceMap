import polars as pl
import os
from data_acquisition import get_jaguar_data
import pytesseract
from PIL import Image, ImageFilter
import re
from datetime import datetime
import shutil

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

clean_device_locations()
#clean_jaguar_data()

base_directory = 'data/bear_pictures'
process_trail_folders(base_directory)




