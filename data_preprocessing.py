import polars as pl
import os
from data_acquisition import get_jaguar_data

def clean_device_locations():
    # Read the Excel file
    excel_file = "data/device_data/Location Data.xlsx"
    df = pl.read_excel(excel_file)

    # Display the first few rows to see the structure
    print("First few rows of the data:")
    print(df.head())

    #create dataframe with excel coordinates 
    if 'Latitude and longitude' in df.columns:
        coordinates = df.select(['Latitude and longitude'])
        print("\nCoordinates:")
        print(coordinates)

    #removes null values
    coordinates_cleaned = coordinates.filter(pl.col("Latitude and longitude").is_not_null())
    print(coordinates_cleaned)

    #splits Latitude and longitude
    coordinates_split = coordinates_cleaned.with_columns([
        pl.col("Latitude and longitude").str.split(", ").list.get(0).alias("lat"),
        pl.col("Latitude and longitude").str.split(", ").list.get(1).alias("lng")
    ]).drop("Latitude and longitude")

    coordinates_split.write_csv("data/device_data/deterrent_devices.csv", separator=",")


def clean_jaguar_data():
    df = get_jaguar_data()
    df = df.rename({
        "event-id": "id",
        "location-long": "lng",
        "location-lat": "lat"
    })
    df.write_csv("data/animal_data/jaguar_rescue.csv", separator = ",")

clean_device_locations()
clean_jaguar_data()




