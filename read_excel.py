import polars as pl

# Read the Excel file
excel_file = 'ReadMe.xlsx'
df = pl.read_excel("/Users/maroloro/Desktop/Studium/Bachelorarbeit/Location Data.xlsx")

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
    pl.col("Latitude and longitude").str.split(", ").list.get(0).alias("Latitude"),
    pl.col("Latitude and longitude").str.split(", ").list.get(1).alias("Longitude")
]).drop("Latitude and longitude")

print(coordinates_split)

# If your columns have different names, you might need to adjust the code above
# You can check all column names with:
#print("\nAll column names in the file:")
#print(df.columns)