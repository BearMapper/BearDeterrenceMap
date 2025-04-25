"""
Database access layer for Carpathian Bears data visualization
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime
import numpy as np

class CarpathianBearsDB:
    """Database handler for Carpathian Bears tracking data"""
    
    def __init__(self, db_path="wildlife_data.db"):
        """Initialize the database connection"""
        self.db_path = db_path
    
    def get_bears_list(self):
        """Get a list of all bears in the database"""
        conn = sqlite3.connect(self.db_path)
        query = """
        SELECT DISTINCT 
            bear_id, 
            sex,
            age,
            COUNT(*) as point_count
        FROM bears_tracking
        GROUP BY bear_id, sex, age
        ORDER BY bear_id
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    
    def get_bear_data(self, bear_id=None, start_date=None, end_date=None, season=None):
        """
        Get tracking data for one or all bears with filtering options
        
        Args:
            bear_id: Optional bear ID to filter
            start_date: Optional start date
            end_date: Optional end date
            season: Optional season filter
            
        Returns:
            DataFrame with tracking data
        """
        conn = sqlite3.connect(self.db_path)
        
        # Build query with parameters
        query = "SELECT * FROM bears_tracking WHERE 1=1"
        params = []
        
        if bear_id:
            query += " AND bear_id = ?"
            params.append(bear_id)
            
        if start_date:
            query += " AND timestamp >= ?"
            params.append(start_date.isoformat())
            
        if end_date:
            query += " AND timestamp <= ?"
            params.append(end_date.isoformat())
            
        if season:
            query += " AND season = ?"
            params.append(season)
            
        query += " ORDER BY bear_id, timestamp"
        
        df = pd.read_sql(query, conn, params=params)
        conn.close()
        
        # Convert timestamp strings to datetime
        if not df.empty and 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
        return df
    
    def get_seasonal_movement(self):
        """Get bear movement data aggregated by season"""
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
            COUNT(DISTINCT date(timestamp)) as day_count
        FROM bears_tracking
        GROUP BY bear_id, season, sex, age
        ORDER BY bear_id, season
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    
    def get_daily_movement(self, bear_id=None):
        """
        Calculate daily movement statistics
        
        This efficiently calculates distance and speed metrics
        for each bear on each day using SQLite
        """
        conn = sqlite3.connect(self.db_path)
        
        # First get all points grouped by bear and day
        if bear_id:
            query = """
            SELECT 
                bear_id, 
                date(timestamp) as date,
                COUNT(*) as num_points,
                AVG(x) as centroid_x,
                AVG(y) as centroid_y,
                MIN(x) as min_x,
                MAX(x) as max_x,
                MIN(y) as min_y,
                MAX(y) as max_y,
                sex,
                age,
                season
            FROM bears_tracking
            WHERE bear_id = ?
            GROUP BY bear_id, date(timestamp)
            ORDER BY bear_id, date(timestamp)
            """
            df = pd.read_sql(query, conn, params=[bear_id])
        else:
            query = """
            SELECT 
                bear_id, 
                date(timestamp) as date,
                COUNT(*) as num_points,
                AVG(x) as centroid_x,
                AVG(y) as centroid_y,
                MIN(x) as min_x,
                MAX(x) as max_x,
                MIN(y) as min_y,
                MAX(y) as max_y,
                sex,
                age,
                season
            FROM bears_tracking
            GROUP BY bear_id, date(timestamp)
            ORDER BY bear_id, date(timestamp)
            """
            df = pd.read_sql(query, conn)
        
        conn.close()
        
        # Now calculate distances and speeds
        # We need to fetch additional data for this since SQLite lacks advanced functions
        result = []
        
        for bear in df['bear_id'].unique():
            bear_days = df[df['bear_id'] == bear]
            
            for _, day_row in bear_days.iterrows():
                date_str = day_row['date']
                bear_id = day_row['bear_id']
                
                # We'll calculate total distance and average speed for days with enough points
                if day_row['num_points'] > 1:
                    # Fetch all points for this bear on this day, ordered by timestamp
                    conn = sqlite3.connect(self.db_path)
                    points_query = """
                    SELECT x, y, timestamp
                    FROM bears_tracking
                    WHERE bear_id = ? AND date(timestamp) = ?
                    ORDER BY timestamp
                    """
                    points = pd.read_sql(points_query, conn, params=[bear_id, date_str])
                    conn.close()
                    
                    # Calculate distances between consecutive points
                    distances = []
                    for i in range(1, len(points)):
                        prev = points.iloc[i-1]
                        curr = points.iloc[i]
                        
                        # Euclidean distance
                        dist = np.sqrt((curr['x'] - prev['x'])**2 + (curr['y'] - prev['y'])**2)
                        distances.append(dist)
                    
                    total_distance = sum(distances)
                    avg_speed = total_distance / len(distances) if distances else 0
                else:
                    total_distance = 0
                    avg_speed = 0
                
                # Create a record with all the information
                record = day_row.to_dict()
                record['total_distance'] = total_distance
                record['avg_speed'] = avg_speed
                
                result.append(record)
        
        # Convert to DataFrame
        return pd.DataFrame(result)
    
    def get_home_range_data(self):
        """
        Simulate home range data retrieval
        
        In a full implementation, this would retrieve home range data from a separate table
        For now, we'll return an example dataframe with the expected structure
        """
        # In a real implementation, you would query the database:
        # conn = sqlite3.connect(self.db_path)
        # df = pd.read_sql("SELECT * FROM bear_home_ranges", conn)
        # conn.close()
        
        # For demonstration, create a sample dataframe with the expected structure
        # In the real implementation, this would be based on actual database data
        
        # Get unique bears first
        conn = sqlite3.connect(self.db_path)
        bears = pd.read_sql("SELECT DISTINCT bear_id, sex, age FROM bears_tracking", conn)
        conn.close()
        
        # Create sample home range data
        data = []
        seasons = ["Winter sleep", "Den exit and reproduction", "Forest fruits", "Hyperphagia"]
        
        for _, bear in bears.iterrows():
            # Create a base record for this bear
            base_record = {
                'bear_id': bear['bear_id'],
                'sex': bear['sex'],
                'age': bear['age'],
                'home_range_area': np.random.uniform(10, 100),  # Random area
                'stage': 'Adult' if bear['age'] == 'adult' else 'Sub-Adult'
            }
            
            # Add areas for each season
            for season in seasons:
                base_record[season] = np.random.uniform(5, 50)
                
            # Find largest season
            max_season = max(seasons, key=lambda s: base_record[s])
            base_record['largest_season'] = max_season
            base_record['largest_season_area'] = base_record[max_season]
            
            data.append(base_record)
            
        return pd.DataFrame(data)
    
    def create_bears_tables(self):
        """
        Create tables for Carpathian bears data
        
        This would be used in a full implementation to create additional tables
        beyond the basic tracking data table
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create home range table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS bears_home_range (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bear_id TEXT,
            season TEXT,
            area REAL,
            sex TEXT,
            age TEXT
        )
        ''')
        
        # Create daily movement stats table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS bears_daily_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bear_id TEXT,
            date TEXT,
            num_points INTEGER,
            total_distance REAL,
            avg_speed REAL,
            centroid_x REAL,
            centroid_y REAL,
            season TEXT
        )
        ''')
        
        conn.commit()
        conn.close()

    def get_date_range(self, table_name, date_column):
        """
        Get the minimum and maximum date from a specific column in a table.

        Args:
            table_name: The name of the table (e.g., 'bears_tracking').
            date_column: The name of the column containing dates/timestamps (e.g., 'timestamp').

        Returns:
            A tuple (min_date, max_date) as date objects, or (None, None) if error/no data.
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            # Ensure column name is safe if coming from external source
            query = f"SELECT MIN({date_column}), MAX({date_column}) FROM {table_name}"
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            # Convert string dates from DB to datetime objects then to date objects
            if result and result[0] and result[1]:
                 # Assuming the timestamp column stores strings like 'YYYY-MM-DD HH:MM:SS'
                 min_date = pd.to_datetime(result[0]).date()
                 max_date = pd.to_datetime(result[1]).date()
                 return min_date, max_date
            else:
                 # Return None if no records found or dates are NULL
                 return None, None
        except sqlite3.Error as e:
            print(f"Database error in get_date_range: {e}")
            return None, None
        except Exception as e: # Catch other potential errors like pd.to_datetime conversion
            print(f"Error processing dates in get_date_range: {e}")
            return None, None
        finally:
            if conn:
                conn.close()

    def get_distinct_values(self, table_name, column_name):
        """
        Get distinct values from a specific column in a table.

        Args:
            table_name: The name of the table.
            column_name: The name of the column.

        Returns:
            A list of distinct values, or an empty list if an error occurs or no values found.
        """
        conn = None # Initialize conn to None
        try:
            conn = sqlite3.connect(self.db_path)
            # Use parameter binding for table and column names safely if possible,
            # but standard SQL does not support placeholders for identifiers (table/column names).
            # Ensure table_name and column_name are validated or controlled internally if they come from user input.
            # For this internal use case, direct string formatting is generally acceptable,
            # assuming table_name and column_name are not from untrusted sources.
            query = f"SELECT DISTINCT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL"
            cursor = conn.cursor()
            cursor.execute(query)
            # fetchall() returns a list of tuples, e.g., [('Spring',), ('Summer',)]
            # We need to extract the first element from each tuple.
            distinct_values = [item[0] for item in cursor.fetchall()]
            return distinct_values
        except sqlite3.Error as e:
            print(f"Database error in get_distinct_values: {e}")
            return [] # Return empty list on error
        finally:
            if conn:
                conn.close()

# Example usage
if __name__ == "__main__":
    db = CarpathianBearsDB()
    bears = db.get_bears_list()
    print(f"Found {len(bears)} bears in the database:")
    print(bears)
    
    if len(bears) > 0:
        sample_bear = bears.iloc[0]['bear_id']
        print(f"\nSample data for bear {sample_bear}:")
        data = db.get_bear_data(sample_bear, limit=5)
        print(data)
        
        print("\nSeasonal movement patterns:")
        seasonal = db.get_seasonal_movement()
        print(seasonal)