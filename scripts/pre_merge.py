# pre_merge.py
import pandas as pd

def pre_merge():
    """
    Deletes 'location', 'stadium', 'notes', and 'precipitation' columns
    from data/end_chain/batters_home_weather_park.csv
    and saves the modified DataFrame back to the same path.
    """
    file_path = 'data/end_chain/batters_home_weather_park.csv'
    try:
        df = pd.read_csv(file_path)
        
        columns_to_delete = ['location', 'stadium', 'notes', 'precipitation']
        
        # Filter out columns that don't exist to prevent errors
        existing_columns_to_delete = [col for col in columns_to_delete if col in df.columns]
        
        if existing_columns_to_delete:
            df = df.drop(columns=existing_columns_to_delete)
            df.to_csv(file_path, index=False)
            print(f"Successfully deleted columns: {', '.join(existing_columns_to_delete)} from {file_path}")
        else:
            print(f"None of the specified columns ({', '.join(columns_to_delete)}) were found in {file_path}")

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    pre_merge()
