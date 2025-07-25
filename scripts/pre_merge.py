import pandas as pd

def pre_merge():
    """
    Deletes columns ending in '_y' and removes '_x' trailing from columns
    in data/adjusted/batters_home_weather_park.csv.
    """
    file_path = 'data/adjusted/batters_home_weather_park.csv'
    try:
        df = pd.read_csv(file_path)
        
        # Delete all columns ending in _y
        columns_to_drop_y = [col for col in df.columns if col.endswith('_y')]
        if columns_to_drop_y:
            df = df.drop(columns=columns_to_drop_y)
            print(f"Successfully deleted columns ending in '_y': {', '.join(columns_to_drop_y)}")
        else:
            print("No columns ending in '_y' were found.")

        # Delete the _x trailing in all columns
        new_columns = []
        for col in df.columns:
            if col.endswith('_x'):
                new_columns.append(col[:-2])  # Remove the last two characters (_x)
            else:
                new_columns.append(col)
        df.columns = new_columns
        print("Successfully removed '_x' trailing from column names.")

        df.to_csv(file_path, index=False)
        print(f"Modified DataFrame saved to {file_path}")

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    pre_merge()
