import pandas as pd

def add_stadium_column(file_path='data/weather_adjustments.csv'):
    """
    Adds a 'stadium' column to the CSV file, mirroring the 'venue' column.

    Args:
        file_path (str): The path to the CSV file.
    """
    try:
        df = pd.read_csv(file_path)

        if 'venue' not in df.columns:
            print(f"Error: 'venue' column not found in {file_path}. Please ensure the column exists.")
            return

        df['stadium'] = df['venue']
        df.to_csv(file_path, index=False)
        print(f"Successfully added 'stadium' column to {file_path}, mirroring 'venue' column.")

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found. Please ensure the path is correct.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    add_stadium_column()
