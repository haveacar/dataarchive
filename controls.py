import os
import random
import sys
import pandas as pd


class DataArchiveService:
    def __init__(self, file_name: str):
        # Initialize DataFrame
        self.df = self._read_parquet_file(file_name)

        # Extract sport and game ID from 'key' column
        self.df['sport'] = self.df['key'].apply(lambda x: x.split('/')[0])
        self.df['game_id'] = self.df['key'].apply(lambda x: x.split('/')[-3])

        # Clean columns with none value
        self.clean_bad_columns()

        # Create a dictionary to store unique game IDs for each sport
        self.sport_game_ids = self.df.groupby('sport')['game_id'].unique().to_dict()

    def _read_parquet_file(self, file_name: str) -> pd.DataFrame:
        """
        Reads a Parquet file and returns its contents as a pandas DataFrame.
        :param file_name: The name of the Parquet file to be read.
        :return:  A DataFrame containing the data from the Parquet file.
        """
        file_path = os.path.join(os.path.dirname(__file__), file_name)
        try:
            return pd.read_parquet(file_path, engine='pyarrow')
        except (FileNotFoundError, ValueError, Exception) as e:
            print(f"Error reading Parquet file: {e}")
            return pd.DataFrame()

    def clean_bad_columns(self, clean_method='drop'):
        """
        Cleans the DataFrame by removing rows containing bad data or filling missing values
        :param clean_method: The method to clean the DataFrame. Can be 'drop' to remove bad rows,
        or 'fill' to fill missing values with zeros.
        :return: None
        """
        if self.df.empty or 'key' not in self.df.columns:
            print("DataFrame is empty or missing 'key' column. No cleaning performed.")
            return

        # Remove rows where 'sport' is an empty string
        self.df = self.df[self.df['sport'] != '']

        if clean_method == 'drop':
            self.df.dropna(axis=0, inplace=True)
            pattern = '|'.join(['Diagnostics', 'Test'])
            self.df = self.df[~self.df['key'].str.contains(pattern, na=False)]
        elif clean_method == 'fill':
            self.df.fillna(0, inplace=True)
        else:
            print("Invalid clean method specified. No cleaning performed.")

    def get_games_count_per_sport(self, sport: str) -> int:
        """
        Get the count of games for a specific sport category.
        :param sport: The name of the sport category to retrieve game count for.
        :return: The count of games for the specified sport category.
        """
        return len(self.sport_game_ids.get(sport, []))

    def get_representative_data(self, sportName: str, frameCount: int, fixturesCount: int) -> list[str]:
        """
        Get representative data for a specific sport category.
        :param sportName: The name of the sport category to retrieve data for.
        :param frameCount: The number of frames or data points to retrieve.
        :param fixturesCount: The maximum number of fixtures to consider.
        :return: List of representative data frames.
        """

        # Get unique game IDs for the sport from the pre-processed dictionary
        unique_game_ids = self.sport_game_ids.get(sportName, [])

        # Randomly select games, ensuring not to exceed the available count
        selected_game_ids = random.sample(list(unique_game_ids), min(fixturesCount, len(unique_game_ids)))

        # Filter DataFrame for the selected games
        filtered_df = self.df[(self.df['sport'] == sportName) & (self.df['game_id'].isin(selected_game_ids))]

        # Get all representative keys
        representative_data = filtered_df['key'].values.tolist()

        # Randomly select frames if necessary
        if frameCount < len(representative_data):
            representative_data = random.sample(representative_data, frameCount)

        return representative_data

    def check_processing_data(self) -> str:
        """Check if all game IDs are numeric and have a length greater than 5"""

        # check valid format
        valid_format = self.df['game_id'].apply(lambda x: x.isnumeric() and len(x) > 5).all()

        # count all games per sport
        unique_sport_count = self.df.groupby('sport')['game_id'].nunique()

        if valid_format:
            return f"All game IDs are in the correct format.\nCount all games per: {unique_sport_count}"
        else:
            return "Some game IDs are not in the correct format."


if __name__ == "__main__":
    sys.exit()
