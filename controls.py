import os
import random
import sys

import pandas as pd


class DataArchiveService:
    def __init__(self, file_name: str):

        self.df = self._read_parquet_file(file_name)
        self._games_list = None
        self.clean_bad_columns()

    def _read_parquet_file(self, file_name: str) -> pd.DataFrame:
        file_path = os.path.join(os.path.dirname(__file__), file_name)
        try:
            return pd.read_parquet(file_path, engine='pyarrow')
        except (FileNotFoundError, ValueError, Exception) as e:
            print(f"Error reading Parquet file: {e}")
            return pd.DataFrame()

    @property
    def games_list(self):
        if self._games_list is None:
            self._games_list = self._generate_games_list()
        return self._games_list

    def _generate_games_list(self) -> dict:

        sport_count = self.df['key'].str.split('/', n=1).str[0].str.strip().value_counts().to_dict()
        return sport_count

    def clean_bad_columns(self, clean_method='drop'):
        if self.df.empty or 'key' not in self.df.columns:
            print("DataFrame is empty or missing 'key' column. No cleaning performed.")
            return

        if clean_method == 'drop':
            self.df.dropna(axis=0, inplace=True)
            pattern = '|'.join(['Diagnostics', 'Test'])
            self.df = self.df[~self.df['key'].str.contains(pattern, na=False)]
        elif clean_method == 'fill':
            self.df.fillna(0, inplace=True)
        else:
            print("Invalid clean method specified. No cleaning performed.")

    def get_games_count_per_sport(self, sport: str) -> int:

        sport_rows = self.df[self.df['key'].str.startswith(sport + '/')]
        return len(sport_rows)

    def get_representative_data(self, sportName: str, frameCount: int, fixturesCount: int) -> list[str]:

        # Filter the DataFrame for rows related to the specified sport
        sport_df = self.df[self.df['key'].str.startswith(sportName + '/')]

        # Extracting the game IDs from the 'key' column and getting unique IDs
        unique_game_ids = sport_df['key'].str.split('/').str[2].dropna().unique()

        # Randomly select Y unique games, ensuring not to exceed the available count
        selected_game_ids = random.sample(list(unique_game_ids), min(fixturesCount, len(unique_game_ids)))

        selected_games_frames = []
        representative_data = []

        for game_id in selected_game_ids:
            # Filter DataFrame for the selected game
            game_df = sport_df[sport_df['key'].str.contains(f'/{game_id}/')]
            # get all game frames
            game_frames = game_df['key'].values.tolist()
            selected_games_frames.extend(game_frames)

        if frameCount < len(selected_games_frames):
            representative_data = random.sample(selected_games_frames, frameCount)

        return representative_data


if __name__ == "__main__":
    sys.exit()
