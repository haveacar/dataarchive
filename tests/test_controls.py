import unittest
from unittest.mock import patch
import pandas as pd
from controls import DataArchiveService


class TestDataArchiveService(unittest.TestCase):

    def setUp(self):
        n = 7
        self.test_data = {
            'bucket': ['ocr-videos-dev'] * n,
            'key': [
                'Tennis/205/589156314/Frames/589156314_20230307.png',
                'TableTennis/108/483957195/Frames/483957195_20230408.png',
                'Tennis/302/485739183/Frames/485739183_20230510.png',
                'SomeOtherSport/400/589156314/Frames/589156314_20230611.png',
                'Tennis/302/485739183/Frames/485739183_20230510.png',
                'ESports/Counter-Strike/ESportsBattle 2x2. Division 2/204/9654362/Frames/9654362_20230220003425.png',
                'Diagnostics/Responses/Volleyball/204/9394317/2022_12_12_07_31_25_response_content.txt',
            ],
            'size': [484941] * n,
            'last_modified_date': pd.date_range(start='2023-01-01', periods=n, freq='D'),
            'bucket_key_status': ['DISABLED'] * n
        }
        self.test_df = pd.DataFrame(self.test_data)

    @patch('pandas.read_parquet')
    @patch('random.sample')
    def test_get_representative_data(self, mock_random_sample, mock_read_parquet):
        mock_read_parquet.return_value = self.test_df
        mock_random_sample.side_effect = lambda x, k: x[:k]
        service = DataArchiveService('dummy_file.parquet')
        data = service.get_representative_data('Tennis', 3, 2)
        self.assertTrue(len(data) <= 3)

    @patch('pandas.read_parquet')
    def test_get_games_count_per_sport(self, mock_read_parquet):
        mock_read_parquet.return_value = self.test_df

        service = DataArchiveService('dummy_file.parquet')

        # Test count for Tennis (with spaces stripped)
        tennis_count = service.get_games_count_per_sport('Tennis'.strip())
        self.assertEqual(tennis_count, 3)  # Adjust this count based on your test data

        # Test count for a sport not in the DataFrame
        unknown_sport_count = service.get_games_count_per_sport('UnknownSport'.strip())
        self.assertEqual(unknown_sport_count, 0)

    @patch('pandas.read_parquet')
    def test_games_list(self, mock_read_parquet):
        mock_read_parquet.return_value = self.test_df

        # Create an instance of DataArchiveService
        service = DataArchiveService('dummy_file.parquet')

        # Calculate the expected games list based on the test data
        expected_games_list = {
            'Tennis': 3,
            'TableTennis': 1,
            'SomeOtherSport': 1,
            'ESports': 1
        }

        # Check if the generated games list matches the expected games list
        self.assertDictEqual(service.games_list, expected_games_list)

    if __name__ == '__main__':
        unittest.main()
