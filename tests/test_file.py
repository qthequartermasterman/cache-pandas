import unittest
from unittest import mock

import pandas as pd

from cache_pandas.file import cache_to_csv

NUM_SAMPLES = 10
number_of_times_called = 0


def sample_constant_function() -> pd.DataFrame:
    """Sample function that returns a constant DataFrame, for testing purpose."""
    data = {
        "ints": list(range(NUM_SAMPLES)),
        "strs": [str(i) for i in range(NUM_SAMPLES)],
        "floats": [float(i) for i in range(NUM_SAMPLES)],
    }

    return pd.DataFrame.from_dict(data)


def sample_random_dataframe() -> pd.DataFrame:
    """Sample function that returns a new DataFrame each time it is called, for testing purpose."""
    global number_of_times_called
    number_of_times_called += 1

    numbers = [1000 * number_of_times_called + i for i in range(NUM_SAMPLES)]

    data = {
        "ints": numbers,
        "strs": [str(i) for i in numbers],
        "floats": [float(i) for i in numbers],
    }

    return pd.DataFrame.from_dict(data)


class TestCacheToCSV(unittest.TestCase):
    def setUp(self) -> None:
        self.filepath = "sample.csv"

    @mock.patch("pandas.DataFrame.to_csv")
    def test_caches_file_if_not_exists(self, mock_csv: mock.MagicMock) -> None:
        wrapped_func = cache_to_csv(self.filepath)(sample_constant_function)

        actual_df = wrapped_func()
        expected_df = sample_constant_function()

        pd.testing.assert_frame_equal(actual_df, expected_df)
        mock_csv.assert_called_once_with(self.filepath)
