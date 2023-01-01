# cache-pandas
Easily cache outputs of functions that generate pandas DataFrames to file or in memory. Useful for data science projects where a large DataFrame is generated, but will not change for some time, so it could be cached to file. The next time the script runs, it will use the cached version. 


## Caching pandas dataframes to csv file
`cache-pandas` includes the decorator `cache_to_csv`, which will cache the result of a function (returning a DataFrame) to a csv file. The next time the function or script is run, it will take that cached file, instead of calling the function again. 

An optional expiration time can also be set. This might be useful for a webscraper where the output DataFrame may change once a day, but within the day, it will be the same. If the decorated function is called after the specified cache expiration, the DataFrame will be regenerated.

### Example

The following example will cache the resulting DataFrame to `file.csv`. It will regenerate the DataFrame and its cache if the function is called a second time atleast 100 seconds after the first.

```python
from cache_pandas import cache_to_csv

@cache_to_csv("file.csv", refresh_time=100)
def sample_constant_function() -> pd.DataFrame:
    """Sample function that returns a constant DataFrame, for testing purpose."""
    data = {
        "ints": list(range(NUM_SAMPLES)),
        "strs": [str(i) for i in range(NUM_SAMPLES)],
        "floats": [float(i) for i in range(NUM_SAMPLES)],
    }

    return pd.DataFrame.from_dict(data)
```

### Args
```
filepath: Filepath to save the cached CSV.
refresh_time: Time seconds. If the file has not been updated in longer than refresh_time, generate the file
    anew. If `None`, the file will never be regenerated if a cached version exists.
create_dirs: Whether to create necessary directories containing the given filepath.
```


## Caching pandas dataframes to memory
`cache-pandas` includes the decorator `timed_lru_cache`, which will cache the result of a function (returning a DataFrame) to a memory, using `functools.lru_cache`.

An optional expiration time can also be set. This might be useful for a webscraper where the output DataFrame may change once a day, but within the day, it will be the same. If the decorated function is called after the specified cache expiration, the DataFrame will be regenerated.

### Example

The following example will cache the resulting DataFrame in memory. It will regenerate the DataFrame and its cache if the function is called a second time atleast 100 seconds after the first.

```python
from cache_pandas import timed_lru_cache

@timed_lru_cache(seconds=100, maxsize=None)
def sample_constant_function() -> pd.DataFrame:
    """Sample function that returns a constant DataFrame, for testing purpose."""
    data = {
        "ints": list(range(NUM_SAMPLES)),
        "strs": [str(i) for i in range(NUM_SAMPLES)],
        "floats": [float(i) for i in range(NUM_SAMPLES)],
    }

    return pd.DataFrame.from_dict(data)
```

### Args
```
seconds: Number of seconds to retain the cache.
maxsize: Maximum number of items to store in the cache. See `functools.lru_cache` for more details.
typed: Whether arguments of different types will be cached separately. See `functools.lru_cache` for more details.
```

## Composing `cache_to_csv` and `timed_lru_cache`
`cache_to_csv` and `timed_lru_cache` can even be composed together. Usually the correct way to do this is to wrap `timed_lru_cache` last, because `cache_to_csv` will always check the file before calling the function. The refresh time can even differ between the two caching mechanisms.

### Example
```python
from cache_pandas import timed_lru_cache, cache_to_csv

@timed_lru_cache(seconds=100, maxsize=None)
@cache_to_csv("file.csv", refresh_time=100)
def sample_constant_function() -> pd.DataFrame:
    """Sample function that returns a constant DataFrame, for testing purpose."""
    data = {
        "ints": list(range(NUM_SAMPLES)),
        "strs": [str(i) for i in range(NUM_SAMPLES)],
        "floats": [float(i) for i in range(NUM_SAMPLES)],
    }

    return pd.DataFrame.from_dict(data)
```
