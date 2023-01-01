import functools
import sys
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

import pandas as pd

if sys.version_info < (3, 10):  # pragma: no cover
    from typing_extensions import ParamSpec
else:  # pragma: no cover
    from typing import ParamSpec

P = ParamSpec("P")


def timed_lru_cache(
        seconds: Optional[int] = None, maxsize: Optional[int] = None, typed: bool = True
) -> Callable[[Callable[P, pd.DataFrame]], Callable[P, pd.DataFrame]]:
    """Decorator-factory that generates a decorator that caches a pandas DataFrame in memory (with some expiration time)
    for faster retrieval in the future.

    Args:
        seconds: Number of seconds to retain the cache.
        maxsize: Maximum number of items to store in the cache.
        typed: Whether arguments of different types will be cached separately.

    Returns:
        Decorator that caches a pandas DataFrame in memory (with an expiration time).
    """

    def cache_decorator(func: Callable[P, pd.DataFrame]) -> Callable[P, pd.DataFrame]:
        """Function decorator that caches its function's returned DataFrame to memory."""
        func = functools.lru_cache(maxsize=maxsize, typed=typed)(func)

        if seconds is None:
            return func

        func.lifetime = timedelta(seconds=seconds)
        func.expiration = datetime.now(timezone.utc) + func.lifetime

        @functools.wraps(func)
        def retrieve_or_cache(*args: P.args, **kwargs: P.kwargs) -> pd.DataFrame:
            """Retrieves the cached DataFrame, or if it doesn't exist/needs to be refreshed, calls the function and
            caches its result.
            """
            if datetime.now(timezone.utc) >= func.expiration:
                func.cache_clear()
                func.expiration = datetime.now(timezone.utc) + func.lifetime

            return func(*args, **kwargs)

        return retrieve_or_cache

    return cache_decorator
