from datetime import datetime
from random import randint
from math import floor
from redis.client import Redis

from redisolar.dao.base import RateLimiterDaoBase
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.dao.redis.key_schema import KeySchema
from redisolar.dao.base import RateLimitExceededException


class SlidingWindowRateLimiter(RateLimiterDaoBase, RedisDaoBase):
    """A sliding-window rate-limiter."""
    def __init__(self,
                 window_size_ms: float,
                 max_hits: int,
                 redis_client: Redis,
                 key_schema: KeySchema = None,
                 **kwargs):
        self.window_size_ms = window_size_ms
        self.max_hits = max_hits
        super().__init__(redis_client, key_schema, **kwargs)

    def _get_key(self, name: str) -> str:
        """
        Returns the key for the sliding window rate-limiter.

        The key is based on the name of the rate-limiter and
        the window size in milliseconds.
        """
        return self.key_schema.sliding_window_rate_limiter_key(name, self.window_size_ms, self.max_hits)

    def hit(self, name: str):
        """Record a hit using the rate-limiter."""
        # START Challenge #7
        key = self._get_key(name)
        pipeline = self.redis.pipeline(transaction=False)
        current_time = datetime.now().timestamp() * 1000

        pipeline.zadd(key, {f"{current_time}-{randint(1,999)}": current_time}, 1)
        pipeline.zremrangebyscore(key, 0, current_time - self.window_size_ms)
        pipeline.zcard(key)
        pipeline.expire(key, floor(self.window_size_ms / 1000) + 1)

        hits = pipeline.execute()[-2]

        if hits > self.max_hits:
            raise RateLimitExceededException()
        # END Challenge #7
