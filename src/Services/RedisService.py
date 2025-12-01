from src.Utils.RedisConection import RedisConection
from redis import Redis
from typing import List, Optional, Union


class RedisService:

    def __init__(self, con: RedisConection):
        self.redis: Redis = con.getConn()

    # ---------------------------
    #        STRING METHODS
    # ---------------------------
    def set(self, key: str, value: str, ttl: Optional[int] = None):
        if ttl:
            return self.redis.setex(key, ttl, value)
        return self.redis.set(key, value)

    def get_value(self, key: str) -> Optional[str]:
        raw = self.redis.get(key)
        if raw is None:
            return None
        return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)

    # ---------------------------
    #          LIST METHODS
    # ---------------------------
    def list_push(self, key: str, value: str):
        return self.redis.rpush(key, value)

    def list_range(self, key: str, start: int = 0, end: int = -1) -> List[str]:
        raw_items = self.redis.lrange(key, start, end)
        if not raw_items:
            return []
        return [
            item.decode("utf-8") if isinstance(item, bytes) else str(item)
            for item in raw_items
        ]

    def get_list(self, key: str) -> List[str]:
        return self.list_range(key, 0, -1)

    def list_trim(self, key: str, max_items: int):
        return self.redis.ltrim(key, -max_items, -1)

    # ---------------------------
    #        KEY UTILITIES
    # ---------------------------
    def delete(self, key: str) -> int:
        return self.redis.delete(key)

    def exists(self, key: str) -> bool:
        return self.redis.exists(key) == 1

    def keys(self, pattern: str) -> List[str]:
        raw_keys = self.redis.keys(pattern)
        return [k.decode("utf-8") for k in raw_keys] if raw_keys else []

    # ---------------------------
    #       SMART AUTO GET
    # ---------------------------
    def get_auto(self, key: str) -> Union[str, List[str], None]:
        """
        Detecta automáticamente si la clave es string o lista.
        """
        type_raw = self.redis.type(key)

        if type_raw == b'string':
            return self.get_value(key)

        if type_raw == b'list':
            return self.get_list(key)

        return None  # sets, hashes, zsets, nada, etc

    # ---------------------------
    #          PIPELINE
    # ---------------------------
    def pipeline(self):
        return self.redis.pipeline()
