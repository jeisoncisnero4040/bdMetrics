from src.Utils.RedisConection import RedisConection
from redis import Redis
from datetime import datetime, timezone

class RedisService:
    def __init__(self, con: RedisConection):
        self.redis: Redis = con.getConn()

    def saveRecord(self, db_name: str, payload_text: str, ttl: int = 86400):
        """
        Guarda el payload ya convertido a text/plain (Prometheus exposition format)
        """
        timestamp = datetime.now(timezone.utc).isoformat()
        key = f"metrics:{db_name}:{timestamp}"
        self.redis.set(key, payload_text, ex=ttl)
        self.redis.rpush(f"metrics:index:{db_name}", key)

        return key

    def getRecords(self, db_name: str) -> list[str]:
        keys = self.redis.lrange(f"metrics:index:{db_name}", 0, -1)
        records = []

        for key in keys:
            raw = self.redis.get(key)
            if raw:
                records.append(raw)

        return records
