import redis

from settings.DataBaseSetting import REDIS_PORT,REDIS_SERVER


class RedisConection:
    
    def __init__(self ):
        self.con=redis.Redis(
            host=REDIS_SERVER,
            port=REDIS_PORT,
            db=0,
            decode_responses=True,
            password=None)
    def getConn(self):
        return self.con
    


