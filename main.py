from flask import Flask, Response
from apscheduler.schedulers.background import BackgroundScheduler
from pytz import timezone
from src.Utils.RedisConection import RedisConection
from src.Services.MetricsService import MetricsService
from src.Services.DatabaseService import DatabaseService
from src.Services.RedisService import RedisService
from src.Utils.DatabaseConnection import DatabaseConnection
from src.Repositories.BdRepository import BdRepository
from settings.AppSettings import TIMEZONE
from src.Services.PrometheusService import PrometheusService

app = Flask(__name__)


databaseConnection = DatabaseConnection()
bdRepo = BdRepository(db_connection=databaseConnection)
database_service = DatabaseService(repo=bdRepo)
redis_service = RedisService(RedisConection())
prometheus=PrometheusService()
metrics_service = MetricsService(redis=redis_service, database=database_service,prometheus=prometheus)


scheduler = BackgroundScheduler()

def execute_metrics_job():
    metrics_service.processRecord("Baseconta")


scheduler.add_job(
    execute_metrics_job,
    trigger="interval",
    minutes=1,
    timezone=timezone(TIMEZONE)  
)
scheduler.start()


@app.get("/metrics")
def getmetrics():
    """Retorna datos listos para Grafana (JSON API datasource)."""
    data = metrics_service.fetchRecords()   
    return Response(data, mimetype="text/plain")

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000,debug=True, use_reloader=False)
