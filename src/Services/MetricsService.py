from datetime import datetime
from src.Services.DatabaseService import DatabaseService
from src.Services.RedisService import RedisService
from src.Domain.QueryDomain import QueryDomain
import pytz
from settings.AppSettings import TIMEZONE
from src.Domain.DeltaDomain import DeltaDomain
from prometheus_client import CollectorRegistry, Gauge, generate_latest


class MetricsService:
    def __init__(self, redis: RedisService, database: DatabaseService):
        self.redis = redis
        self.database = database
        self.timezone = pytz.timezone(TIMEZONE)

    def processRecord(self, db_name: str):
        heavy = self.database.getHeaviesQuery()       
        frequent = self.database.getMostRequestedQueries()   
        snapshot_time = datetime.now(tz=self.timezone).isoformat()

    # Añadimos tabla principal y snapshot al objeto
        for item in heavy:
            item["main_table"] = QueryDomain.getMainTable(item["query_text"])
            item["snapshot"] = snapshot_time

        for item in frequent:
            item["main_table"] = QueryDomain.getMainTable(item["query_text"])
            item["snapshot"] = snapshot_time

        registry = CollectorRegistry()

        # DEFINICIÓN DE MÉTRICAS (TODAS CON SNAPSHOT)
        queries_total = Gauge(
            'sql_queries_total',
            'Total de consultas ejecutadas',
            ['database', 'table', 'snapshot'],
            registry=registry
        )

        cpu_total = Gauge(
            'sql_query_cpu_total',
            'Tiempo total de CPU usado por query',
            ['database', 'query', 'table', 'snapshot'],
            registry=registry
        )

        duration_total = Gauge(
            'sql_query_duration_total',
            'Duración total de la query en milisegundos',
            ['database', 'query', 'table', 'snapshot'],
            registry=registry
        )

        logical_reads_total = Gauge(
            'sql_query_logical_reads_total',
            'Lecturas lógicas totales de la query',
            ['database', 'query', 'table', 'snapshot'],
            registry=registry
        )

        logical_writes_total = Gauge(
            'sql_query_logical_writes_total',
            'Escrituras lógicas totales de la query',
            ['database', 'query', 'table', 'snapshot'],
            registry=registry
        )

        physical_reads_total = Gauge(
            'sql_query_physical_reads_total',
            'Lecturas físicas totales de la query',
            ['database', 'query', 'table', 'snapshot'],
            registry=registry
        )

        exec_per_second = Gauge(
            'sql_query_exec_per_second',
            'Ejecuciones aproximadas por segundo de la query',
            ['database', 'query', 'table', 'snapshot'],
            registry=registry
        )

        # APLICAMOS SNAPSHOT EN LOS SET()
        for query in frequent:
            table = query['main_table']
            q_text = query['query_text']

            queries_total.labels(
                database=db_name, table=table, snapshot=snapshot_time
            ).set(query['execution_count'])

            cpu_total.labels(
                database=db_name, query=q_text, table=table, snapshot=snapshot_time
            ).set(query.get('cpu_time_total', 0))

            duration_total.labels(
                database=db_name, query=q_text, table=table, snapshot=snapshot_time
            ).set(query.get('duration_total', 0))

            logical_reads_total.labels(
                database=db_name, query=q_text, table=table, snapshot=snapshot_time
            ).set(query.get('logical_reads_total', 0))

            logical_writes_total.labels(
                database=db_name, query=q_text, table=table, snapshot=snapshot_time
            ).set(query.get('logical_writes_total', 0))

            physical_reads_total.labels(
                database=db_name, query=q_text, table=table, snapshot=snapshot_time
            ).set(query.get('physical_reads_total', 0))

            exec_per_second.labels(
                database=db_name, query=q_text, table=table, snapshot=snapshot_time
            ).set(query.get('exec_per_second', 0))

        for query in heavy:
            table = query['main_table']
            q_text = query['query_text']

            cpu_total.labels(
                database=db_name, query=q_text, table=table, snapshot=snapshot_time
            ).set(query.get('cpu_time_total', 0))

            duration_total.labels(
                database=db_name, query=q_text, table=table, snapshot=snapshot_time
            ).set(query.get('duration_total', 0))

            logical_reads_total.labels(
                database=db_name, query=q_text, table=table, snapshot=snapshot_time
            ).set(query.get('logical_reads_total', 0))

            logical_writes_total.labels(
                database=db_name, query=q_text, table=table, snapshot=snapshot_time
            ).set(query.get('logical_writes_total', 0))

            physical_reads_total.labels(
                database=db_name, query=q_text, table=table, snapshot=snapshot_time
            ).set(query.get('physical_reads_total', 0))

        prometheus_text = generate_latest(registry).decode('utf-8')

        # Guardamos en Redis con clave del snapshot
        key = self.redis.saveRecord(db_name, prometheus_text)
        return key

    def fetchRecords(self):
        """
        Devuelve todas las métricas guardadas en Redis en formato text/plain listo para Prometheus
        """
        records = self.redis.getRecords('Baseconta')
        # records ya es una lista de strings, podemos unirlos en un solo bloque
        return "\n".join(records)

