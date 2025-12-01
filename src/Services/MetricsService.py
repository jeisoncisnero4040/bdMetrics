from datetime import datetime
from src.Services.DatabaseService import DatabaseService
from src.Services.RedisService import RedisService
from src.Domain.QueryDomain import QueryDomain
from src.Domain.MetricsDomain import MetricsDomain
import pytz
from settings.AppSettings import TIMEZONE
from prometheus_client import CollectorRegistry, Gauge, generate_latest
import json


class MetricsService:
    def __init__(self, redis: RedisService, database: DatabaseService):
        self.redis = redis
        self.database = database
        self.timezone = pytz.timezone(TIMEZONE)


    def processRecord(self, db_name:str):
        snapshot: str = datetime.now(tz=self.timezone).isoformat()

        heavy_raw = self.database.getHeaviesQueries()
        freq_raw  = self.database.getMostRequestedQueries()
        queries   = self.database.getCurrentQueries()      
        users     = self.database.getCurrentUsers()
        memory    = self.database.getMemoryUsage()         


        # Normalización de consultas
        heavy = MetricsDomain.normalize_queries(heavy_raw, snapshot, QueryDomain.getMainTable)
        freq  = MetricsDomain.normalize_queries(freq_raw,  snapshot, QueryDomain.getMainTable)

        grouped_heavy = MetricsDomain.group_heavy_queries(heavy)
        grouped_freq  = MetricsDomain.group_frequent_queries(freq)

        current_snapshot = MetricsDomain.build_snapshot(grouped_heavy, grouped_freq, snapshot)

        # SNAPSHOT ANTERIOR
        last_snapshot_raw = self.redis.get_value("BaseContaLastMetrics")

        if not last_snapshot_raw:
            self.redis.set("BaseContaLastMetrics", json.dumps(current_snapshot))
            return "FIRST SNAPSHOT STORED"

        last_snapshot = json.loads(last_snapshot_raw)

        # DELTAS
        new_heavy = MetricsDomain.detect_new_tables(last_snapshot["heavy"], grouped_heavy)
        new_freq  = MetricsDomain.detect_new_tables(last_snapshot["frequent"], grouped_freq)

        heavy_deltas = MetricsDomain.calculate_deltas(last_snapshot["heavy"], grouped_heavy, new_heavy)
        freq_deltas  = MetricsDomain.calculate_deltas(last_snapshot["frequent"], grouped_freq, new_freq)

        # --------------------------------------------------------
        # REGISTRY PRINCIPAL PARA HEAVY Y FREQUENT
        # --------------------------------------------------------
        registry_main = CollectorRegistry()
        gauges_cache = {}

        # HEAVY EXPORT
        for table, metrics in heavy_deltas.items():
            for key, value in metrics.items():
                if key == "is_new_table":
                    continue

                gauge_name = f"db_heavy_{key}"

                if gauge_name not in gauges_cache:
                    gauges_cache[gauge_name] = Gauge(
                        gauge_name,
                        f"Métrica heavy {key}",
                        ["table", "snapshot", "is_new_table"],
                        registry=registry_main,
                    )

                gauges_cache[gauge_name].labels(
                    table=table,
                    snapshot=snapshot,
                    is_new_table=str(metrics["is_new_table"]),
                ).set(value)

        # FREQUENT EXPORT
        for table, metrics in freq_deltas.items():
            for key, value in metrics.items():
                if key == "is_new_table":
                    continue

                gauge_name = f"db_freq_{key}"

                if gauge_name not in gauges_cache:
                    gauges_cache[gauge_name] = Gauge(
                        gauge_name,
                        f"Métrica freq {key}",
                        ["table", "snapshot", "is_new_table"],
                        registry=registry_main,
                    )

                gauges_cache[gauge_name].labels(
                    table=table,
                    snapshot=snapshot,
                    is_new_table=str(metrics["is_new_table"]),
                ).set(value)

        # TEXTO PROMETHEUS DEL REGISTRY PRINCIPAL
        main_text = generate_latest(registry_main).decode("utf-8")

        # --------------------------------------------------------
        # REGISTRY PARA QUERIES PROCESSING
        # --------------------------------------------------------
        registry_queries = CollectorRegistry()
        
        gauge_q = Gauge(
            "db_current_queries",
            "Consultas ejecutándose ahora en SQL Server",
            ["snapshot"],
            registry=registry_queries
        )

        current_queries = queries[0]["queries_processing_now"]
        gauge_q.labels(snapshot=snapshot).set(current_queries)

        queries_text = generate_latest(registry_queries).decode("utf-8")
        self.redis.set("BaseContaQueriesProcessing", queries_text, 1200)

        # --------------------------------------------------------
        # REGISTRY PARA MEMORY USAGE
        # --------------------------------------------------------
        registry_memory = CollectorRegistry()

        mem = memory[0]

        for key, value in mem.items():
            g = Gauge(
                f"db_memory_{key}",
                f"Métrica de memoria SQL Server: {key}",
                ["snapshot"],
                registry=registry_memory
            )
            g.labels(snapshot=snapshot).set(value)

        memory_text = generate_latest(registry_memory).decode("utf-8")
        self.redis.set("BaseContaMemoryUsage", memory_text, 1200)

        # --------------------------------------------------------
        # GUARDAR SNAPSHOT PRINCIPAL E HISTORIAL
        # --------------------------------------------------------
        self.redis.set("BaseContaLastMetrics", json.dumps(current_snapshot))

        record_key = f"metrics:{db_name}:{snapshot}"
        self.redis.set(record_key, main_text, ttl=86400)
        self.redis.redis.rpush(f"metrics:index:{db_name}", record_key)

        # RETORNO FINAL CONCATENADO (3 REGISTRIES)
        return "\n".join([main_text, queries_text, memory_text])


    def fetchRecords(self):
        keys = self.redis.list_range("metrics:index:Baseconta")
        records = []

        for key in keys:
            raw = self.redis.get_value(key)
            if raw:
                records.append(raw)

        # Añadir los registries de memoria y queries processing
        records.append(self.redis.get_value("BaseContaQueriesProcessing"))
        records.append(self.redis.get_value("BaseContaMemoryUsage"))

        return "\n".join(filter(None, records))
