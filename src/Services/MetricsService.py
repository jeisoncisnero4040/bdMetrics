from datetime import datetime
from src.Services.DatabaseService import DatabaseService
from src.Services.RedisService import RedisService
from src.Domain.QueryDomain import QueryDomain
from src.Domain.MetricsDomain import MetricsDomain
import pytz
from settings.AppSettings import TIMEZONE
from src.Services.PrometheusService import PrometheusService
import json


class MetricsService:
    def __init__(self, redis: RedisService, database: DatabaseService, prometheus: PrometheusService):
        self.redis = redis
        self.database = database
        self.prometheus = prometheus
        self.timezone = pytz.timezone(TIMEZONE)

    # ------------------- Procesamiento principal -------------------
    def processRecord(self, db_name: str):
        snapshot = self._get_current_snapshot()

        heavy_raw, freq_raw, queries, users, memory = self._fetch_db_data()
        grouped_heavy, grouped_freq = self._normalize_and_group(heavy_raw, freq_raw, snapshot)

        current_snapshot = MetricsDomain.build_snapshot(grouped_heavy, grouped_freq, snapshot)
        last_snapshot = self._get_last_snapshot()
        if not last_snapshot:
            self.redis.set("BaseContaLastMetrics", json.dumps(current_snapshot))
            return "FIRST SNAPSHOT STORED"
        combined_text = self._process_deltas(grouped_heavy, grouped_freq, last_snapshot, db_name)

        self._store_simple_metrics(queries, memory)
        self._store_texplain_metrics(heavy_raw, users)
        self.redis.set("BaseContaLastMetrics", json.dumps(current_snapshot))

        return combined_text

    # ------------------- Funciones auxiliares -------------------
    def _get_current_snapshot(self):
        return datetime.now(tz=self.timezone).isoformat()

    def _fetch_db_data(self):
        heavy_raw = self.database.getHeaviesQueries()
        freq_raw = self.database.getMostRequestedQueries()
        queries = self.database.getCurrentQueries()
        users = self.database.getCurrentUsers()
        memory = self.database.getMemoryUsage()
        return heavy_raw, freq_raw, queries, users, memory

    def _normalize_and_group(self, heavy_raw, freq_raw, snapshot):
        heavy = MetricsDomain.normalize_queries(heavy_raw, snapshot, QueryDomain.getMainTable)
        freq = MetricsDomain.normalize_queries(freq_raw, snapshot, QueryDomain.getMainTable)
        grouped_heavy = MetricsDomain.group_heavy_queries(heavy)
        grouped_freq = MetricsDomain.group_frequent_queries(freq)
        return grouped_heavy, grouped_freq

    def _get_last_snapshot(self):
        last_snapshot_raw = self.redis.get_value("BaseContaLastMetrics")
        return json.loads(last_snapshot_raw) if last_snapshot_raw else None

    def _process_deltas(self, grouped_heavy, grouped_freq, last_snapshot, db_name):
        new_heavy = MetricsDomain.detect_new_tables(last_snapshot["heavy"], grouped_heavy)
        new_freq = MetricsDomain.detect_new_tables(last_snapshot["frequent"], grouped_freq)
        heavy_deltas = MetricsDomain.calculate_deltas(last_snapshot["heavy"], grouped_heavy, new_heavy)
        freq_deltas = MetricsDomain.calculate_deltas(last_snapshot["frequent"], grouped_freq, new_freq)

        main_text = self.prometheus.generate_text(heavy_deltas, "heavy")
        freq_text = self.prometheus.generate_text(freq_deltas, "freq")
        combined_text = main_text + "\n" + freq_text

        self.redis.set(f"metrics:{db_name}", combined_text, ttl=86400)
        return combined_text

    def _store_simple_metrics(self, queries, memory):
        queries_text = self.prometheus.generate_simple_gauge(
            "db_current_queries",
            "Consultas ejecutándose ahora en SQL Server",
            queries[0]["queries_processing_now"]
        )
        self.redis.set("BaseContaQueriesProcessing", queries_text, 1200)

        memory_text = ""
        for key, value in memory[0].items():
            memory_text += self.prometheus.generate_simple_gauge(
                f"db_memory_{key}",
                f"Métrica de memoria SQL Server: {key}",
                value
            )
        self.redis.set("BaseContaMemoryUsage", memory_text, 1200)

    def _store_texplain_metrics(self, heavy_raw, users):
        texplain = MetricsDomain.generate_texplain_top10(heavy_raw, QueryDomain.getMainTable)
        texplain_text = self.prometheus.generate_texplain_gauges(texplain)
        self.redis.set("BaseContaTexplainTop10", texplain_text, 3600)

        texplain_users = MetricsDomain.generate_texplain_users(users)
        text_pain_users = self.prometheus.generate_texplain_users_gauges(texplain_users=texplain_users)
        self.redis.set("BaseContaTexplainUsers", json.dumps(text_pain_users), 3600)


    def fetchRecords(self):
        record = self.redis.get_value("metrics:Baseconta")
        mem = self.redis.get_value("BaseContaMemoryUsage")
        q = self.redis.get_value("BaseContaQueriesProcessing")
        user = self.redis.get_value("BaseContaTexplainUsers")
        rop = self.redis.get_value("BaseContaTexplainTop10")
        return "\n".join(filter(None, [record, q, mem, rop, user]))
