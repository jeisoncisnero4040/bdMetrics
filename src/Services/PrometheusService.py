from prometheus_client import CollectorRegistry, Gauge, generate_latest

class PrometheusService:
    def __init__(self):
        pass

    def generate_text(self, deltas: dict, metric_type: str):
        """
        Genera textPlain de Prometheus a partir de un dict de métricas.
        metric_type: 'heavy' o 'freq'
        """
        registry = CollectorRegistry()
        gauges_cache = {}

        for table, metrics in deltas.items():
            for key, value in metrics.items():
                if key == "is_new_table":
                    continue

                gauge_name = f"db_{metric_type}_{key}"

                if gauge_name not in gauges_cache:
                    gauges_cache[gauge_name] = Gauge(
                        gauge_name,
                        f"Métrica {metric_type} {key}",
                        ["table", "is_new_table"],
                        registry=registry,
                    )

                gauges_cache[gauge_name].labels(
                    table=table,
                    is_new_table=str(metrics["is_new_table"]),
                ).set(value)

        return generate_latest(registry).decode("utf-8")

    def generate_simple_gauge(self, name: str, description: str, value: float):
        """
        Genera textPlain simple para un único gauge.
        """
        registry = CollectorRegistry()
        g = Gauge(name, description, registry=registry)
        g.set(value)
        return generate_latest(registry).decode("utf-8")

    def generate_texplain_gauges(self, texplain_top10):
        """
        Recibe la lista generada por generate_texplain_top10 y devuelve
        un string para exportar a Prometheus, incluyendo query_text.
        """
        lines = []

        for row in texplain_top10:
            labels = f'rank="{row["rank"]}",table="{row["table"]}",query="{row["query_text"]}"'

            lines.append(f'texplain_cpu_time_total{{{labels}}} {row["cpu_time_total"]}')
            lines.append(f'texplain_duration_total{{{labels}}} {row["duration_total"]}')
            lines.append(f'texplain_logical_reads_total{{{labels}}} {row["logical_reads_total"]}')
            lines.append(f'texplain_logical_writes_total{{{labels}}} {row["logical_writes_total"]}')
            lines.append(f'texplain_physical_reads_total{{{labels}}} {row["physical_reads_total"]}')
            lines.append(f'texplain_plan_reuse_count{{{labels}}} {row["plan_reuse_count"]}')

        return "\n".join(lines)

    def generate_texplain_users_gauges(self, texplain_users):
        """
        Genera textPlain Prometheus a partir de un diccionario de usuarios conectados.
        """
        registry = CollectorRegistry()
        gauges_cache = {}

        for row in texplain_users:
            host = row["host_name"]
            client = row["client_net_address"]
            program = row["program_name"]
            rank = str(row["rank"])

            gauge_name = "user_requests_running_now"

            if gauge_name not in gauges_cache:
                gauges_cache[gauge_name] = Gauge(
                    gauge_name,
                    "Número de requests activas por usuario",
                    ["host_name", "client_net_address", "program_name", "rank"],
                    registry=registry
                )

            gauges_cache[gauge_name].labels(
                host_name=host,
                client_net_address=client,
                program_name=program,
                rank=rank
            ).set(row["requests_running_now"])

        return generate_latest(registry).decode("utf-8")