from datetime import datetime
from typing import List, Dict, Optional


class DeltaDomain:

    METRICS = [
        "execution_count",
        "cpu_time_total",
        "duration_total",
        "logical_reads_total",
        "logical_writes_total",
        "physical_reads_total",
    ]

    @staticmethod
    def calculate(records: List[Dict]) -> List[Dict]:
        """
        Dado un conjunto de snapshots obtenidos desde Redis, calcula deltas entre ellos.
        Regresa una nueva lista con los snapshots enriquecidos.
        """

        if not records:
            return []

        # Asegurar orden temporal
        records = sorted(records, key=lambda r: r["snapshot_time"])

        enriched = []
        previous_by_query = {}

        for snapshot in records:
            snapshot_time = snapshot["snapshot_time"]
            db_name = snapshot["database"]

            heavy = snapshot.get("heavy_queries", [])
            frequent = snapshot.get("frequent_queries", [])

            enriched_snapshot = {
                "database": db_name,
                "snapshot_time": snapshot_time,
                "heavy_queries": [],
                "frequent_queries": [],
            }

            # Procesar ambos tipos
            enriched_snapshot["heavy_queries"] = DeltaDomain._process_list(
                heavy, previous_by_query
            )
            enriched_snapshot["frequent_queries"] = DeltaDomain._process_list(
                frequent, previous_by_query
            )

            enriched.append(enriched_snapshot)

        return enriched

    @staticmethod
    def _process_list(items: List[Dict], previous_by_query: dict) -> List[Dict]:
        processed = []

        for item in items:
            uid = DeltaDomain._build_uid(item)
            prev = previous_by_query.get(uid)

            delta = DeltaDomain._calculate_delta(item, prev)

            # Adjuntar delta al registro
            item_with_delta = {
                **item,
                "delta": delta,
            }

            processed.append(item_with_delta)

            # Guardar como última versión
            previous_by_query[uid] = item

        return processed

    @staticmethod
    def _build_uid(item: Dict) -> str:
        """
        Identificador único por query.
        """
        return f"{item.get('main_table','none')}::{item.get('query_text','').strip()}"

    @staticmethod
    def _calculate_delta(current: Dict, previous: Optional[Dict]) -> Dict:
        """
        Calcula el delta entre las métricas actuales y las previas.
        """
        if not previous:
            return {m: None for m in DeltaDomain.METRICS}

        delta = {}
        for m in DeltaDomain.METRICS:
            try:
                delta[m] = current.get(m, 0) - previous.get(m, 0)
            except:
                delta[m] = None

        return delta
