from typing import List, Dict, Callable


class MetricsDomain:

    # -------------------------------------------------------------
    # 🧹 Normalización de texto SQL + asignar tabla principal
    # -------------------------------------------------------------
    @staticmethod
    def normalize_queries(
        queries: List[Dict],
        snapshot: str,
        main_table_func: Callable[[str], str],
    ) -> List[Dict]:
        """
        Enriquecer cada query con:
        - tabla principal detectada
        - snapshot timestamp
        - query normalizada
        """
        normalized = []

        for q in queries:
            q = q.copy()
            text = q.get("query_text", "") or ""
            clean = " ".join(text.split()).lower()

            q["query_normalized"] = clean
            q["main_table"] = main_table_func(clean)
            q["snapshot"] = snapshot

            normalized.append(q)

        return normalized

    # -------------------------------------------------------------
    # 🧮 Agrupaciones para consultas pesadas
    # -------------------------------------------------------------
    @staticmethod
    def group_heavy_queries(queries: List[Dict]) -> Dict:
        """
        Agrupa por tabla los indicadores de consultas pesadas.
        Las columnas utilizadas provienen del query getHeaviesQuery().
        """
        tables = {}

        for q in queries:
            tbl = q.get("main_table")
            if tbl not in tables:
                tables[tbl] = {
                    "execution_count": 0,
                    "cpu_time_total": 0,
                    "logical_reads_total": 0,
                    "logical_writes_total": 0,
                    "physical_reads_total": 0,
                    "duration_total": 0,
                }

            # sumar solo si existe en el diccionario
            for field in tables[tbl].keys():
                tables[tbl][field] += q.get(field, 0) or 0

        return tables

    # -------------------------------------------------------------
    # 📊 Agrupación para consultas más frecuentes
    # -------------------------------------------------------------
    @staticmethod
    def group_frequent_queries(queries: List[Dict]) -> Dict:
        """
        Agrupa por tabla las consultas más ejecutadas.
        Columnas provienen de getMostRequestedQuery().
        """
        tables = {}

        for q in queries:
            tbl = q.get("main_table")
            if tbl not in tables:
                tables[tbl] = {
                    "execution_count": 0,
                    "total_elapsed_time": 0,
                    "exec_per_second": 0,
                    "queries": [],
                }

            tables[tbl]["execution_count"] += q.get("execution_count", 0) or 0
            tables[tbl]["total_elapsed_time"] += q.get("duration_total", 0) or 0
            tables[tbl]["exec_per_second"] += q.get("exec_per_second", 0) or 0


        return tables

    # -------------------------------------------------------------
    # 🆕 Detectar tablas nuevas que antes no existían
    # -------------------------------------------------------------
    @staticmethod
    def detect_new_tables(old: Dict, current: Dict) -> List[str]:
        return [tbl for tbl in current.keys() if tbl not in old]

    # -------------------------------------------------------------
    # 🔺 Cálculo de deltas entre snapshot anterior y actual
    # -------------------------------------------------------------
    @staticmethod
    def calculate_deltas(old: Dict, current: Dict, new_tables: List[str]) -> Dict:
        deltas = {}

        for tbl, values in current.items():
            deltas[tbl] = {}

            if tbl in new_tables:
                for k, v in values.items():
                    deltas[tbl][f"{k}_delta"] = MetricsDomain._normalize_number(v)
                deltas[tbl]["is_new_table"] = True
            else:
                for k, v in values.items():
                    v = MetricsDomain._normalize_number(v)
                    old_val_raw = old[tbl].get(k, 0) if tbl in old else 0
                    old_val = MetricsDomain._normalize_number(old_val_raw)

                    deltas[tbl][f"{k}_delta"] = v - old_val

                deltas[tbl]["is_new_table"] = False

        return deltas

    # -------------------------------------------------------------
    # 🧱 Construcción final de snapshot
    # -------------------------------------------------------------
    @staticmethod
    def build_snapshot(heavy: Dict, frequent: Dict, snapshot: str) -> Dict:
        return {
            "snapshot": snapshot,
            "heavy": heavy,
            "frequent": frequent
        }
    @staticmethod
    def _normalize_number(value):
        # Si es None → 0
        if value is None:
            return 0

        # Si es lista → tomar primer valor o 0
        if isinstance(value, list):
            if len(value) == 0:
                return 0
            return value[0]

        # Si es número → devolver tal cual
        if isinstance(value, (int, float)):
            return value

        # Si no es interpretable → 0
        return 0