from datetime import datetime
from src.Services.DatabaseService import DatabaseService
from src.Services.RedisService import RedisService
from src.Domain.QueryDomain import QueryDomain
import pytz
import json
from settings.AppSettings import TIMEZONE
from prometheus_client import CollectorRegistry, Gauge, generate_latest


class MetricsService:
    def __init__(self, redis: RedisService, database: DatabaseService):
        self.redis = redis
        self.database = database
        self.timezone = pytz.timezone(TIMEZONE)

    def calculate_deltas(self, current_data, previous_data):
        """Calcula diferencias entre el snapshot actual y anterior"""
        deltas = []
        
        for current in current_data:
            previous = next(
                (item for item in previous_data 
                if item['query_text'] == current['query_text'] 
                and item['main_table'] == current['main_table']), 
                None
            )
            
            delta_record = current.copy()
            
            # Definir campos con valores por defecto
            fields = [
                'execution_count', 'cpu_time_total', 'duration_total',
                'logical_reads_total', 'logical_writes_total', 'physical_reads_total'
            ]
            
            # Campos opcionales con valores por defecto
            optional_fields = {
                'exec_per_second': 0.0
            }
            
            if previous:
                # Calcular deltas para campos principales
                for field in fields:
                    current_val = current.get(field, 0)
                    previous_val = previous.get(field, 0)
                    delta_record[field] = current_val - previous_val
                
                # Manejar campos opcionales
                for field, default_val in optional_fields.items():
                    current_val = current.get(field, default_val)
                    previous_val = previous.get(field, default_val)
                    delta_record[field] = current_val - previous_val
                    
            else:
                # Es la primera vez, usar valores actuales
                for field in fields:
                    delta_record[field] = current.get(field, 0)
                
                for field, default_val in optional_fields.items():
                    delta_record[field] = current.get(field, default_val)
            
            deltas.append(delta_record)
        
        return deltas

    def getPreviousSnapshot(self, db_name: str):
        """Obtiene el snapshot anterior desde Redis"""
        try:
            previous_data = self.redis.get(f"{db_name}_snapshot_data")
            if previous_data:
                # Decodificar si es bytes y cargar JSON
                if isinstance(previous_data, bytes):
                    previous_data = previous_data.decode('utf-8')
                return json.loads(previous_data)
        except Exception as e:
            print(f"Error obteniendo snapshot anterior: {e}")
        return None

    def saveCurrentSnapshot(self, db_name: str, heavy_data, frequent_data, snapshot_time: str):
        """Guarda el snapshot actual en Redis para usar en el próximo delta"""
        
        def convert_datetime_to_string(obj):
            """Convierte objetos datetime a strings ISO format"""
            if isinstance(obj, datetime):
                return obj.isoformat()
            elif isinstance(obj, dict):
                return {key: convert_datetime_to_string(value) for key, value in obj.items()}
            elif isinstance(obj, list):
                return [convert_datetime_to_string(item) for item in obj]
            else:
                return obj
        
        try:
            # Convertir cualquier datetime a string antes de serializar
            heavy_serializable = convert_datetime_to_string(heavy_data)
            frequent_serializable = convert_datetime_to_string(frequent_data)
            
            snapshot_data = {
                'heavy': heavy_serializable,
                'frequent': frequent_serializable,
                'timestamp': snapshot_time
            }
            
            # Guardar por 24 horas para asegurar disponibilidad en el próximo run
            self.redis.setex(
                f"{db_name}_snapshot_data", 
                86400,  # 24 horas en segundos
                json.dumps(snapshot_data, default=str)  # Usar default=str como respaldo
            )
            print(f"Snapshot guardado exitosamente para {db_name}")
            
        except Exception as e:
            print(f"Error guardando snapshot: {e}")

    def processRecord(self, db_name: str):
        # Obtener datos actuales (sin procesar aún)
        raw_heavy = self.database.getHeaviesQuery()       
        raw_frequent = self.database.getMostRequestedQueries()   
        snapshot_time = datetime.now(tz=self.timezone).isoformat()

        # Función para normalizar y asegurar campos
        def normalize_query_data(query_list):
            normalized = []
            for item in query_list:
                # Asegurar que todos los campos existan
                normalized_item = {
                    'query_text': item.get('query_text', ''),
                    'execution_count': item.get('execution_count', 0),
                    'cpu_time_total': item.get('cpu_time_total', 0),
                    'duration_total': item.get('duration_total', 0),
                    'logical_reads_total': item.get('logical_reads_total', 0),
                    'logical_writes_total': item.get('logical_writes_total', 0),
                    'physical_reads_total': item.get('physical_reads_total', 0),
                    'exec_per_second': item.get('exec_per_second', 0.0),
                    'main_table': QueryDomain.getMainTable(item.get('query_text', '')),
                    'snapshot': snapshot_time
                }
                normalized.append(normalized_item)
            return normalized

        # Normalizar los datos
        normalized_heavy = normalize_query_data(raw_heavy)
        normalized_frequent = normalize_query_data(raw_frequent)

        # Obtener snapshot anterior y calcular deltas
        previous_data = self.getPreviousSnapshot(db_name)
        
        # Hacer copias para guardar el estado actual antes de calcular deltas
        current_heavy = [item.copy() for item in normalized_heavy]
        current_frequent = [item.copy() for item in normalized_frequent]
        
        # Aplicar deltas a los datos que se exportarán a Prometheus
        if previous_data:
            normalized_heavy = self.calculate_deltas(normalized_heavy, previous_data.get('heavy', []))
            normalized_frequent = self.calculate_deltas(normalized_frequent, previous_data.get('frequent', []))
        
        registry = CollectorRegistry()

        # DEFINICIÓN DE MÉTRICAS DELTA
        queries_total = Gauge(
            'delta_sql_queries_total',
            'Delta de consultas ejecutadas desde último snapshot',
            ['database', 'table', 'snapshot'],
            registry=registry
        )

        cpu_total = Gauge(
            'delta_sql_query_cpu_total',
            'Delta de CPU usado por query desde último snapshot (ms)',
            ['database', 'query', 'table', 'snapshot'],
            registry=registry
        )

        duration_total = Gauge(
            'delta_sql_query_duration_total',
            'Delta de duración de queries desde último snapshot (ms)',
            ['database', 'query', 'table', 'snapshot'],
            registry=registry
        )

        logical_reads_total = Gauge(
            'delta_sql_query_logical_reads_total',
            'Delta de lecturas lógicas desde último snapshot',
            ['database', 'query', 'table', 'snapshot'],
            registry=registry
        )

        logical_writes_total = Gauge(
            'delta_sql_query_logical_writes_total',
            'Delta de escrituras lógicas desde último snapshot',
            ['database', 'query', 'table', 'snapshot'],
            registry=registry
        )

        physical_reads_total = Gauge(
            'delta_sql_query_physical_reads_total',
            'Delta de lecturas físicas desde último snapshot',
            ['database', 'query', 'table', 'snapshot'],
            registry=registry
        )

        exec_per_second = Gauge(
            'delta_sql_query_exec_per_second',
            'Ejecuciones por segundo de la query',
            ['database', 'query', 'table', 'snapshot'],
            registry=registry
        )

        # EXPORTAR MÉTRICAS DELTA A PROMETHEUS
        for query in normalized_frequent:
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

        for query in normalized_heavy:
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

        # Guardar snapshot actual para el próximo cálculo de deltas
        self.saveCurrentSnapshot(db_name, current_heavy, current_frequent, snapshot_time)

        # Guardar métricas en Redis para la API
        key = self.redis.saveRecord(db_name, prometheus_text)
        return key

    def fetchRecords(self):
        """
        Devuelve todas las métricas guardadas en Redis en formato text/plain listo para Prometheus
        """
        records = self.redis.getRecords('Baseconta')
        return "\n".join(records)