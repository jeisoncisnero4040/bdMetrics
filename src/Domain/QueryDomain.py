import re
from typing import Optional
from src.Const.tables import TABLES



class QueryDomain:

    @staticmethod
    def getMainTable(sql: str) -> str:
        """
        Extrae la tabla principal de una consulta SQL de manera robusta.
        Maneja diferentes tipos de consultas y escenarios complejos.
        """
        if not sql or not sql.strip():
            return "unknown"
        
        sql = sql.lower()
        # Limpiar y normalizar el SQL
        clean_sql = QueryDomain._normalize_sql(sql)
        
        # Intentar diferentes patrones en orden de prioridad
        table = (
            QueryDomain._extract_from_update(clean_sql) or
            QueryDomain._extract_from_delete(clean_sql) or
            QueryDomain._extract_from_insert(clean_sql) or
            QueryDomain._extract_from_select(clean_sql) or
            QueryDomain._extract_from_join(clean_sql) or
            None
        )
        
        # Si no se encontró nada, buscar la primera concordancia con TABLES
        if not table:
            for t in TABLES:
                pattern = r'\b' + re.escape(t.lower()) + r'\b'
                if re.search(pattern, clean_sql):
                    table = t
                    break
        
        if not table:
            print('aqui en la execion')
            print(sql)
            table = "unknown"
        
        return QueryDomain._clean_table_name(table)


    @staticmethod
    def _normalize_sql(sql: str) -> str:
        """Normaliza el SQL para hacer el parsing más fácil"""
        # Reemplazar múltiples espacios y newlines con un solo espacio
        sql = re.sub(r'\s+', ' ', sql)
        # Remover comentarios
        sql = re.sub(r'--.*?$', '', sql, flags=re.MULTILINE)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        sql = sql.replace('[', '').replace(']', '').replace('dbo','').replace('.',' ')
        return sql.strip()

    @staticmethod
    def _extract_from_select(sql: str) -> Optional[str]:
        """Extrae tabla de consultas SELECT"""
        # Patrón para SELECT ... FROM table
        patterns = [
            r'\bFROM\s+([a-zA-Z0-9_#@]+(?:\.[a-zA-Z0-9_#@]+)?(?:\s+AS\s+\w+)?)(?:\s|$)',  # FROM table
            r'\bJOIN\s+([a-zA-Z0-9_#@]+(?:\.[a-zA-Z0-9_#@]+)?)(?:\s|$)',  # JOIN table
        ]
        
        for pattern in patterns:
            match = re.search(pattern, sql, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    @staticmethod
    def _extract_from_update(sql: str) -> Optional[str]:
        """Extrae tabla de consultas UPDATE"""
        match = re.search(r'\bUPDATE\s+([a-zA-Z0-9_#@]+(?:\.[a-zA-Z0-9_#@]+)?)', sql, re.IGNORECASE)
        return match.group(1) if match else None

    @staticmethod
    def _extract_from_delete(sql: str) -> Optional[str]:
        """Extrae tabla de consultas DELETE"""
        match = re.search(r'\bDELETE\s+(?:\w+\s+)?FROM\s+([a-zA-Z0-9_#@]+(?:\.[a-zA-Z0-9_#@]+)?)', sql, re.IGNORECASE)
        return match.group(1) if match else None

    @staticmethod
    def _extract_from_insert(sql: str) -> Optional[str]:
        """Extrae tabla de consultas INSERT"""
        match = re.search(r'\bINSERT\s+(?:\w+\s+)?INTO\s+([a-zA-Z0-9_#@]+(?:\.[a-zA-Z0-9_#@]+)?)', sql, re.IGNORECASE)
        return match.group(1) if match else None

    @staticmethod
    def _extract_from_join(sql: str) -> Optional[str]:
        """Extrae la primera tabla en JOINs complejos"""
        # Buscar el primer FROM que no sea subquery
        from_match = re.search(r'\bFROM\s+([a-zA-Z0-9_#@]+)', sql, re.IGNORECASE)
        return from_match.group(1) if from_match else None

    @staticmethod
    def _clean_table_name(table_name: str) -> str:
        """Limpia el nombre de tabla removiendo alias y espacios"""
        if table_name == "unknown":
            return table_name
            
        # Remover alias (AS alias)
        table_name = re.sub(r'\s+AS\s+\w+$', '', table_name, flags=re.IGNORECASE)
        # Remover espacios extras
        table_name = table_name.strip()
        # Remover comillas si existen
        table_name = re.sub(r'^\[|\]$|^"|"$|^`|`$', '', table_name)
        
        return table_name.lower() if table_name else "unknown"

    @staticmethod
    def get_query_type(sql: str) -> str:
        """Determina el tipo de consulta"""
        clean_sql = QueryDomain._normalize_sql(sql)
        
        if re.search(r'^\s*SELECT', clean_sql, re.IGNORECASE):
            return "SELECT"
        elif re.search(r'^\s*UPDATE', clean_sql, re.IGNORECASE):
            return "UPDATE"
        elif re.search(r'^\s*INSERT', clean_sql, re.IGNORECASE):
            return "INSERT"
        elif re.search(r'^\s*DELETE', clean_sql, re.IGNORECASE):
            return "DELETE"
        elif re.search(r'^\s*CREATE', clean_sql, re.IGNORECASE):
            return "DDL"
        elif re.search(r'^\s*ALTER', clean_sql, re.IGNORECASE):
            return "DDL"
        elif re.search(r'^\s*DROP', clean_sql, re.IGNORECASE):
            return "DDL"
        else:
            return "UNKNOWN"

    @staticmethod
    def extract_all_tables(sql: str) -> list:
        """Extrae todas las tablas mencionadas en la consulta"""
        clean_sql = QueryDomain._normalize_sql(sql)
        tables = set()
        
        # Patrones para encontrar tablas
        patterns = [
            r'\bFROM\s+([a-zA-Z0-9_#@]+(?:\.[a-zA-Z0-9_#@]+)?)',
            r'\bJOIN\s+([a-zA-Z0-9_#@]+(?:\.[a-zA-Z0-9_#@]+)?)',
            r'\bUPDATE\s+([a-zA-Z0-9_#@]+(?:\.[a-zA-Z0-9_#@]+)?)',
            r'\bINSERT\s+(?:\w+\s+)?INTO\s+([a-zA-Z0-9_#@]+(?:\.[a-zA-Z0-9_#@]+)?)',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, clean_sql, re.IGNORECASE)
            for match in matches:
                clean_table = QueryDomain._clean_table_name(match)
                if clean_table and clean_table != "unknown":
                    tables.add(clean_table)
        
        return sorted(list(tables))