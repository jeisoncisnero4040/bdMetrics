from src.Utils.DatabaseConnection import DatabaseConnection
from pyodbc import Connection
from settings.AppSettings import TIMEZONE
import pytz


class BdRepository:
    def __init__(self, db_connection: DatabaseConnection):
        self.con:Connection=db_connection.connection()
        self.timezone:str = pytz.timezone(TIMEZONE)

    def getHeaviesQuerys(self)->list:
        query = """SELECT TOP 50
                    qs.execution_count,                         -- cuántas veces
                    qs.total_worker_time AS cpu_time_total,     -- CPU total
                    qs.total_worker_time / qs.execution_count AS cpu_time_avg,
                    qs.total_elapsed_time AS duration_total,    -- duración total
                    qs.total_elapsed_time / qs.execution_count AS duration_avg,
                    qs.total_logical_reads AS logical_reads_total,
                    qs.total_logical_reads / qs.execution_count AS logical_reads_avg,
                    qs.total_logical_writes AS logical_writes_total,
                    qs.total_logical_writes / qs.execution_count AS logical_writes_avg,
                    qs.total_physical_reads AS physical_reads_total,
                    qs.total_physical_reads / qs.execution_count AS physical_reads_avg,
                    qs.plan_generation_num AS plan_reuse_count,
                    qs.creation_time,
                    qs.last_execution_time,
                    SUBSTRING(
                        st.text,
                        (qs.statement_start_offset / 2) + 1,
                        (
                            (CASE qs.statement_end_offset
                                WHEN -1 THEN DATALENGTH(st.text)
                                ELSE qs.statement_end_offset
                            END - qs.statement_start_offset
                            ) / 2
                        ) + 1
                    ) AS query_text
                FROM sys.dm_exec_query_stats qs
                CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
                ORDER BY qs.total_worker_time DESC;
        """
        return self.__fetchQuery(query=query)
    def getMostRequestedQuery(self):
        query="""
            SELECT TOP 50
            SUBSTRING(
                st.text,
                (qs.statement_start_offset/2)+1,
                ((CASE qs.statement_end_offset
                    WHEN -1 THEN DATALENGTH(st.text)
                    ELSE qs.statement_end_offset
                END - qs.statement_start_offset)/2)+1
            ) AS query_text,
            qs.execution_count,
            qs.total_worker_time AS cpu_time_total,
            qs.total_worker_time / qs.execution_count AS cpu_time_avg,
            qs.total_elapsed_time AS duration_total,
            qs.total_elapsed_time / qs.execution_count AS duration_avg,
            qs.total_logical_reads AS logical_reads_total,
            qs.total_logical_reads / qs.execution_count AS logical_reads_avg,
            qs.creation_time,
            qs.last_execution_time,
            qs.plan_generation_num AS plan_reuse_count,
            -- Métrica de frecuencia: ejecuciones por segundo aproximadas
            qs.execution_count / 
                (DATEDIFF(SECOND, qs.creation_time, qs.last_execution_time) + 1) AS exec_per_second
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
        WHERE qs.execution_count > 1000 -- Ajustable: mínimo número de ejecuciones sospechosas
        ORDER BY exec_per_second DESC
        """
        return self.__fetchQuery(query=query)
    def __fetchQuery(self, query: str, params: tuple = ()) -> list:
        try:
            cursor = self.con.cursor()
            cursor.execute(query, params)
            results = cursor.fetchall()

            if not results:
                return []

            column_names = [column[0] for column in cursor.description]
            return [dict(zip(column_names, row)) for row in results]
        except Exception as e:
            return []