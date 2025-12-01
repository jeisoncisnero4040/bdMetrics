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
                    qs.execution_count,
                    qs.total_worker_time AS cpu_time_total,
                    qs.total_worker_time / qs.execution_count AS cpu_time_avg,
                    qs.total_elapsed_time AS duration_total,
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
                WHERE 
                    DB_NAME(st.dbid) = 'Baseconta'          
                    AND st.text NOT LIKE '%sys.%'           
                    AND st.text NOT LIKE '%dm_%'            
                    AND st.text NOT LIKE '%INTERNAL%'      
                    AND st.text NOT LIKE '%sp_%'            
                    AND st.text NOT LIKE '%xp_%'           
                ORDER BY qs.total_worker_time DESC;
        """
        return self.__fetchQuery(query=query)
    def getMostRequestedQuery(self):
        query="""
            SELECT TOP 50
                qs.execution_count,
                qs.total_worker_time AS cpu_time_total,
                qs.total_worker_time / qs.execution_count AS cpu_time_avg,
                qs.total_elapsed_time AS duration_total,
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
            WHERE 
                DB_NAME(st.dbid) = 'Baseconta'            
                AND st.text NOT LIKE '%sys.%'           
                AND st.text NOT LIKE '%INTERNAL%'     
                AND st.text NOT LIKE '%dm_exec%'         
            ORDER BY qs.total_worker_time DESC;
        """
        return self.__fetchQuery(query=query)
    def getCurrentQuerys(self):
        query="""SELECT 
            COUNT(*) AS queries_processing_now
        FROM sys.dm_exec_requests
        WHERE status = 'running'"""
        return self.__fetchQuery(query=query)
    def getCurrentUsers(self):
        query="""SELECT 
                s.host_name,
                c.client_net_address,
                s.program_name,
                COUNT(r.session_id) AS requests_running_now
            FROM sys.dm_exec_sessions s
            LEFT JOIN sys.dm_exec_requests r
                ON s.session_id = r.session_id
            LEFT JOIN sys.dm_exec_connections c
                ON s.session_id = c.session_id
            WHERE s.is_user_process = 1
            GROUP BY 
                s.host_name,
                c.client_net_address,
                s.program_name
            ORDER BY requests_running_now DESC;
            """
        return self.__fetchQuery(query=query)

    def getMemoryData(self):
        return self.__fetchQuery(
            """SELECT 
                physical_memory_in_use_kb / 1024 AS sqlserver_memory_used_mb,
                virtual_address_space_reserved_kb / 1024 AS vas_reserved_mb,
                virtual_address_space_committed_kb / 1024 AS vas_committed_mb,
                locked_page_allocations_kb / 1024 AS locked_pages_mb
            FROM sys.dm_os_process_memory;

            """
        )

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