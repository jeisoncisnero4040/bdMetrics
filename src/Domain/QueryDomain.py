import re

class QueryDomain:

    @staticmethod
    def getMainTable(sql: str) -> str:
        match = re.search(r"from\s+([a-zA-Z0-9_\.]+)", sql, re.IGNORECASE)
        return match.group(1) if match else "unknown"
