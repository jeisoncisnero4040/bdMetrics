from src.Repositories.BdRepository import BdRepository

class DatabaseService:
    def __init__(self, repo: BdRepository):
        self.repo = repo

    # Consultas más pesadas (TOP 50 por CPU)
    def getHeaviesQueries(self):
        return self.repo.getHeaviesQuerys()

    # Consultas más ejecutadas (TOP 50 por execution_count)
    def getMostRequestedQueries(self):
        return self.repo.getMostRequestedQuery()

    # Consultas que están corriendo justo ahora
    def getCurrentQueries(self):
        return self.repo.getCurrentQuerys()

    # Usuarios conectados y sus requests
    def getCurrentUsers(self):
        return self.repo.getCurrentUsers()

    # Información de memoria del proceso de SQL Server
    def getMemoryUsage(self):
        return self.repo.getMemoryData()
