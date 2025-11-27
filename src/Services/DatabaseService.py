from src.Repositories.BdRepository import BdRepository

class DatabaseService():
    def __init__(self,repo:BdRepository):
        self.repo=repo
    def getHeaviesQuery(self):
        return self.repo.getHeaviesQuerys()
    def getMostRequestedQueries(self):
        return self.repo.getMostRequestedQuery()
