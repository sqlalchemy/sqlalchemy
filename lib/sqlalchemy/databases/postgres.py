import sys, StringIO, string

import sqlalchemy.sql as sql
import sqlalchemy.schema as schema
import sqlalchemy.ansisql as ansisql
from sqlalchemy.ansisql import *

def engine(**params):
    return PGSQLEngine(**params)
    
class PGSQLEngine(ansisql.ANSISQLEngine):
    def __init__(self, **params):
        ansisql.ANSISQLEngine.__init__(self, **params)

    def create_connection(self):
        raise NotImplementedError()
        
