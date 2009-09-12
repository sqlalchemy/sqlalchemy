"""Provide forwards compatibility with SQLAlchemy 0.6 which 
uses the name "postgresql" for the Postgresql dialect.

"""
from sqlalchemy.databases.postgres import dialect