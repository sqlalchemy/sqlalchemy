from sqlalchemy.dialects.postgres import base, psycopg2

base.dialect = psycopg2.dialect