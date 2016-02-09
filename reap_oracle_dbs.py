"""Drop Oracle databases that are left over from a 
multiprocessing test run.

Currently the cx_Oracle driver seems to sometimes not release a
TCP connection even if close() is called, which prevents the provisioning
system from dropping a database in-process.

"""
from sqlalchemy.testing.plugin import plugin_base
from sqlalchemy.testing import engines
from sqlalchemy.testing import provision
import logging

logging.basicConfig()
logging.getLogger(provision.__name__).setLevel(logging.INFO)

plugin_base.read_config()
oracle = plugin_base.file_config.get('db', 'oracle')
from sqlalchemy.testing import provision

engine = engines.testing_engine(oracle, {})
provision.reap_oracle_dbs(engine)


