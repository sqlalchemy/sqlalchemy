"""Drop Oracle databases that are left over from a
multiprocessing test run.

Currently the cx_Oracle driver seems to sometimes not release a
TCP connection even if close() is called, which prevents the provisioning
system from dropping a database in-process.

"""
from sqlalchemy.testing import provision
import logging
import sys

logging.basicConfig()
logging.getLogger(provision.__name__).setLevel(logging.INFO)

provision.reap_oracle_dbs(sys.argv[1])


