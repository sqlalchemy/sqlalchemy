from __future__ import print_function

import logging
import random
import sys

import gevent.monkey

from sqlalchemy import create_engine
from sqlalchemy import event


gevent.monkey.patch_all()  # noqa

logging.basicConfig()  # noqa
# logging.getLogger("sqlalchemy.pool").setLevel(logging.INFO)

engine = create_engine(
    "mysql+pymysql://scott:tiger@localhost/test", pool_size=50, max_overflow=0
)


@event.listens_for(engine, "connect")
def conn(*arg):
    print("new connection!")


def worker():
    while True:
        conn = engine.connect()
        try:
            conn.begin()
            for i in range(5):
                conn.exec_driver_sql("SELECT 1+1")
                gevent.sleep(random.random() * 1.01)

        except Exception:
            # traceback.print_exc()
            sys.stderr.write("X")
        else:
            conn.close()
            sys.stderr.write(".")


def main():
    for i in range(40):
        gevent.spawn(worker)

    gevent.sleep(3)

    while True:
        result = list(engine.exec_driver_sql("show processlist"))
        engine.exec_driver_sql("kill %d" % result[-2][0])
        print("\n\n\n BOOM!!!!! \n\n\n")
        gevent.sleep(5)
        print(engine.pool.status())


main()
