import gevent.monkey
gevent.monkey.patch_all()  # noqa

import logging
logging.basicConfig()  # noqa
# logging.getLogger("sqlalchemy.pool").setLevel(logging.INFO)
from sqlalchemy import event
import random
import sys
from sqlalchemy import create_engine
import traceback

engine = create_engine('mysql+pymysql://scott:tiger@localhost/test',
                       pool_size=50, max_overflow=0)


@event.listens_for(engine, "connect")
def conn(*arg):
    print "new connection!"


def worker():
    while True:
        conn = engine.connect()
        try:
            trans = conn.begin()
            for i in range(5):
                conn.execute("SELECT 1+1")
                gevent.sleep(random.random() * 1.01)

        except Exception:
            # traceback.print_exc()
            sys.stderr.write('X')
        else:
            conn.close()
            sys.stderr.write('.')


def main():
    for i in range(40):
        gevent.spawn(worker)

    gevent.sleep(3)

    while True:
        result = list(engine.execute("show processlist"))
        engine.execute("kill %d" % result[-2][0])
        print "\n\n\n BOOM!!!!! \n\n\n"
        gevent.sleep(5)
        print(engine.pool.status())


main()
