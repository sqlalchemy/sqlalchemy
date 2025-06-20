from decimal import Decimal
import os
import random

from sqlalchemy import __version__
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import Numeric
from sqlalchemy import String
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session


Base = declarative_base()


class Employee(Base):
    __tablename__ = "employee"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    type = Column(String(50), nullable=False)

    __mapper_args__ = {"polymorphic_on": type}


class Boss(Employee):
    __tablename__ = "boss"

    id = Column(Integer, ForeignKey("employee.id"), primary_key=True)
    golf_average = Column(Numeric)

    __mapper_args__ = {"polymorphic_identity": "boss"}


class Grunt(Employee):
    __tablename__ = "grunt"

    id = Column(Integer, ForeignKey("employee.id"), primary_key=True)
    savings = Column(Numeric)

    employer_id = Column(Integer, ForeignKey("boss.id"))

    employer = relationship(
        "Boss", backref="employees", primaryjoin=Boss.id == employer_id
    )

    __mapper_args__ = {"polymorphic_identity": "grunt"}


if os.path.exists("orm2010.db"):
    os.remove("orm2010.db")
# use a file based database so that cursor.execute() has some
# palpable overhead.
engine = create_engine("sqlite:///orm2010.db")

Base.metadata.create_all(engine)

sess = Session(engine)


def runit_persist(status, factor=1, query_runs=5):
    num_bosses = 100 * factor
    num_grunts = num_bosses * 100

    bosses = [
        Boss(name="Boss %d" % i, golf_average=Decimal(random.randint(40, 150)))
        for i in range(num_bosses)
    ]

    sess.add_all(bosses)
    status("Added %d boss objects" % num_bosses)

    grunts = [
        Grunt(
            name="Grunt %d" % i,
            savings=Decimal(random.randint(5000000, 15000000) / 100),
        )
        for i in range(num_grunts)
    ]
    status("Added %d grunt objects" % num_grunts)

    while grunts:
        # this doesn't associate grunts with bosses evenly,
        # just associates lots of them with a relatively small
        # handful of bosses
        batch_size = 100
        batch_num = (num_grunts - len(grunts)) / batch_size
        boss = sess.query(Boss).filter_by(name="Boss %d" % batch_num).first()
        for grunt in grunts[0:batch_size]:
            grunt.employer = boss

        grunts = grunts[batch_size:]

    sess.commit()
    status("Associated grunts w/ bosses and committed")


def runit_query_runs(status, factor=1, query_runs=5):
    # do some heavier reading
    for i in range(query_runs):
        status("Heavy query run #%d" % (i + 1))

        report = []

        # load all the Grunts, print a report with their name, stats,
        # and their bosses' stats.
        for grunt in sess.query(Grunt):
            report.append(
                (
                    grunt.name,
                    grunt.savings,
                    grunt.employer.name,
                    grunt.employer.golf_average,
                )
            )

        sess.close()  # close out the session


def run_with_profile(runsnake=False, dump=False):
    import cProfile
    import pstats

    filename = "orm2010.profile"

    if os.path.exists("orm2010.profile"):
        os.remove("orm2010.profile")

    def status(msg):
        print(msg)

    cProfile.runctx(
        # "runit_persist(status)",
        "runit_persist(status); runit_query_runs(status)",
        globals(),
        locals(),
        filename,
    )
    stats = pstats.Stats(filename)

    counts_by_methname = {key[2]: stats.stats[key][0] for key in stats.stats}

    print("SQLA Version: %s" % __version__)
    print("Total calls %d" % stats.total_calls)
    print("Total cpu seconds: %.2f" % stats.total_tt)
    print(
        "Total execute calls: %d"
        % counts_by_methname["<method 'execute' of 'sqlite3.Cursor' objects>"]
    )
    print(
        "Total executemany calls: %d"
        % counts_by_methname.get(
            "<method 'executemany' of 'sqlite3.Cursor' objects>", 0
        )
    )

    if dump:
        #        stats.sort_stats("nfl")
        stats.sort_stats("cumtime", "calls")
        stats.print_stats()
    #        stats.print_callers()

    if runsnake:
        os.system("runsnake %s" % filename)


def run_with_time(factor):
    import time

    now = time.time()

    def status(msg):
        print("%d - %s" % (time.time() - now, msg))

    runit_persist(status, factor)

    print("Total time: %d" % (time.time() - now))

    runit_query_runs(status, factor)

    print("Total time: %d" % (time.time() - now))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--profile",
        action="store_true",
        help="run shorter test suite w/ cprofilng",
    )
    parser.add_argument(
        "--dump",
        action="store_true",
        help="dump full call profile (implies --profile)",
    )
    parser.add_argument(
        "--runsnake",
        action="store_true",
        help="invoke runsnakerun (implies --profile)",
    )
    parser.add_argument(
        "--factor",
        type=int,
        default=10,
        help="scale factor, a multiple of how many records to work with.  "
        "defaults to 10",
    )
    args = parser.parse_args()

    args.profile = args.profile or args.dump or args.runsnake

    if args.profile:
        run_with_profile(runsnake=args.runsnake, dump=args.dump)
    else:
        run_with_time(args.factor)
