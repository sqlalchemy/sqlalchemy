# monkeypatch the "cdecimal" library in.
# this is a drop-in replacement for "decimal".
# All SQLA versions support cdecimal except
# for the MS-SQL dialect, which is fixed in 0.7
try:
    import cdecimal
    import sys
    sys.modules['decimal'] = cdecimal
except ImportError:
    pass

from sqlalchemy import __version__
from sqlalchemy import Column, Integer, create_engine, ForeignKey, \
    String, Numeric

if __version__ < "0.6":
    from sqlalchemy.orm.session import Session
    from sqlalchemy.orm import relation as relationship
else:
    from sqlalchemy.orm import Session, relationship

from sqlalchemy.ext.declarative import declarative_base
import random
import os
from decimal import Decimal

Base = declarative_base()

class Employee(Base):
    __tablename__ = 'employee'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    type = Column(String(50), nullable=False)

    __mapper_args__ = {'polymorphic_on':type}

class Boss(Employee):
    __tablename__ = 'boss'

    id = Column(Integer, ForeignKey('employee.id'), primary_key=True)
    golf_average = Column(Numeric)

    __mapper_args__ = {'polymorphic_identity':'boss'}

class Grunt(Employee):
    __tablename__ = 'grunt'

    id = Column(Integer, ForeignKey('employee.id'), primary_key=True)
    savings = Column(Numeric)

    employer_id = Column(Integer, ForeignKey('boss.id'))

    # Configure an 'employer' relationship, where Grunt references 
    # Boss.  This is a joined-table subclass to subclass relationship, 
    # which is a less typical case.

    # In 0.7, "Boss.id" is the "id" column of "boss", as would be expected.
    if __version__ >= "0.7":
        employer = relationship("Boss", backref="employees", 
                                    primaryjoin=Boss.id==employer_id)

    # Prior to 0.7, "Boss.id" is the "id" column of "employee".
    # Long story.  So we hardwire the relationship against the "id"
    # column of Boss' table.
    elif __version__ >= "0.6":
        employer = relationship("Boss", backref="employees", 
                                primaryjoin=Boss.__table__.c.id==employer_id)

    # In 0.5, the many-to-one loader wouldn't recognize the above as a 
    # simple "identity map" fetch.  So to give 0.5 a chance to emit
    # the same amount of SQL as 0.6, we hardwire the relationship against
    # "employee.id" to work around the bug.
    else:
        employer = relationship("Boss", backref="employees", 
                                primaryjoin=Employee.__table__.c.id==employer_id, 
                                foreign_keys=employer_id)

    __mapper_args__ = {'polymorphic_identity':'grunt'}

if os.path.exists('orm2010.db'):
    os.remove('orm2010.db')
# use a file based database so that cursor.execute() has some 
# palpable overhead.
engine = create_engine('sqlite:///orm2010.db')

Base.metadata.create_all(engine)

sess = Session(engine)

def runit():
    # create 1000 Boss objects.
    bosses = [
        Boss(
            name="Boss %d" % i, 
            golf_average=Decimal(random.randint(40, 150))
        )
        for i in xrange(1000)
    ]

    sess.add_all(bosses)


    # create 10000 Grunt objects.
    grunts = [
        Grunt(
            name="Grunt %d" % i,
            savings=Decimal(random.randint(5000000, 15000000) / 100)
        )
        for i in xrange(10000)
    ]

    # Assign each Grunt a Boss.  Look them up in the DB
    # to simulate a little bit of two-way activity with the 
    # DB while we populate.  Autoflush occurs on each query.
    # In 0.7 executemany() is used for all the "boss" and "grunt" 
    # tables since priamry key fetching is not needed.
    while grunts:
        boss = sess.query(Boss).\
                    filter_by(name="Boss %d" % (101 - len(grunts) / 100)).\
                    first()
        for grunt in grunts[0:100]:
            grunt.employer = boss

        grunts = grunts[100:]

    sess.commit()

    report = []

    # load all the Grunts, print a report with their name, stats,
    # and their bosses' stats.
    for grunt in sess.query(Grunt):
        # here, the overhead of a many-to-one fetch of 
        # "grunt.employer" directly from the identity map 
        # is less than half of that of 0.6.
        report.append((
                        grunt.name, 
                        grunt.savings, 
                        grunt.employer.name, 
                        grunt.employer.golf_average
                    ))

import cProfile, os, pstats

filename = "orm2010.profile"
cProfile.runctx('runit()', globals(), locals(), filename)
stats = pstats.Stats(filename)

counts_by_methname = dict((key[2], stats.stats[key][0]) for key in stats.stats)

print "SQLA Version: %s" % __version__
print "Total calls %d" % stats.total_calls
print "Total cpu seconds: %.2f" % stats.total_tt
print 'Total execute calls: %d' \
    % counts_by_methname["<method 'execute' of 'sqlite3.Cursor' "
                         "objects>"]
print 'Total executemany calls: %d' \
    % counts_by_methname.get("<method 'executemany' of 'sqlite3.Cursor' "
                         "objects>", 0)

os.system("runsnake %s" % filename)

# SQLA Version: 0.7b1
# Total calls 4956750
# Total execute calls: 11201
# Total executemany calls: 101

# SQLA Version: 0.6.6
# Total calls 7963214
# Total execute calls: 22201
# Total executemany calls: 0

# SQLA Version: 0.5.8
# Total calls 10556480
# Total execute calls: 22201
# Total executemany calls: 0









