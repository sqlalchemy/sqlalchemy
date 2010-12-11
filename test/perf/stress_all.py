# -*- encoding: utf8 -*-
from datetime import *
from sqlalchemy.util.compat import decimal
#from fastdec import mpd as Decimal
from cPickle import dumps, loads

#from sqlalchemy.dialects.postgresql.base import ARRAY

from stresstest import *

# ---
test_types = False
test_methods = True
test_pickle = False
test_orm = False
# ---
verbose = True

def values_results(raw_results):
    return [tuple(r.values()) for r in raw_results]

def getitem_str_results(raw_results):
    return [
        (r['id'],
         r['field0'], r['field1'], r['field2'], r['field3'], r['field4'],
         r['field5'], r['field6'], r['field7'], r['field8'], r['field9'])
         for r in raw_results]

def getitem_fallback_results(raw_results):
    return [
        (r['ID'],
         r['FIELD0'], r['FIELD1'], r['FIELD2'], r['FIELD3'], r['FIELD4'],
         r['FIELD5'], r['FIELD6'], r['FIELD7'], r['FIELD8'], r['FIELD9'])
         for r in raw_results]

def getitem_int_results(raw_results):
    return [
        (r[0],
         r[1], r[2], r[3], r[4], r[5],
         r[6], r[7], r[8], r[9], r[10])
         for r in raw_results]

def getitem_long_results(raw_results):
    return [
        (r[0L],
         r[1L], r[2L], r[3L], r[4L], r[5L],
         r[6L], r[7L], r[8L], r[9L], r[10L])
         for r in raw_results]

def getitem_obj_results(raw_results):
    c = test_table.c
    fid, f0, f1, f2, f3, f4, f5, f6, f7, f8, f9 = (
        c.id, c.field0, c.field1, c.field2, c.field3, c.field4,
        c.field5, c.field6, c.field7, c.field8, c.field9)
    return [
        (r[fid],
         r[f0], r[f1], r[f2], r[f3], r[f4],
         r[f5], r[f6], r[f7], r[f8], r[f9])
         for r in raw_results]

def slice_results(raw_results):
    return [row[0:6] + row[6:11] for row in raw_results]

# ---------- #
# Test types #
# ---------- #

# Array
#def genarrayvalue(rnum, fnum):
#    return [fnum, fnum + 1, fnum + 2]
#arraytest = (ARRAY(Integer), genarrayvalue,
#             dict(num_fields=100, num_records=1000,
#                  engineurl='postgresql:///test'))

# Boolean
def genbooleanvalue(rnum, fnum):
    if rnum % 4:
        return bool(fnum % 2)
    else:
        return None
booleantest = (Boolean, genbooleanvalue, dict(num_records=100000))

# Datetime
def gendatetimevalue(rnum, fnum):
    return (rnum % 4) and datetime(2005, 3, 3) or None
datetimetest = (DateTime, gendatetimevalue, dict(num_records=10000))

# Decimal
def gendecimalvalue(rnum, fnum):
    if rnum % 4:
        return Decimal(str(0.25 * fnum))
    else:
        return None
decimaltest = (Numeric(10, 2), gendecimalvalue, dict(num_records=10000))

# Interval

# no microseconds because Postgres does not seem to support it
from_epoch = timedelta(14643, 70235)
def genintervalvalue(rnum, fnum):
    return from_epoch
intervaltest = (Interval, genintervalvalue,
                dict(num_fields=2, num_records=100000))

# PickleType
def genpicklevalue(rnum, fnum):
    return (rnum % 4) and {'str': "value%d" % fnum, 'int': rnum} or None
pickletypetest = (PickleType, genpicklevalue,
                  dict(num_fields=1, num_records=100000))

# TypeDecorator
class MyIntType(TypeDecorator):
    impl = Integer

    def process_bind_param(self, value, dialect):
        return value * 10

    def process_result_value(self, value, dialect):
        return value / 10

    def copy(self):
        return MyIntType()

def genmyintvalue(rnum, fnum):
    return rnum + fnum
typedecoratortest = (MyIntType, genmyintvalue,
                     dict(num_records=100000))

# Unicode
def genunicodevalue(rnum, fnum):
    return (rnum % 4) and (u"value%d" % fnum) or None
unicodetest = (Unicode(20, assert_unicode=False), genunicodevalue,
               dict(num_records=100000))
#               dict(engineurl='mysql:///test', freshdata=False))

# do the tests
if test_types:
    tests = [booleantest, datetimetest, decimaltest, intervaltest,
             pickletypetest, typedecoratortest, unicodetest]
    for engineurl in ('postgresql://scott:tiger@localhost/test', 
                        'sqlite://', 'mysql://scott:tiger@localhost/test'):
        print "\n%s\n" % engineurl
        for datatype, genvalue, kwargs in tests:
            print "%s:" % getattr(datatype, '__name__',
                                  datatype.__class__.__name__),
            profile_and_time_dbfunc(iter_results, datatype, genvalue,
                                    profile=False, engineurl=engineurl,
                                    verbose=verbose, **kwargs)

# ---------------------- #
# test row proxy methods #
# ---------------------- #

if test_methods:
    methods = [iter_results, values_results, getattr_results,
               getitem_str_results, getitem_fallback_results,
               getitem_int_results, getitem_long_results, getitem_obj_results,
               slice_results]
    for engineurl in ('postgresql://scott:tiger@localhost/test', 
                       'sqlite://', 'mysql://scott:tiger@localhost/test'):
        print "\n%s\n" % engineurl
        test_table = prepare(Unicode(20, assert_unicode=False),
                             genunicodevalue,
                             num_fields=10, num_records=100000,
                             verbose=verbose, engineurl=engineurl)
        for method in methods:
            print "%s:" % method.__name__,
            time_dbfunc(test_table, method, genunicodevalue,
                        num_fields=10, num_records=100000, profile=False,
                        verbose=verbose)

# --------------------------------
# test pickling Rowproxy instances
# --------------------------------

def pickletofile_results(raw_results):
    from cPickle import dump, load
    for protocol in (0, 1, 2):
        print "dumping protocol %d..." % protocol
        f = file('noext.pickle%d' % protocol, 'wb')
        dump(raw_results, f, protocol)
        f.close()
    return raw_results

def pickle_results(raw_results):
    return loads(dumps(raw_results, 2))

def pickle_meta(raw_results):
    pickled = dumps(raw_results[0]._parent, 2)
    metadata = loads(pickled)
    return raw_results

def pickle_rows(raw_results):
    return [loads(dumps(row, 2)) for row in raw_results]

if test_pickle:
    test_table = prepare(Unicode, genunicodevalue,
                         num_fields=10, num_records=10000)
    funcs = [pickle_rows, pickle_results]
    for func in funcs:
        print "%s:" % func.__name__,
        time_dbfunc(test_table, func, genunicodevalue,
                    num_records=10000, profile=False, verbose=verbose)

# --------------------------------
# test ORM
# --------------------------------

if test_orm:
    from sqlalchemy.orm import *

    class Test(object):
        pass

    Session = sessionmaker()
    session = Session()

    def get_results():
        return session.query(Test).all()
    print "ORM:",
    for engineurl in ('postgresql:///test', 'sqlite://', 'mysql:///test'):
        print "\n%s\n" % engineurl
        profile_and_time_dbfunc(getattr_results, Unicode(20), genunicodevalue,
                                class_=Test, getresults_func=get_results,
                                engineurl=engineurl, #freshdata=False,
                                num_records=10000, verbose=verbose)
