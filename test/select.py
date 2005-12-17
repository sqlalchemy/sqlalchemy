
from sqlalchemy import *
import sqlalchemy.ansisql as ansisql
import sqlalchemy.databases.postgres as postgres
import sqlalchemy.databases.oracle as oracle

db = ansisql.engine()

from testbase import PersistTest
import unittest, re


table = Table('mytable', db,
    Column('myid', Integer, key = 'id'),
    Column('name', String, key = 'name'),
    Column('description', String, key = 'description'),
)

table2 = Table(
    'myothertable', db,
    Column('otherid', Integer, key='id'),
    Column('othername', String, key='name'),
)

table3 = Table(
    'thirdtable', db,
    Column('userid', Integer, key='id'),
    Column('otherstuff', Integer),
)

table4 = Table(
    'remotetable', db,
    Column('rem_id', Integer, primary_key=True),
    Column('datatype_id', Integer),
    Column('value', String(20)),
    schema = 'remote_owner'
)

users = Table('users', db,
    Column('user_id', Integer, primary_key = True),
    Column('user_name', String(40)),
    Column('password', String(10)),
)

addresses = Table('addresses', db,
    Column('address_id', Integer, primary_key = True),
    Column('user_id', Integer, ForeignKey("users.user_id")),
    Column('street', String(100)),
    Column('city', String(80)),
    Column('state', String(2)),
    Column('zip', String(10))
)


class SQLTest(PersistTest):
    def runtest(self, clause, result, engine = None, params = None, checkparams = None):
        c = clause.compile(engine, params)
        self.echo("\n" + str(c) + repr(c.get_params()))
        cc = re.sub(r'\n', '', str(c))
        self.assert_(cc == result, str(c) + "\n does not match \n" + result)
        if checkparams is not None:
            self.assert_(c.get_params() == checkparams, "params dont match")
            
class SelectTest(SQLTest):


    def testtableselect(self):
        self.runtest(table.select(), "SELECT mytable.myid, mytable.name, mytable.description FROM mytable")

        self.runtest(select([table, table2]), "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, \
myothertable.othername FROM mytable, myothertable")

    def testsubquery(self):

        # TODO: a subquery in a column clause.
        #self.runtest(
        #    select([table, select([table2.c.id])]),
        #    """"""
        #)

        s = select([table], table.c.name == 'jack')
        self.runtest(
            select(
                [s],
                s.c.id == 7
            )
            ,
        "SELECT myid, name, description FROM (SELECT mytable.myid, mytable.name, mytable.description FROM mytable WHERE mytable.name = :mytable_name) WHERE myid = :myid")
        
        sq = select([table])
        self.runtest(
            sq.select(),
            "SELECT myid, name, description FROM (SELECT mytable.myid, mytable.name, mytable.description FROM mytable)"
        )
        
        sq = subquery(
            'sq',
            [table],
        )

        self.runtest(
            sq.select(sq.c.id == 7), 
            "SELECT sq.myid, sq.name, sq.description FROM \
(SELECT mytable.myid, mytable.name, mytable.description FROM mytable) AS sq WHERE sq.myid = :sq_myid"
        )
        
        sq = subquery(
            'sq',
            [table, table2],
            and_(table.c.id ==7, table2.c.id==table.c.id),
            use_labels = True
        )
        
        sqstring = "SELECT mytable.myid AS mytable_myid, mytable.name AS mytable_name, \
mytable.description AS mytable_description, myothertable.otherid AS myothertable_otherid, \
myothertable.othername AS myothertable_othername FROM mytable, myothertable \
WHERE mytable.myid = :mytable_myid AND myothertable.otherid = mytable.myid"

        self.runtest(sq.select(), "SELECT sq.mytable_myid, sq.mytable_name, sq.mytable_description, sq.myothertable_otherid, \
sq.myothertable_othername FROM (" + sqstring + ") AS sq")

        sq2 = subquery(
            'sq2',
            [sq],
            use_labels = True
        )

        self.runtest(sq2.select(), "SELECT sq2.sq_mytable_myid, sq2.sq_mytable_name, sq2.sq_mytable_description, \
sq2.sq_myothertable_otherid, sq2.sq_myothertable_othername FROM \
(SELECT sq.mytable_myid AS sq_mytable_myid, sq.mytable_name AS sq_mytable_name, \
sq.mytable_description AS sq_mytable_description, sq.myothertable_otherid AS sq_myothertable_otherid, \
sq.myothertable_othername AS sq_myothertable_othername FROM (" + sqstring + ") AS sq) AS sq2")
        
        
    def testand(self):
        self.runtest(
            select(['*'], and_(table.c.id == 12, table.c.name=='asdf', table2.c.name == 'foo', "sysdate() = today()")), 
            "SELECT * FROM mytable, myothertable WHERE mytable.myid = :mytable_myid AND mytable.name = :mytable_name AND myothertable.othername = :myothertable_othername AND sysdate() = today()"
        )

    def testor(self):
        self.runtest(
            select([table], and_(
                table.c.id == 12,
                or_(table2.c.name=='asdf', table2.c.name == 'foo', table2.c.id == 9),
                "sysdate() = today()", 
            )),
            "SELECT mytable.myid, mytable.name, mytable.description FROM mytable, myothertable WHERE mytable.myid = :mytable_myid AND (myothertable.othername = :myothertable_othername OR myothertable.othername = :myothertable_othername_1 OR myothertable.otherid = :myothertable_otherid) AND sysdate() = today()",
            checkparams = {'myothertable_othername': 'asdf', 'myothertable_othername_1':'foo', 'myothertable_otherid': 9, 'mytable_myid': 12}
        )

    def testoperators(self):
        self.runtest(
            table.select((table.c.id != 12) & ~(table.c.name=='john')), 
            "SELECT mytable.myid, mytable.name, mytable.description FROM mytable WHERE mytable.myid != :mytable_myid AND NOT (mytable.name = :mytable_name)"
        )
        
        self.runtest(
            literal("a") + literal("b") * literal("c"), ":literal + :literal_1 * :literal_2", db
        )

    def testmultiparam(self):
        self.runtest(
            select(["*"], or_(table.c.id == 12, table.c.id=='asdf', table.c.id == 'foo')), 
            "SELECT * FROM mytable WHERE mytable.myid = :mytable_myid OR mytable.myid = :mytable_myid_1 OR mytable.myid = :mytable_myid_2"
        )

    def testorderby(self):
        self.runtest(
            table2.select(order_by = [table2.c.id, asc(table2.c.name)]),
            "SELECT myothertable.otherid, myothertable.othername FROM myothertable ORDER BY myothertable.otherid, myothertable.othername ASC"
        )
    def testalias(self):
        # test the alias for a table.  column names stay the same, table name "changes" to "foo".
        self.runtest(
        select([alias(table, 'foo')])
        ,"SELECT foo.myid, foo.name, foo.description FROM mytable AS foo")
    
        # create a select for a join of two tables.  use_labels means the column names will have
        # labels tablename_columnname, which become the column keys accessible off the Selectable object.
        # also, only use one column from the second table and all columns from the first table.
        q = select([table, table2.c.id], table.c.id == table2.c.id, use_labels = True)
        
        # make an alias of the "selectable".  column names stay the same (i.e. the labels), table name "changes" to "t2view".
        a = alias(q, 't2view')

        # select from that alias, also using labels.  two levels of labels should produce two underscores.
        # also, reference the column "mytable_myid" off of the t2view alias.
        self.runtest(
            a.select(a.c.mytable_myid == 9, use_labels = True),
            "SELECT t2view.mytable_myid AS t2view_mytable_myid, t2view.mytable_name AS t2view_mytable_name, \
t2view.mytable_description AS t2view_mytable_description, t2view.myothertable_otherid AS t2view_myothertable_otherid FROM \
(SELECT mytable.myid AS mytable_myid, mytable.name AS mytable_name, mytable.description AS mytable_description, \
myothertable.otherid AS myothertable_otherid FROM mytable, myothertable \
WHERE mytable.myid = myothertable.otherid) AS t2view WHERE t2view.mytable_myid = :t2view_mytable_myid"
        )
        
    def testtext(self):
        self.runtest(
            text("select * from foo where lala = bar") ,
            "select * from foo where lala = bar",
            engine = db
        )

        self.runtest(select(
            ["foobar(a)", "pk_foo_bar(syslaal)"],
            "a = 12",
            from_obj = ["foobar left outer join lala on foobar.foo = lala.foo"],
            engine = db
        ), 
        "SELECT foobar(a), pk_foo_bar(syslaal) FROM foobar left outer join lala on foobar.foo = lala.foo WHERE a = 12")

        # test building a select query programmatically with text
        s = select()
        s.append_column("column1")
        s.append_column("column2")
        s.append_whereclause("column1=12")
        s.append_whereclause("column2=19")
        s.order_by("column1")
        s.append_from("table1")
        self.runtest(s, "SELECT column1, column2 FROM table1 WHERE column1=12 AND column2=19 ORDER BY column1", db)

    def testtextmix(self):
        self.runtest(select(
            [table, table2.c.id, "sysdate()", "foo, bar, lala"],
            and_(
                "foo.id = foofoo(lala)",
                "datetime(foo) = Today",
                table.c.id == table2.c.id,
            )
        ), 
        "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, sysdate(), foo, bar, lala \
FROM mytable, myothertable WHERE foo.id = foofoo(lala) AND datetime(foo) = Today AND mytable.myid = myothertable.otherid")

    def testtextualsubquery(self):
        self.runtest(select(
            [alias(table, 't'), "foo.f"],
            "foo.f = t.id",
            from_obj = ["(select f from bar where lala=heyhey) foo"]
        ), 
        "SELECT t.myid, t.name, t.description, foo.f FROM mytable AS t, (select f from bar where lala=heyhey) foo WHERE foo.f = t.id")

    def testliteral(self):
        self.runtest(select([literal("foo") + literal("bar")], from_obj=[table]), 
            "SELECT :literal + :literal_1 FROM mytable", engine=db)

    def testfunction(self):
        self.runtest(func.lala(3, 4, literal("five"), table.c.id) * table2.c.id, 
            "lala(:lala, :lala_1, :literal, mytable.myid) * myothertable.otherid", engine=db)

    def testjoin(self):
        self.runtest(
            join(table2, table, table.c.id == table2.c.id).select(),
            "SELECT myothertable.otherid, myothertable.othername, mytable.myid, mytable.name, \
mytable.description FROM myothertable JOIN mytable ON mytable.myid = myothertable.otherid"
        )

        self.runtest(
            select(
             [table],
                from_obj = [join(table, table2, table.c.id == table2.c.id)]
            ),
        "SELECT mytable.myid, mytable.name, mytable.description FROM mytable JOIN myothertable ON mytable.myid = myothertable.otherid")

        self.runtest(
            select(
                [join(join(table, table2, table.c.id == table2.c.id), table3, table.c.id == table3.c.id)
            ]),
            "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername, thirdtable.userid, thirdtable.otherstuff FROM mytable JOIN myothertable ON mytable.myid = myothertable.otherid JOIN thirdtable ON mytable.myid = thirdtable.userid"
        )
        
    def testmultijoin(self):
        self.runtest(
                select([table, table2, table3],
                
                from_obj = [join(table, table2, table.c.id == table2.c.id).outerjoin(table3, table.c.id==table3.c.id)]
                
                #from_obj = [outerjoin(join(table, table2, table.c.id == table2.c.id), table3, table.c.id==table3.c.id)]
                )
                ,"SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername, thirdtable.userid, thirdtable.otherstuff FROM mytable JOIN myothertable ON mytable.myid = myothertable.otherid LEFT OUTER JOIN thirdtable ON mytable.myid = thirdtable.userid"
            )
        self.runtest(
                select([table, table2, table3],
                from_obj = [outerjoin(table, join(table2, table3, table2.c.id == table3.c.id), table.c.id==table2.c.id)]
                )
                ,"SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername, thirdtable.userid, thirdtable.otherstuff FROM mytable LEFT OUTER JOIN (myothertable JOIN thirdtable ON myothertable.otherid = thirdtable.userid) ON mytable.myid = myothertable.otherid"
            )
            
    def testunion(self):
            x = union(
                  select([table], table.c.id == 5),
                  select([table], table.c.id == 12),
                  order_by = [table.c.id],
            )
  
            self.runtest(x, "SELECT mytable.myid, mytable.name, mytable.description \
FROM mytable WHERE mytable.myid = :mytable_myid UNION \
SELECT mytable.myid, mytable.name, mytable.description \
FROM mytable WHERE mytable.myid = :mytable_myid_1 ORDER BY mytable.myid")
  
            self.runtest(
                    union(
                        select([table]),
                        select([table2]),
                        select([table3])
                    )
            ,
            "SELECT mytable.myid, mytable.name, mytable.description \
FROM mytable UNION SELECT myothertable.otherid, myothertable.othername \
FROM myothertable UNION SELECT thirdtable.userid, thirdtable.otherstuff FROM thirdtable")
            
            
    def testouterjoin(self):
        # test an outer join.  the oracle module should take the ON clause of the join and
        # move it up to the WHERE clause of its parent select, and append (+) to all right-hand-side columns
        # within the original onclause, but leave right-hand-side columns unchanged outside of the onclause
        # parameters.
        
        query = select(
                [table, table2],
                and_(
                    table.c.name == 'fred',
                    table.c.id == 10,
                    table2.c.name != 'jack',
                    "EXISTS (select yay from foo where boo = lar)"
                ),
                from_obj = [ outerjoin(table, table2, table.c.id == table2.c.id) ]
                )
                
        self.runtest(query, 
            "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername \
FROM mytable LEFT OUTER JOIN myothertable ON mytable.myid = myothertable.otherid \
WHERE mytable.name = %(mytable_name)s AND mytable.myid = %(mytable_myid)s AND \
myothertable.othername != %(myothertable_othername)s AND \
EXISTS (select yay from foo where boo = lar)",
            engine = postgres.engine({}))


        self.runtest(query, 
            "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername \
FROM mytable, myothertable WHERE mytable.myid = myothertable.otherid(+) AND \
mytable.name = :mytable_name AND mytable.myid = :mytable_myid AND \
myothertable.othername != :myothertable_othername AND EXISTS (select yay from foo where boo = lar)",
            engine = oracle.engine({}, use_ansi = False))

    def testbindparam(self):
        self.runtest(select(
                    [table, table2],
                    and_(table.c.id == table2.c.id,
                    table.c.name == bindparam('mytablename'),
                    )
                ),
                "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername \
FROM mytable, myothertable WHERE mytable.myid = myothertable.otherid AND mytable.name = :mytablename"
                )

        # check that the bind params sent along with a compile() call
        # get preserved when the params are retreived later
        s = select([table], table.c.id == bindparam('test'))
        c = s.compile(parameters = {'test' : 7})
        self.assert_(c.get_params() == {'test' : 7})

    def testcorrelatedsubquery(self):
        self.runtest(
            table.select(table.c.id == select([table2.c.id], table.c.name == table2.c.name)),
            "SELECT mytable.myid, mytable.name, mytable.description FROM mytable WHERE mytable.myid = (SELECT myothertable.otherid FROM myothertable WHERE mytable.name = myothertable.othername)"
        )

        self.runtest(
            table.select(exists([1], table2.c.id == table.c.id)),
            "SELECT mytable.myid, mytable.name, mytable.description FROM mytable WHERE EXISTS (SELECT 1 FROM myothertable WHERE myothertable.otherid = mytable.myid)"
        )

        talias = table.alias('ta')
        s = subquery('sq2', [talias], exists([1], table2.c.id == talias.c.id))
        self.runtest(
            select([s, table])
            ,"SELECT sq2.myid, sq2.name, sq2.description, mytable.myid, mytable.name, mytable.description FROM (SELECT ta.myid, ta.name, ta.description FROM mytable AS ta WHERE EXISTS (SELECT 1 FROM myothertable WHERE myothertable.otherid = ta.myid)) AS sq2, mytable")

        s = select([addresses.c.street], addresses.c.user_id==users.c.user_id).alias('s')
        self.runtest(
            select([users, s.c.street], from_obj=[s]),
            """SELECT users.user_id, users.user_name, users.password, s.street FROM users, (SELECT addresses.street FROM addresses WHERE addresses.user_id = users.user_id) AS s""")

    def testin(self):
        self.runtest(select([table], table.c.id.in_(1, 2, 3)),
        "SELECT mytable.myid, mytable.name, mytable.description FROM mytable WHERE mytable.myid IN (1, 2, 3)")

        self.runtest(select([table], table.c.id.in_(select([table2.c.id]))),
        "SELECT mytable.myid, mytable.name, mytable.description FROM mytable WHERE mytable.myid IN (SELECT myothertable.otherid FROM myothertable)")
    
        
        
class CRUDTest(SQLTest):
    def testinsert(self):
        # generic insert, will create bind params for all columns
        self.runtest(insert(table), "INSERT INTO mytable (myid, name, description) VALUES (:myid, :name, :description)")

        # insert with user-supplied bind params for specific columns,
        # cols provided literally
        self.runtest(
            insert(table, {table.c.id : bindparam('userid'), table.c.name : bindparam('username')}), 
            "INSERT INTO mytable (myid, name) VALUES (:userid, :username)")
        
        # insert with user-supplied bind params for specific columns, cols
        # provided as strings
        self.runtest(
            insert(table, dict(id = 3, name = 'jack')), 
            "INSERT INTO mytable (myid, name) VALUES (:myid, :name)"
        )

        # test with a tuple of params instead of named
        self.runtest(
            insert(table, (3, 'jack', 'mydescription')), 
            "INSERT INTO mytable (myid, name, description) VALUES (:myid, :name, :description)",
            checkparams = {'myid':3, 'name':'jack', 'description':'mydescription'}
        )
        
    def testupdate(self):
        self.runtest(update(table, table.c.id == 7), "UPDATE mytable SET name=:name WHERE mytable.myid = :mytable_myid", params = {table.c.name:'fred'})
        self.runtest(update(table, table.c.id == 7), "UPDATE mytable SET name=:name WHERE mytable.myid = :mytable_myid", params = {'name':'fred'})
        self.runtest(update(table, values = {table.c.name : table.c.id}), "UPDATE mytable SET name=mytable.myid")
        self.runtest(update(table, whereclause = table.c.name == bindparam('crit'), values = {table.c.name : 'hi'}), "UPDATE mytable SET name=:name WHERE mytable.name = :crit", params = {'crit' : 'notthere'})
        self.runtest(update(table, table.c.id == 12, values = {table.c.name : table.c.id}), "UPDATE mytable SET name=mytable.myid, description=:description WHERE mytable.myid = :mytable_myid", params = {'description':'test'})
        self.runtest(update(table, table.c.id == 12, values = {table.c.id : 9}), "UPDATE mytable SET myid=:myid, description=:description WHERE mytable.myid = :mytable_myid", params = {'mytable_myid': 12, 'myid': 9, 'description': 'test'})
        s = table.update(table.c.id == 12, values = {table.c.name : 'lala'})
        print str(s)
        c = s.compile(parameters = {'mytable_id':9,'name':'h0h0'})
        print str(c)
        self.assert_(str(s) == str(c))
        
    def testupdateexpression(self):
        self.runtest(update(table, 
            (table.c.id == func.hoho(4)) &
            (table.c.name == literal('foo') + table.c.name + literal('lala')),
            values = {
            table.c.name : table.c.name + "lala",
            table.c.id : func.do_stuff(table.c.id, literal('hoho'))
            }), "UPDATE mytable SET myid=(do_stuff(mytable.myid, :literal_2)), name=(mytable.name + :mytable_name) WHERE mytable.myid = hoho(:hoho) AND mytable.name = :literal + mytable.name + :literal_1")
        
    def testcorrelatedupdate(self):
        # test against a straight text subquery
        u = update(table, values = {table.c.name : text("select name from mytable where id=mytable.id")})
        self.runtest(u, "UPDATE mytable SET name=(select name from mytable where id=mytable.id)")
        
        # test against a regular constructed subquery
        s = select([table2], table2.c.id == table.c.id)
        u = update(table, table.c.name == 'jack', values = {table.c.name : s})
        self.runtest(u, "UPDATE mytable SET name=(SELECT myothertable.otherid, myothertable.othername FROM myothertable WHERE myothertable.otherid = mytable.myid) WHERE mytable.name = :mytable_name")
        
    def testdelete(self):
        self.runtest(delete(table, table.c.id == 7), "DELETE FROM mytable WHERE mytable.myid = :mytable_myid")
        
class SchemaTest(SQLTest):
    def testselect(self):
        self.runtest(table4.select(), "SELECT remotetable.rem_id, remotetable.datatype_id, remotetable.value FROM remote_owner.remotetable")
        self.runtest(table4.select(and_(table4.c.datatype_id==7, table4.c.value=='hi')), "SELECT remotetable.rem_id, remotetable.datatype_id, remotetable.value FROM remote_owner.remotetable WHERE remotetable.datatype_id = :remotetable_datatype_id AND remotetable.value = :remotetable_value")

        s = table4.select(and_(table4.c.datatype_id==7, table4.c.value=='hi'))
        s.use_labels = True
        self.runtest(s, "SELECT remotetable.rem_id AS remotetable_rem_id, remotetable.datatype_id AS remotetable_datatype_id, remotetable.value AS remotetable_value FROM remote_owner.remotetable WHERE remotetable.datatype_id = :remotetable_datatype_id AND remotetable.value = :remotetable_value")

    def testalias(self):
        a = alias(table4, 'remtable')
        self.runtest(a.select(a.c.datatype_id==7), "SELECT remtable.rem_id, remtable.datatype_id, remtable.value FROM remote_owner.remotetable AS remtable WHERE remtable.datatype_id = :remtable_datatype_id")
        
    def testupdate(self):
        self.runtest(table4.update(table4.c.value=='test', values={table4.c.datatype_id:12}), "UPDATE remote_owner.remotetable SET datatype_id=:datatype_id WHERE remotetable.value = :remotetable_value")
        
    def testinsert(self):
        self.runtest(table4.insert(values=(2, 5, 'test')), "INSERT INTO remote_owner.remotetable (rem_id, datatype_id, value) VALUES (:rem_id, :datatype_id, :value)")
        
if __name__ == "__main__":
    unittest.main()        
