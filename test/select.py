
import sqlalchemy.ansisql as ansisql
import sqlalchemy.databases.postgres as postgres
import sqlalchemy.databases.oracle as oracle

db = ansisql.engine()

from sqlalchemy.sql import *
from sqlalchemy.schema import *

from testbase import PersistTest
import unittest, re

class SelectTest(PersistTest):
    
    def setUp(self):

        self.table = Table('mytable', db,
            Column('myid', 3, key = 'id'),
            Column('name', 4, key = 'name'),
            Column('description', 4, key = 'description'),
        )

        self.table2 = Table(
            'myothertable', db,
            Column('otherid',3, key='id'),
            Column('othername', 4, key='name'),
        )

        self.table3 = Table(
            'thirdtable', db,
            Column('userid', 5, key='id'),
            Column('otherstuff', 5),
        )

    
    def testoperator(self):
        return
        table = Table(
            'mytable',
            Column('myid',3, key='id'),
            Column('name', 4)
        )

        print (table.c.id == 5)

    def testtext(self):
        self.runtest(
            textclause("select * from foo where lala = bar") ,
            "select * from foo where lala = bar",
            engine = db
        )
    
    def testtableselect(self):
        self.runtest(self.table.select(), "SELECT mytable.myid, mytable.name, mytable.description FROM mytable")

        self.runtest(select([self.table, self.table2]), "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, \
myothertable.othername FROM mytable, myothertable")
        
    def testsubquery(self):
    
        s = select([self.table], self.table.c.name == 'jack')    
        self.runtest(
            select(
                [s],
                s.c.id == 7
            )
            ,
        "SELECT myid, name, description FROM (SELECT mytable.myid, mytable.name, mytable.description FROM mytable WHERE mytable.name = :mytable_name) WHERE myid = :myid")
        
        sq = Select([self.table])
        self.runtest(
            sq.select(),
            "SELECT myid, name, description FROM (SELECT mytable.myid, mytable.name, mytable.description FROM mytable)"
        )
        
        sq = subquery(
            'sq',
            [self.table],
        )

        self.runtest(
            sq.select(sq.c.id == 7), 
            "SELECT sq.myid, sq.name, sq.description FROM \
(SELECT mytable.myid, mytable.name, mytable.description FROM mytable) sq WHERE sq.myid = :sq_myid"
        )
        
        sq = subquery(
            'sq',
            [self.table, self.table2],
            and_(self.table.c.id ==7, self.table2.c.id==self.table.c.id),
            use_labels = True
        )
        
        sqstring = "SELECT mytable.myid AS mytable_myid, mytable.name AS mytable_name, \
mytable.description AS mytable_description, myothertable.otherid AS myothertable_otherid, \
myothertable.othername AS myothertable_othername FROM mytable, myothertable \
WHERE mytable.myid = :mytable_myid AND myothertable.otherid = mytable.myid"

        self.runtest(sq.select(), "SELECT sq.mytable_myid, sq.mytable_name, sq.mytable_description, sq.myothertable_otherid, \
sq.myothertable_othername FROM (" + sqstring + ") sq")

        sq2 = subquery(
            'sq2',
            [sq],
            use_labels = True
        )

        self.runtest(sq2.select(), "SELECT sq2.sq_mytable_myid, sq2.sq_mytable_name, sq2.sq_mytable_description, \
sq2.sq_myothertable_otherid, sq2.sq_myothertable_othername FROM \
(SELECT sq.mytable_myid AS sq_mytable_myid, sq.mytable_name AS sq_mytable_name, \
sq.mytable_description AS sq_mytable_description, sq.myothertable_otherid AS sq_myothertable_otherid, \
sq.myothertable_othername AS sq_myothertable_othername FROM (" + sqstring + ") sq) sq2")
        
        
    def testand(self):
        self.runtest(
            select(['*'], and_(self.table.c.id == 12, self.table.c.name=='asdf', self.table2.c.name == 'foo', "sysdate() = today()")), 
            "SELECT * FROM mytable, myothertable WHERE mytable.myid = :mytable_myid AND mytable.name = :mytable_name AND myothertable.othername = :myothertable_othername AND sysdate() = today()"
        )

    def testor(self):
        self.runtest(
            select([self.table], and_(
                self.table.c.id == 12,
                or_(self.table2.c.name=='asdf', self.table2.c.name == 'foo', self.table2.c.id == 9),
                "sysdate() = today()", 
            )),
            "SELECT mytable.myid, mytable.name, mytable.description FROM mytable, myothertable WHERE mytable.myid = :mytable_myid AND (myothertable.othername = :myothertable_othername OR myothertable.othername = :myothertable_othername_1 OR myothertable.otherid = :myothertable_otherid) AND sysdate() = today()"
        )


    def testmultiparam(self):
        self.runtest(
            select(["*"], or_(self.table.c.id == 12, self.table.c.id=='asdf', self.table.c.id == 'foo')), 
            "SELECT * FROM mytable WHERE mytable.myid = :mytable_myid OR mytable.myid = :mytable_myid_1 OR mytable.myid = :mytable_myid_2"
        )

    def testorderby(self):
        self.runtest(
            self.table2.select(order_by = [self.table2.c.id, asc(self.table2.c.name)]),
            "SELECT myothertable.otherid, myothertable.othername FROM myothertable ORDER BY myothertable.otherid, myothertable.othername ASC"
        )
    def testalias(self):
        # test the alias for a table.  column names stay the same, table name "changes" to "foo".
        self.runtest(
        select([alias(self.table, 'foo')])
        ,"SELECT foo.myid, foo.name, foo.description FROM mytable foo")
    
        # create a select for a join of two tables.  use_labels means the column names will have
        # labels tablename_columnname, which become the column keys accessible off the Selectable object.
        # also, only use one column from the second table and all columns from the first table.
        q = select([self.table, self.table2.c.id], self.table.c.id == self.table2.c.id, use_labels = True)
        
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
WHERE mytable.myid = myothertable.otherid) t2view WHERE t2view.mytable_myid = :t2view_mytable_myid"
        )
        
    def testliteral(self):        
        self.runtest(select(
            ["foobar(a)", "pk_foo_bar(syslaal)"],
            "a = 12",
            from_obj = ["foobar left outer join lala on foobar.foo = lala.foo"],
            engine = db
        ), 
        "SELECT foobar(a), pk_foo_bar(syslaal) FROM foobar left outer join lala on foobar.foo = lala.foo WHERE a = 12")

    def testliteralmix(self):
        self.runtest(select(
            [self.table, self.table2.c.id, "sysdate()", "foo, bar, lala"],
            and_(
                "foo.id = foofoo(lala)",
                "datetime(foo) = Today",
                self.table.c.id == self.table2.c.id,
            )
        ), 
        "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, sysdate(), foo, bar, lala \
FROM mytable, myothertable WHERE foo.id = foofoo(lala) AND datetime(foo) = Today AND mytable.myid = myothertable.otherid")

    def testliteralsubquery(self):
        self.runtest(select(
            [alias(self.table, 't'), "foo.f"],
            "foo.f = t.id",
            from_obj = ["(select f from bar where lala=heyhey) foo"]
        ), 
        "SELECT t.myid, t.name, t.description, foo.f FROM mytable t, (select f from bar where lala=heyhey) foo WHERE foo.f = t.id")

    def testjoin(self):
        self.runtest(
            join(self.table2, self.table, self.table.c.id == self.table2.c.id).select(),
            "SELECT myothertable.otherid, myothertable.othername, mytable.myid, mytable.name, mytable.description \
FROM myothertable, mytable WHERE mytable.myid = myothertable.otherid"
        )
        
        self.runtest(
            select(
                [self.table],
                from_obj = [join(self.table, self.table2, self.table.c.id == self.table2.c.id)]
            ),
        "SELECT mytable.myid, mytable.name, mytable.description FROM mytable JOIN myothertable ON mytable.myid = myothertable.otherid")
        
        self.runtest(
            select(
                [join(join(self.table, self.table2, self.table.c.id == self.table2.c.id), self.table3, self.table.c.id == self.table3.c.id)
            ]),
            "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername, thirdtable.userid, thirdtable.otherstuff FROM mytable JOIN myothertable ON mytable.myid = myothertable.otherid JOIN thirdtable ON mytable.myid = thirdtable.userid"
        )
        
    def testmultijoin(self):
        self.runtest(
                select([self.table, self.table2, self.table3],
                from_obj = [outerjoin(join(self.table, self.table2, self.table.c.id == self.table2.c.id), self.table3, self.table.c.id==self.table3.c.id)]
                )
                ,"SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername, thirdtable.userid, thirdtable.otherstuff FROM mytable JOIN myothertable ON mytable.myid = myothertable.otherid LEFT OUTER JOIN thirdtable ON mytable.myid = thirdtable.userid"
            )
            
    def testunion(self):
            x = union(
                  select([self.table], self.table.c.id == 5),
                  select([self.table], self.table.c.id == 12),
                  order_by = [self.table.c.id],
            )
  
            self.runtest(x, "SELECT mytable.myid, mytable.name, mytable.description \
FROM mytable WHERE mytable.myid = :mytable_myid UNION \
SELECT mytable.myid, mytable.name, mytable.description \
FROM mytable WHERE mytable.myid = :mytable_myid_1 ORDER BY mytable.myid")
  
            self.runtest(
                    union(
                        select([self.table]),
                        select([self.table2]),
                        select([self.table3])
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
                [self.table, self.table2],
                and_(
                    self.table.c.name == 'fred',
                    self.table.c.id == 10,
                    self.table2.c.name != 'jack',
                    "EXISTS (select yay from foo where boo = lar)"
                ),
                from_obj = [ outerjoin(self.table, self.table2, self.table.c.id == self.table2.c.id) ]
                )
                
        self.runtest(query, 
            "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername \
FROM mytable LEFT OUTER JOIN myothertable ON mytable.myid = myothertable.otherid \
WHERE mytable.name = :mytable_name AND mytable.myid = :mytable_myid AND \
myothertable.othername != :myothertable_othername AND \
EXISTS (select yay from foo where boo = lar)",
            engine = postgres.engine())


        self.runtest(query, 
            "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername \
FROM mytable, myothertable WHERE mytable.myid = myothertable.otherid(+) AND \
mytable.name = :mytable_name AND mytable.myid = :mytable_myid AND \
myothertable.othername != :myothertable_othername AND EXISTS (select yay from foo where boo = lar)",
            engine = oracle.engine(use_ansi = False))



    def testbindparam(self):
        #return
        self.runtest(select(
                    [self.table, self.table2],
                    and_(self.table.c.id == self.table2.c.id,
                    self.table.c.name == bindparam('mytablename'),
                    )
                ),
                "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername \
FROM mytable, myothertable WHERE mytable.myid = myothertable.otherid AND mytable.name = :mytablename"
                )


    def testinsert(self):
        # generic insert, will create bind params for all columns
        self.runtest(insert(self.table), "INSERT INTO mytable (myid, name, description) VALUES (:myid, :name, :description)")

        # insert with user-supplied bind params for specific columns,
        # cols provided literally
        self.runtest(
            insert(self.table, {self.table.c.id : bindparam('userid'), self.table.c.name : bindparam('username')}), 
            "INSERT INTO mytable (myid, name) VALUES (:userid, :username)")
        
        # insert with user-supplied bind params for specific columns, cols
        # provided as strings
        self.runtest(
            insert(self.table, dict(id = 3, name = 'jack')), 
            "INSERT INTO mytable (myid, name) VALUES (:myid, :name)"
        )
        
        # insert with a subselect provided 
        #self.runtest(
         #   insert(self.table, select([self.table2])),
         #   ""
        #)

    def testupdate(self):
        self.runtest(update(self.table, self.table.c.id == 7), "UPDATE mytable SET name=:name WHERE mytable.myid = :mytable_myid", params = {self.table.c.name:'fred'})
        self.runtest(update(self.table, self.table.c.id == 7), "UPDATE mytable SET name=:name WHERE mytable.myid = :mytable_myid", params = {'name':'fred'})

    def testdelete(self):
        self.runtest(delete(self.table, self.table.c.id == 7), "DELETE FROM mytable WHERE mytable.myid = :mytable_myid")
        
        
    def runtest(self, clause, result, engine = None, params = None):
        c = clause.compile(engine, params)
        print "\n" + str(c) + repr(c.get_params())
        cc = re.sub(r'\n', '', str(c))
        self.assert_(cc == result)

if __name__ == "__main__":
    unittest.main()        
