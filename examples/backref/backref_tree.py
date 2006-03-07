from sqlalchemy import *
import sqlalchemy.attributes as attributes

engine = create_engine('sqlite://', echo=True)

class Tree(object):
    def __init__(self, name='', father=None):
        self.name = name
        self.father = father
    def __str__(self):
        return '<TreeNode: %s>' % self.name
    def __repr__(self):
        return self.__str__()
        
table = Table('tree', engine,
              Column('id', Integer, primary_key=True),
              Column('name', String(64), nullable=False),
              Column('father_id', Integer, ForeignKey('tree.id'), nullable=True),)

assign_mapper(Tree, table,
              properties={
     # set up a backref using a string
     #'father':relation(Tree, foreignkey=table.c.id,primaryjoin=table.c.father_id==table.c.id,  backref='childs')},
                
     # or set up using the backref() function, which allows arguments to be passed
     'childs':relation(Tree, foreignkey=table.c.father_id, primaryjoin=table.c.father_id==table.c.id,  backref=backref('father', uselist=False, foreignkey=table.c.id))},
            )

table.create()
root = Tree('root')
child1 = Tree('child1', root)
child2 = Tree('child2', root)
child3 = Tree('child3', child1)

objectstore.commit()

print root.childs
print child1.childs
print child2.childs
print child2.father
print child3.father
