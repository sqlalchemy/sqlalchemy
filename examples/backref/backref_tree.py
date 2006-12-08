from sqlalchemy import *

metadata = BoundMetaData('sqlite:///', echo=True)

class Tree(object):
    def __init__(self, name='', father=None):
        self.name = name
        self.father = father
    def __str__(self):
        return '<TreeNode: %s>' % self.name
    def __repr__(self):
        return self.__str__()
        
table = Table('tree', metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String(64), nullable=False),
              Column('father_id', Integer, ForeignKey('tree.id'), nullable=True))
table.create()

mapper(Tree, table,
              properties={
                'childs':relation(Tree, remote_side=table.c.father_id, primaryjoin=table.c.father_id==table.c.id,  backref=backref('father', remote_side=table.c.id))},
            )

root = Tree('root')
child1 = Tree('child1', root)
child2 = Tree('child2', root)
child3 = Tree('child3', child1)

child4 = Tree('child4')
child1.childs.append(child4)

session = create_session()
session.save(root)
session.flush()

print root.childs
print child1.childs
print child2.childs
print child2.father
print child3.father
