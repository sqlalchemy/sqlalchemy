"""illlustrates techniques for dealing with very large collections"""

from sqlalchemy import *
meta = BoundMetaData('sqlite://', echo=True)

org_table = Table('organizations', meta, 
    Column('org_id', Integer, primary_key=True),
    Column('org_name', String(50), nullable=False, key='name'))
    
member_table = Table('members', meta,
    Column('member_id', Integer, primary_key=True),
    Column('member_name', String(50), nullable=False, key='name'),
    Column('org_id', Integer, ForeignKey('organizations.org_id')))
meta.create_all()    
    
class Organization(object):
    def __init__(self, name):
        self.name = name
    def find_members(self, criterion):
        """locate a subset of the members associated with this Organization"""
        return object_session(self).query(Member).select(and_(member_table.c.name.like(criterion), org_table.c.org_id==self.org_id), from_obj=[org_table.join(member_table)])
    
class Member(object):
    def __init__(self, name):
        self.name = name

# note that we can also place "ON DELETE CASCADE" on the tables themselves,
# instead of using this extension
class DeleteMemberExt(MapperExtension):
    """will delete child Member objects in one pass when Organizations are deleted"""
    def before_delete(self, mapper, connection, instance):
        connection.execute(member_table.delete(member_table.c.org_id==instance.org_id))

mapper(Organization, org_table, extension=DeleteMemberExt(), properties = {
    # set up the relationship with "lazy=None" so no loading occurs (even lazily),
    # "cascade='all, delete-orphan'" to declare Member objects as local to their parent Organization,
    # "passive_deletes=True" so that the "delete, delete-orphan" cascades do not load in the child objects
    # upon deletion
    'members' : relation(Member, lazy=None, passive_deletes=True, cascade="all, delete-orphan")
})

mapper(Member, member_table)

sess = create_session()

# create org with some members
org = Organization('org one')
org.members.append(Member('member one'))
org.members.append(Member('member two'))
org.members.append(Member('member three'))

sess.save(org)

print "-------------------------\nflush one - save org + 3 members"
sess.flush()
sess.clear()

# reload. load the org and some child members
print "-------------------------\nload subset of members"
org = sess.query(Organization).get(org.org_id)
members = org.find_members('%member t%')
print members

sess.clear()


# reload.  create some more members and flush, without loading any of the original members
org = sess.query(Organization).get(org.org_id)
org.members.append(Member('member four'))
org.members.append(Member('member five'))
org.members.append(Member('member six'))

print "-------------------------\nflush two - save 3 more members"
sess.flush()

sess.clear()
org = sess.query(Organization).get(org.org_id)

# now delete.  note that this will explictily delete members four, five and six because they are in the session,
# but will not issue individual deletes for members one, two and three, nor will it load them.
sess.delete(org)
print "-------------------------\nflush three - delete org, delete members in one statement"
sess.flush()

