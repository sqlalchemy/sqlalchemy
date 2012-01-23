
from sqlalchemy import (MetaData, Table, Column, Integer, String, ForeignKey,
                        create_engine)
from sqlalchemy.orm import (mapper, relationship, sessionmaker)


meta = MetaData()

org_table = Table('organizations', meta, 
    Column('org_id', Integer, primary_key=True),
    Column('org_name', String(50), nullable=False, key='name'),
    mysql_engine='InnoDB')

member_table = Table('members', meta,
    Column('member_id', Integer, primary_key=True),
    Column('member_name', String(50), nullable=False, key='name'),
    Column('org_id', Integer, ForeignKey('organizations.org_id', ondelete="CASCADE")),
    mysql_engine='InnoDB')


class Organization(object):
    def __init__(self, name):
        self.name = name

class Member(object):
    def __init__(self, name):
        self.name = name

mapper(Organization, org_table, properties = {
    'members' : relationship(Member, 
        # Organization.members will be a Query object - no loading
        # of the entire collection occurs unless requested
        lazy="dynamic", 

        # Member objects "belong" to their parent, are deleted when 
        # removed from the collection
        cascade="all, delete-orphan",

        # "delete, delete-orphan" cascade does not load in objects on delete,
        # allows ON DELETE CASCADE to handle it.
        # this only works with a database that supports ON DELETE CASCADE - 
        # *not* sqlite or MySQL with MyISAM
        passive_deletes=True, 
    )
})

mapper(Member, member_table)

if __name__ == '__main__':
    engine = create_engine("postgresql://scott:tiger@localhost/test", echo=True)
    meta.create_all(engine)

    # expire_on_commit=False means the session contents
    # will not get invalidated after commit.
    sess = sessionmaker(engine, expire_on_commit=False)()

    # create org with some members
    org = Organization('org one')
    org.members.append(Member('member one'))
    org.members.append(Member('member two'))
    org.members.append(Member('member three'))

    sess.add(org)

    print "-------------------------\nflush one - save org + 3 members\n"
    sess.commit()

    # the 'members' collection is a Query.  it issues 
    # SQL as needed to load subsets of the collection.
    print "-------------------------\nload subset of members\n"
    members = org.members.filter(member_table.c.name.like('%member t%')).all()
    print members

    # new Members can be appended without any
    # SQL being emitted to load the full collection
    org.members.append(Member('member four'))
    org.members.append(Member('member five'))
    org.members.append(Member('member six'))

    print "-------------------------\nflush two - save 3 more members\n"
    sess.commit()

    # delete the object.   Using ON DELETE CASCADE 
    # SQL is only emitted for the head row - the Member rows 
    # disappear automatically without the need for additional SQL.
    sess.delete(org)
    print "-------------------------\nflush three - delete org, delete members in one statement\n"
    sess.commit()

    print "-------------------------\nno Member rows should remain:\n"
    print sess.query(Member).count()
    sess.close()

    print "------------------------\ndone.  dropping tables."
    meta.drop_all(engine)