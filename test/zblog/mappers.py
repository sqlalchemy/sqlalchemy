"""mapper.py - defines mappers for domain objects, mapping operations"""

from test.zblog import tables, user
from test.zblog.blog import *
from sqlalchemy import *
from sqlalchemy.orm import *
import sqlalchemy.util as util

def zblog_mappers():
    # User mapper.  Here, we redefine the names of some of the columns to
    # different property names.  normally the table columns are all sucked in
    # automatically.
    mapper(user.User, tables.users, properties={
        'id':tables.users.c.user_id,
        'name':tables.users.c.user_name,
        'group':tables.users.c.groupname,
        'crypt_password':tables.users.c.password,
    })

    # blog mapper.  this contains a reference to the user mapper, and also
    # installs a "backreference" on that relationship to handle it in both
    # ways. this will also attach a 'blogs' property to the user mapper.
    mapper(Blog, tables.blogs, properties={
        'id':tables.blogs.c.blog_id,
        'owner':relationship(user.User, lazy='joined',
                         backref=backref('blogs', cascade="all, delete-orphan")),
    })

    # topic mapper.  map all topic columns to the Topic class.
    mapper(Topic, tables.topics)

    # TopicAssocation mapper.  This is an "association" object, which is
    # similar to a many-to-many relationship except extra data is associated
    # with each pair of related data.  because the topic_xref table doesnt
    # have a primary key, the "primary key" columns of a TopicAssociation are
    # defined manually here.
    mapper(TopicAssociation,tables.topic_xref,
                primary_key=[tables.topic_xref.c.post_id,
                             tables.topic_xref.c.topic_id],
                properties={
                    'topic':relationship(Topic, lazy='joined'),
                })

    # Post mapper, these are posts within a blog.
    # since we want the count of comments for each post, create a select that
    # will get the posts and count the comments in one query.
    posts_with_ccount = select(
        [c for c in tables.posts.c if c.key != 'body'] + [
            func.count(tables.comments.c.comment_id).label('comment_count')
        ],
        from_obj = [
            outerjoin(tables.posts, tables.comments)
        ],
        group_by=[
            c for c in tables.posts.c if c.key != 'body'
        ]
        ) .alias('postswcount')

    # then create a Post mapper on that query.
    # we have the body as "deferred" so that it loads only when needed, the
    # user as a Lazy load, since the lazy load will run only once per user and
    # its usually only one user's posts is needed per page, the owning blog is
    # a lazy load since its also probably loaded into the identity map
    # already, and topics is an eager load since that query has to be done per
    # post in any case.
    mapper(Post, posts_with_ccount, properties={
        'id':posts_with_ccount.c.post_id,
        'body':deferred(tables.posts.c.body),
        'user':relationship(user.User, lazy='select',
                        backref=backref('posts', cascade="all, delete-orphan")),
        'blog':relationship(Blog, lazy='select',
                        backref=backref('posts', cascade="all, delete-orphan")),
        'topics':relationship(TopicAssociation, lazy='joined',
                          cascade="all, delete-orphan",
                          backref='post')
    }, order_by=[desc(posts_with_ccount.c.datetime)])


    # comment mapper.  This mapper is handling a hierarchical relationship on
    # itself, and contains a lazy reference both to its parent comment and its
    # list of child comments.
    mapper(Comment, tables.comments, properties={
        'id':tables.comments.c.comment_id,
        'post':relationship(Post, lazy='select',
                        backref=backref('comments',
                                        cascade="all, delete-orphan")),
        'user':relationship(user.User, lazy='joined',
                        backref=backref('comments',
                                        cascade="all, delete-orphan")),
        'parent':relationship(Comment,
                          primaryjoin=(tables.comments.c.parent_comment_id ==
                                       tables.comments.c.comment_id),
                          foreign_keys=[tables.comments.c.comment_id],
                          lazy='select', uselist=False),
        'replies':relationship(Comment,
                           primaryjoin=(tables.comments.c.parent_comment_id ==
                                        tables.comments.c.comment_id),
                           lazy='select', uselist=True, cascade="all"),
    })

# we define one special find-by for the comments of a post, which is going to
# make its own "noload" mapper and organize the comments into their correct
# hierarchy in one pass. hierarchical data normally needs to be loaded by
# separate queries for each set of children, unless you use a proprietary
# extension like CONNECT BY.
def find_by_post(post):
    """returns a hierarchical collection of comments based on a given criterion.

    Uses a mapper that does not lazy load replies or parents, and instead
    organizes comments into a hierarchical tree when the result is produced.
    """

    q = session().query(Comment).options(noload('replies'), noload('parent'))
    comments = q.select_by(post_id=post.id)
    result = []
    d = {}
    for c in comments:
        d[c.id] = c
        if c.parent_comment_id is None:
            result.append(c)
            c.parent=None
        else:
            parent = d[c.parent_comment_id]
            parent.replies.append(c)
            c.parent = parent
    return result

Comment.find_by_post = staticmethod(find_by_post)

def start_session():
    """creates a new session for the start of a request."""
    trans.session = create_session(bind_to=zblog.database.engine )

def session():
    return trans.session
