"""application table metadata objects are described here."""

from sqlalchemy import *
from sqlalchemy.test.schema import Table, Column

metadata = MetaData()

users = Table('users', metadata,
    Column('user_id', Integer, primary_key=True, test_needs_autoincrement=True),
    Column('user_name', String(30), nullable=False),
    Column('fullname', String(100), nullable=False),
    Column('password', String(40), nullable=False),
    Column('groupname', String(20), nullable=False),
    )

blogs = Table('blogs', metadata,
    Column('blog_id', Integer, primary_key=True, test_needs_autoincrement=True),
    Column('owner_id', Integer, ForeignKey('users.user_id'), nullable=False),
    Column('name', String(100), nullable=False),
    Column('description', String(500))
    )

posts = Table('posts', metadata,
    Column('post_id', Integer, primary_key=True, test_needs_autoincrement=True),
    Column('blog_id', Integer, ForeignKey('blogs.blog_id'), nullable=False),
    Column('user_id', Integer, ForeignKey('users.user_id'), nullable=False),
    Column('datetime', DateTime, nullable=False),
    Column('headline', String(500)),
    Column('summary', String(255)),
    Column('body', Text),
    )

topics = Table('topics', metadata,
    Column('topic_id', Integer, primary_key=True, test_needs_autoincrement=True),
    Column('keyword', String(50), nullable=False),
    Column('description', String(500))
   )

topic_xref = Table('topic_post_xref', metadata,
    Column('topic_id', Integer, ForeignKey('topics.topic_id'), nullable=False),
    Column('is_primary', Boolean, nullable=False),
    Column('post_id', Integer, ForeignKey('posts.post_id'), nullable=False)
   )

comments = Table('comments', metadata,
    Column('comment_id', Integer, primary_key=True, test_needs_autoincrement=True),
    Column('user_id', Integer, ForeignKey('users.user_id'), nullable=False),
    Column('post_id', Integer, ForeignKey('posts.post_id'), nullable=False),
    Column('datetime', DateTime, nullable=False),
    Column('parent_comment_id', Integer, ForeignKey('comments.comment_id')),
    Column('subject', String(500)),
    Column('body', Text),
    )
