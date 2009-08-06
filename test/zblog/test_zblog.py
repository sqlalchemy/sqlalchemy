from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.test import *
from test.zblog import mappers, tables
from test.zblog.user import *
from test.zblog.blog import *


class ZBlogTest(TestBase, AssertsExecutionResults):

    @classmethod
    def create_tables(cls):
        tables.metadata.drop_all(bind=testing.db)
        tables.metadata.create_all(bind=testing.db)
    
    @classmethod
    def drop_tables(cls):
        tables.metadata.drop_all(bind=testing.db)

    @classmethod
    def setup_class(cls):
        cls.create_tables()
    @classmethod
    def teardown_class(cls):
        cls.drop_tables()
    def teardown(self):
        pass
    def setup(self):
        pass


class SavePostTest(ZBlogTest):
    @classmethod
    def setup_class(cls):
        super(SavePostTest, cls).setup_class()
        
        mappers.zblog_mappers()
        global blog_id, user_id
        s = create_session(bind=testing.db)
        user = User('zbloguser', "Zblog User", "hello", group=administrator)
        blog = Blog(owner=user)
        blog.name = "this is a blog"
        s.add(user)
        s.add(blog)
        s.flush()
        blog_id = blog.id
        user_id = user.id
        s.close()

    @classmethod
    def teardown_class(cls):
        clear_mappers()
        super(SavePostTest, cls).teardown_class()

    def testattach(self):
        """test that a transient/pending instance has proper bi-directional behavior.

        this requires that lazy loaders do not fire off for a transient/pending instance."""
        s = create_session(bind=testing.db)

        s.begin()
        try:
            blog = s.query(Blog).get(blog_id)
            post = Post(headline="asdf asdf", summary="asdfasfd")
            s.add(post)
            post.blog_id=blog_id
            post.blog = blog
            assert post in blog.posts
        finally:
            s.rollback()

    def testoptimisticorphans(self):
        """test that instances in the session with un-loaded parents will not
        get marked as "orphans" and then deleted """
        s = create_session(bind=testing.db)

        s.begin()
        try:
            blog = s.query(Blog).get(blog_id)
            post = Post(headline="asdf asdf", summary="asdfasfd")
            post.blog = blog
            user = s.query(User).get(user_id)
            post.user = user
            s.add(post)
            s.flush()
            s.expunge_all()

            user = s.query(User).get(user_id)
            blog = s.query(Blog).get(blog_id)
            post = blog.posts[0]
            comment = Comment(subject="some subject", body="some body")
            comment.post = post
            comment.user = user
            s.flush()
            s.expunge_all()

            assert s.query(Post).get(post.id) is not None

        finally:
            s.rollback()


