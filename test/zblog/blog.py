import datetime


__all__ = ['Blog', 'Post', 'Topic', 'TopicAssociation', 'Comment']

class Blog(object):
    def __init__(self, owner=None):
        self.owner = owner

class Post(object):
    topics = set
    def __init__(self, user=None, headline=None, summary=None):
        self.user = user
        self.datetime = datetime.datetime.today()
        self.headline = headline
        self.summary = summary
        self.comments = []
        self.comment_count = 0

class Topic(object):
    def __init__(self, keyword=None, description=None):
        self.keyword = keyword
        self.description = description

class TopicAssociation(object):
    def __init__(self, post=None, topic=None, is_primary=False):
        self.post = post
        self.topic = topic
        self.is_primary = is_primary

class Comment(object):
    def __init__(self, subject=None, body=None):
        self.subject = subject
        self.datetime = datetime.datetime.today()
        self.body = body
