"""user.py - handles user login and validation"""

import random, string
from sha import sha

administrator = 'admin'
user = 'user'
groups = [user, administrator]

def cryptpw(password, salt=None):
    if salt is None:
        salt = string.join([chr(random.randint(ord('a'), ord('z'))), chr(random.randint(ord('a'), ord('z')))],'')
    return sha(password + salt).hexdigest()
    
def checkpw(password, dbpw):
    return cryptpw(password, dbpw[:2]) == dbpw

class User(object):
    def __init__(self, name=None, fullname=None, password=None, group=user):
        self.name = name
        self.fullname = fullname
        self.password = password
        self.group = group

    def is_administrator(self):
        return self.group == administrator

    def _set_password(self, password):
        if password:
            self.crypt_password=cryptpw(password)

    password = property(lambda s: None, _set_password)

    def checkpw(self, password):
        return checkpw(password, self.crypt_password)