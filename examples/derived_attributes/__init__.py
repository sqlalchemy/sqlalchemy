"""Illustrates a clever technique using Python descriptors to create custom attributes representing SQL expressions when used at the class level, and Python expressions when used at the instance level.   In some cases this technique replaces the need to configure the attribute in the mapping, instead relying upon ordinary Python behavior to create custom expression components.

E.g.::

    class BaseInterval(object):
        @hybrid
        def contains(self,point):
            return (self.start <= point) & (point < self.end)

"""
