"""A variant of the versioned_rows example. Here
we store a dictionary of key/value pairs, storing the k/v's in a
"vertical" fashion where each key gets a row. The value is split out
into two separate datatypes, string and int - the range of datatype
storage can be adjusted for individual needs.

Changes to the "data" attribute of a ConfigData object result in the
ConfigData object being copied into a new one, and new associations to
its data are created. Values which aren't changed between versions are
referenced by both the former and the newer ConfigData object.
Overall, only INSERT statements are emitted - no rows are UPDATed or
DELETEd.

An optional feature is also illustrated which associates individual
key/value pairs with the ConfigData object in which it first
originated. Since a new row is only persisted when a new value is
created for a particular key, the recipe provides a way to query among
the full series of changes which occurred for any particular key in
the dictionary.

The set of all ConfigData in a particular table represents a single
series of versions. By adding additional columns to ConfigData, the
system can be made to store multiple version streams distinguished by
those additional values.

"""

from sqlalchemy import Column, String, Integer, ForeignKey, \
    create_engine
from sqlalchemy.orm.interfaces import SessionExtension
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import attributes, relationship, backref, \
    sessionmaker, make_transient, validates
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm.collections import attribute_mapped_collection

class VersionExtension(SessionExtension):
    """Apply the new_version() method of objects which are
    marked as dirty during a flush.

    See http://www.sqlalchemy.org/trac/wiki/UsageRecipes/VersionedRows

    """
    def before_flush(self, session, flush_context, instances):
        for instance in session.dirty:
            if hasattr(instance, 'new_version') and \
                session.is_modified(instance, passive=True):

                # make it transient
                instance.new_version(session)

                # re-add
                session.add(instance)

Base = declarative_base()

class ConfigData(Base):
    """Represent a series of key/value pairs.

    ConfigData will generate a new version of itself
    upon change.

    The "data" dictionary provides access via
    string name mapped to a string/int value.

    """
    __tablename__ = 'config'

    id = Column(Integer, primary_key=True)
    """Primary key column of this ConfigData."""

    elements = relationship("ConfigValueAssociation",
                    collection_class=attribute_mapped_collection("name"),
                    backref=backref("config_data"),
                    lazy="subquery"
                )
    """Dictionary-backed collection of ConfigValueAssociation objects,
    keyed to the name of the associated ConfigValue.

    Note there's no "cascade" here.  ConfigValueAssociation objects
    are never deleted or changed.
    """

    def _new_value(name, value):
        """Create a new entry for usage in the 'elements' dictionary."""
        return ConfigValueAssociation(ConfigValue(name, value))

    data = association_proxy("elements", "value", creator=_new_value)
    """Proxy to the 'value' elements of each related ConfigValue,
    via the 'elements' dictionary.
    """

    def __init__(self, data):
        self.data = data

    @validates('elements')
    def _associate_with_element(self, key, element):
        """Associate incoming ConfigValues with this
        ConfigData, if not already associated.

        This is an optional feature which allows
        more comprehensive history tracking.

        """
        if element.config_value.originating_config is None:
            element.config_value.originating_config = self
        return element

    def new_version(self, session):
        # convert to an INSERT
        make_transient(self)
        self.id = None

        # history of the 'elements' collection.
        # this is a tuple of groups: (added, unchanged, deleted)
        hist = attributes.get_history(self, 'elements')

        # rewrite the 'elements' collection
        # from scratch, removing all history
        attributes.set_committed_value(self, 'elements', {})

        # new elements in the "added" group
        # are moved to our new collection.
        for elem in hist.added:
            self.elements[elem.name] = elem

        # copy elements in the 'unchanged' group.
        # the new ones associate with the new ConfigData,
        # the old ones stay associated with the old ConfigData
        for elem in hist.unchanged:
            self.elements[elem.name] = ConfigValueAssociation(elem.config_value)

        # we also need to expire changes on each ConfigValueAssociation
        # that is to remain associated with the old ConfigData.
        # Here, each one takes care of that in its new_version()
        # method, though we could do that here as well.


class ConfigValueAssociation(Base):
    """Relate ConfigData objects to associated ConfigValue objects."""

    __tablename__ = 'config_value_association'

    config_id = Column(ForeignKey('config.id'), primary_key=True)
    """Reference the primary key of the ConfigData object."""


    config_value_id = Column(ForeignKey('config_value.id'), primary_key=True)
    """Reference the primary key of the ConfigValue object."""

    config_value = relationship("ConfigValue", lazy="joined", innerjoin=True)
    """Reference the related ConfigValue object."""

    def __init__(self, config_value):
        self.config_value = config_value

    def new_version(self, session):
        """Expire all pending state, as ConfigValueAssociation is immutable."""

        session.expire(self)

    @property
    def name(self):
        return self.config_value.name

    @property
    def value(self):
        return self.config_value.value

    @value.setter
    def value(self, value):
        """Intercept set events.

        Create a new ConfigValueAssociation upon change,
        replacing this one in the parent ConfigData's dictionary.

        If no net change, do nothing.

        """
        if value != self.config_value.value:
            self.config_data.elements[self.name] = \
                    ConfigValueAssociation(
                        ConfigValue(self.config_value.name, value)
                    )

class ConfigValue(Base):
    """Represent an individual key/value pair at a given point in time.

    ConfigValue is immutable.

    """
    __tablename__ = 'config_value'

    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    originating_config_id = Column(Integer, ForeignKey('config.id'),
                            nullable=False)
    int_value = Column(Integer)
    string_value = Column(String(255))

    def __init__(self, name, value):
        self.name = name
        self.value = value

    originating_config = relationship("ConfigData")
    """Reference to the originating ConfigData.

    This is optional, and allows history tracking of
    individual values.

    """

    def new_version(self, session):
        raise NotImplementedError("ConfigValue is immutable.")

    @property
    def value(self):
        for k in ('int_value', 'string_value'):
            v = getattr(self, k)
            if v is not None:
                return v
        else:
            return None

    @value.setter
    def value(self, value):
        if isinstance(value, int):
            self.int_value = value
            self.string_value = None
        else:
            self.string_value = str(value)
            self.int_value = None

if __name__ == '__main__':
    engine = create_engine('sqlite://', echo=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, extension=VersionExtension())

    sess = Session()

    config = ConfigData({
        'user_name':'twitter',
        'hash_id':'4fedffca37eaf',
        'x':27,
        'y':450
        })

    sess.add(config)
    sess.commit()
    version_one = config.id

    config.data['user_name'] = 'yahoo'
    sess.commit()

    version_two = config.id

    assert version_one != version_two

    # two versions have been created.

    assert config.data == {
        'user_name':'yahoo',
        'hash_id':'4fedffca37eaf',
        'x':27,
        'y':450
    }

    old_config = sess.query(ConfigData).get(version_one)
    assert old_config.data == {
        'user_name':'twitter',
        'hash_id':'4fedffca37eaf',
        'x':27,
        'y':450
    }

    # the history of any key can be acquired using
    # the originating_config_id attribute
    history = sess.query(ConfigValue).\
            filter(ConfigValue.name=='user_name').\
            order_by(ConfigValue.originating_config_id).\
            all()

    assert [(h.value, h.originating_config_id) for h in history] == \
            [('twitter', version_one), ('yahoo', version_two)]
