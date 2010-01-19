"""
Two examples illustrating modifications to SQLAlchemy's attribute management system.

``listen_for_events.py`` illustrates the usage of :class:`~sqlalchemy.orm.interfaces.AttributeExtension` to intercept attribute events.  It additionally illustrates a way to automatically attach these listeners to all class attributes using a :class:`~sqlalchemy.orm.interfaces.InstrumentationManager`.

``custom_management.py`` illustrates much deeper usage of :class:`~sqlalchemy.orm.interfaces.InstrumentationManager` as well as collection adaptation, to completely change the underlying method used to store state on an object.   This example was developed to illustrate techniques which would be used by other third party object instrumentation systems to interact with SQLAlchemy's event system and is only intended for very intricate framework integrations.

"""