

from . import autodoc_mods, dialect_info, sqlformatter, mako, changelog

def setup(app):
    app.add_config_value('release_date', "", True)
    app.add_config_value('site_base', "", True)
    app.add_config_value('build_number', "", 1)
    mako.setup(app)
    autodoc_mods.setup(app)
    dialect_info.setup(app)
    sqlformatter.setup(app)
    changelog.setup(app)
