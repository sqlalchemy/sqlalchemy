def install_mods(*mods):
    for mod in mods:
        _mod = getattr(__import__('sqlalchemy.mods.%s' % mod).mods, mod)
        _mod.install_plugin()