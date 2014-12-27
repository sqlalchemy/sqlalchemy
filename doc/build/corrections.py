targets = {}
quit = False
def missing_reference(app, env, node, contnode):
    global quit
    if quit:
        return
    reftarget = node.attributes['reftarget']
    reftype = node.attributes['reftype']
    refdoc = node.attributes['refdoc']
    rawsource = node.rawsource
    if reftype == 'paramref':
        return

    target = rawsource
    if target in targets:
        return
    print "\n%s" % refdoc
    print "Reftarget: %s" % rawsource
    correction = raw_input("? ")
    correction = correction.strip()
    if correction == ".":
        correction = ":%s:`.%s`" % (reftype, reftarget)
    elif correction == 'q':
        quit = True
    else:
        targets[target] = correction

def write_corrections(app, exception):
    print "#!/bin/sh\n\n"
    for targ, corr in targets.items():
        if not corr:
            continue

        print """find lib/ -print -type f -name "*.py" -exec sed -i '' 's/%s/%s/g' {} \;""" % (targ, corr)
        print """find doc/build/ -print -type f -name "*.rst" -exec sed -i '' 's/%s/%s/g' {} \;""" % (targ, corr)

def setup(app):
    app.connect('missing-reference', missing_reference)
    app.connect('build-finished', write_corrections)
