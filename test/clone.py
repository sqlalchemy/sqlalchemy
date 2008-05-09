# only tested with cpython!
import optparse, os, shutil, sys
from os import path
from testlib import filters

__doc__ = """
Creates and maintains a 'clone' of the test suite, optionally transforming
the source code through a filter.  The primary purpose of this utility is
to allow the tests to run on Python VMs that do not implement a parser that
groks 2.4 style @decorations.

Creating a clone:

  Create a new, exact clone of the suite:
  $ python test/clone.py -c myclone

  Create a new clone using the 2.3 filter:
  $ python test/clone.py -c --filter=py23 myclone

After the clone is set up, changes in the master can be pulled into the clone
with the -u or --update switch.  If the clone was created with a filter, it
will be applied automatically when updating.

  Update the clone:
  $ python test/clone.py -u myclone

The updating algorithm is very simple: if the version in test/ is newer than
the one in your clone, the clone version is overwritten.
"""

options = None
clone, clone_path = None, None
filter = lambda x: x[:]

def optparser():
    parser = optparse.OptionParser(
        usage=('usage: %prog [options] CLONE-NAME\n' + __doc__ ).rstrip())
    parser.add_option('-n', '--dry-run', dest='dryrun',
                      action='store_true',
                      help=('Do not actually change any files; '
                            'just print what would happen.'))
    parser.add_option('-u', '--update', dest='update', action='store_true',
                      help='Update an existing clone.')
    parser.add_option('-c', '--create', dest='create', action='store_true',
                      help='Create a new clone.')
    parser.add_option('--filter', dest='filter',
                      help='Run source code through a filter.')
    parser.add_option('-l', '--filter-list', dest='filter_list',
                      action='store_true',
                      help='Show available filters.')
    parser.add_option('-f', '--force', dest='force', action='store_true',
                      help='Overwrite clone files even if unchanged.')
    parser.add_option('-q', '--quiet', dest='quiet', action='store_true',
                      help='Run quietly.')
    parser.set_defaults(update=False, create=False,
                        dryrun=False, filter_list=False,
                        force=False, quiet=False)
    return parser

def config():
    global clone, clone_path, options, filter

    parser = optparser()
    (options, args) = parser.parse_args()

    if options.filter_list:
        if options.quiet:
            print '\n'.join(filters.__all__)
        else:
            print 'Available filters:'
            for name in filters.__all__:
                print '\t%s' % name
        sys.exit(0)

    if not options.update and not options.create:
        parser.error('One of -u or -c is required.')

    if len(args) != 1:
        parser.error('A clone name is required.')

    clone = args[0]
    clone_path = path.abspath(clone)

    if options.update and not path.exists(clone_path):
        parser.error(
            'Clone %s does not exist; create it with --create first.' % clone)
    if options.create and path.exists(clone_path):
        parser.error('Clone %s already exists.' % clone)

    if options.filter:
        if options.filter not in filters.__all__:
            parser.error(('Filter "%s" unknown; use --filter-list to see '
                          'available filters.') % options.filter)
        filter = getattr(filters, options.filter)

def setup():
    global filter

    if options.create:
        if not options.quiet:
            print "mkdir %s" % clone_path
        if not options.dryrun:
            os.mkdir(clone_path)

        if options.filter and not options.dryrun:
            if not options.quiet:
                print 'storing filter "%s" in %s/.filter' % (
                    options.filter, clone)
            stash = open(path.join(clone_path, '.filter'), 'w')
            stash.write(options.filter)
            stash.close()
    else:
        stash_file = path.join(clone_path, '.filter')
        if path.exists(stash_file):
            stash = open(stash_file)
            stashed = stash.read().strip()
            stash.close()
            if options.filter:
                if (options.filter != stashed and stashed in filters.__all__ and
                    not options.quiet):
                    print (('Warning: --filter=%s overrides %s specified in '
                            '%s/.filter') % (options.filter, stashed, clone))
            else:
                if stashed not in filters.__all__:
                    sys.stderr.write(
                        'Filter "%s" in %s/.filter is not valid, aborting.' %
                        (stashed, clone))
                    sys.exit(-1)
            filter = getattr(filters, stashed)

def sync():
    source_path, _ = path.split(path.abspath(__file__))

    ls = lambda root: [fn
                       for fn in os.listdir(root)
                       if (fn.endswith('.py') and not fn.startswith('.'))]

    def walker(x, dirname, fnames):
        if '.svn' in fnames:
            fnames.remove('.svn')

        rel_path = dirname[len(source_path) + 1:]
        dest_path = path.join(clone_path, rel_path)

        if not path.exists(dest_path):
            if not options.quiet:
                print "mkdir %s/%s" % (clone, rel_path)
            if not options.dryrun:
                os.mkdir(dest_path)

        for filename in ls(dirname):
            source_file = path.join(source_path, rel_path, filename)
            dest_file = path.join(dest_path, filename)

            if (options.force or
                (not path.exists(dest_file) or
                 os.stat(source_file)[-1] > os.stat(dest_file)[-1])):
                if not options.quiet:
                    print "syncing %s" % path.join(rel_path, filename)

                raw = open(source_file)
                filtered = filter(raw.readlines())
                raw.close()

                if not options.dryrun:
                    synced = open(dest_file, 'w')
                    synced.writelines(filtered)
                    synced.close()

    os.path.walk(source_path, walker, None)

if __name__ == '__main__':
    config()
    setup()
    sync()
