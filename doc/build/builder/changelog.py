import re
from sphinx.util.compat import Directive
from docutils.statemachine import StringList
from docutils import nodes
import textwrap
import itertools
import collections

def _comma_list(text):
    return re.split(r"\s*,\s*", text.strip())

def _parse_content(content):
    d = {}
    d['text'] = []
    idx = 0
    for line in content:
        idx += 1
        m = re.match(r' *\:(.+?)\:(?: +(.+))?', line)
        if m:
            attrname, value = m.group(1, 2)
            d[attrname] = value or ''
        else:
            break
    d["text"] = content[idx:]
    return d

def _ticketurl(ticket):
    return "http://www.sqlalchemy.org/trac/ticket/%s" % ticket

class EnvDirective(object):
    @property
    def env(self):
        return self.state.document.settings.env

class ChangeLogDirective(EnvDirective, Directive):
    has_content = True

    type_ = "change"

    sections = _comma_list("general, orm, orm declarative, orm querying, \
                orm configuration, engine, sql, \
                schema, \
                postgresql, mysql, sqlite, mssql, oracle, firebird, misc")

    subsections = ["feature", "bug", "moved", "changed", "removed", ""]


    def _organize_by_section(self, changes):
        compound_sections = [(s, s.split(" ")) for s in
                                self.sections if " " in s]

        bysection = collections.defaultdict(list)
        for rec in changes:
            subsection = rec['tags'].intersection(self.subsections)
            if subsection:
                subsection = subsection.pop()
            else:
                subsection = ""

            for compound, comp_words in compound_sections:
                if rec['tags'].issuperset(comp_words):
                    bysection[(compound, subsection)].append(rec)
                    break

            intersect = rec['tags'].intersection(self.sections)
            if intersect:
                bysection[(intersect.pop(), subsection)].append(rec)
                continue

            bysection[('misc', subsection)].append(rec)
        return bysection

    @classmethod
    def changes(cls, env):
        return env.temp_data['ChangeLogDirective_%s_changes' % cls.type_]

    def _setup_run(self):
        self.env.temp_data['ChangeLogDirective_%s_changes' % self.type_] = []
        self._parsed_content = _parse_content(self.content)

        p = nodes.paragraph('', '',)
        self.state.nested_parse(self.content[1:], 0, p)

    def run(self):
        self._setup_run()
        changes = self.changes(self.env)
        output = []

        version = self._parsed_content.get('version', '')
        id_prefix = "%s-%s" % (self.type_, version)
        topsection = self._run_top(id_prefix)
        output.append(topsection)

        bysection = self._organize_by_section(changes)

        counter = itertools.count()

        for section in self.sections:
            sec, append_sec = self._section(section, id_prefix)

            for cat in self.subsections:
                for rec in bysection[(section, cat)]:
                    rec["id"] = "%s-%s" % (id_prefix, next(counter))

                    self._render_rec(rec, section, cat, append_sec)

            if append_sec.children:
                topsection.append(sec)

        return output

    def _section(self, section, id_prefix):
        bullets = nodes.bullet_list()
        sec = nodes.section('',
                nodes.title(section, section),
                bullets,
                ids=["%s-%s" % (id_prefix, section.replace(" ", "-"))]
        )
        return sec, bullets

    def _run_top(self, id_prefix):
        version = self._parsed_content.get('version', '')
        topsection = nodes.section('',
                nodes.title(version, version),
                ids=[id_prefix]
            )

        if self._parsed_content.get("released"):
            topsection.append(nodes.Text("Released: %s" % self._parsed_content['released']))
        else:
            topsection.append(nodes.Text("no release date"))
        return topsection

    def _render_rec(self, rec, section, cat, append_sec):
        para = rec['node'].deepcopy()
        insert_ticket = nodes.paragraph('')
        para.append(insert_ticket)

        for i, ticket in enumerate(rec['tickets']):
            if i > 0:
                insert_ticket.append(nodes.Text(", ", ", "))
            else:
                insert_ticket.append(nodes.Text(" ", " "))
            insert_ticket.append(
                nodes.reference('', '',
                    nodes.Text("#%s" % ticket, "#%s" % ticket),
                    refuri=_ticketurl(ticket)
                )
            )

        if cat or rec['tags']:
            #tag_node = nodes.strong('',
            #            "[" + cat + "] "
            #        )
            tag_node = nodes.strong('',
                        " ".join("[%s]" % t for t
                            in
                                [cat] +
                                list(rec['tags'].difference([cat]))
                            if t
                        ) + " "
                    )
            para.children[0].insert(0, tag_node)

        append_sec.append(
            nodes.list_item('',
                nodes.target('', '', ids=[rec['id']]),
                para
            )
        )


class MigrationLogDirective(ChangeLogDirective):
    type_ = "migration"

    sections = _comma_list("New Features, Behavioral Changes, Removed")

    subsections = _comma_list("general, orm, orm declarative, orm querying, \
                orm configuration, engine, sql, \
                postgresql, mysql, sqlite")

    def _run_top(self, id_prefix):
        version = self._parsed_content.get('version', '')
        title = "What's new in %s?" % version
        topsection = nodes.section('',
                nodes.title(title, title),
                ids=[id_prefix]
            )
        if "released" in self._parsed_content:
            topsection.append(nodes.Text("Released: %s" % self._parsed_content['released']))
        return topsection

    def _section(self, section, id_prefix):
        sec = nodes.section('',
                nodes.title(section, section),
                ids=["%s-%s" % (id_prefix, section.replace(" ", "-"))]
        )
        return sec, sec

    def _render_rec(self, rec, section, cat, append_sec):
        para = rec['node'].deepcopy()

        insert_ticket = nodes.paragraph('')
        para.append(insert_ticket)

        for i, ticket in enumerate(rec['tickets']):
            if i > 0:
                insert_ticket.append(nodes.Text(", ", ", "))
            else:
                insert_ticket.append(nodes.Text(" ", " "))
            insert_ticket.append(
                nodes.reference('', '',
                    nodes.Text("#%s" % ticket, "#%s" % ticket),
                    refuri=_ticketurl(ticket)
                )
            )

        append_sec.append(
            nodes.section('',
                nodes.title(rec['title'], rec['title']),
                para,
                ids=[rec['id']]
            )
        )



class ChangeDirective(EnvDirective, Directive):
    has_content = True

    type_ = "change"
    parent_cls = ChangeLogDirective

    def run(self):
        content = _parse_content(self.content)
        p = nodes.paragraph('', '',)
        rec = {
            'tags': set(_comma_list(content.get('tags', ''))).difference(['']),
            'tickets': set(_comma_list(content.get('tickets', ''))).difference(['']),
            'node': p,
            'type': self.type_,
            "title": content.get("title", None)
        }

        if "declarative" in rec['tags']:
            rec['tags'].add("orm")

        self.state.nested_parse(content['text'], 0, p)
        self.parent_cls.changes(self.env).append(rec)

        return []

class MigrationDirective(ChangeDirective):
    type_ = "migration"
    parent_cls = MigrationLogDirective


def _rst2sphinx(text):
    return StringList(
        [line.strip() for line in textwrap.dedent(text).split("\n")]
    )

def setup(app):
    app.add_directive('changelog', ChangeLogDirective)
    app.add_directive('migrationlog', MigrationLogDirective)
    app.add_directive('migration', MigrationDirective)
    app.add_directive('change', ChangeDirective)

