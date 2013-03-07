import re
from sphinx.util.compat import Directive
from docutils.statemachine import StringList
from docutils import nodes, utils
import textwrap
import itertools
import collections
import md5

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


class EnvDirective(object):
    @property
    def env(self):
        return self.state.document.settings.env

class ChangeLogDirective(EnvDirective, Directive):
    has_content = True

    type_ = "change"

    default_section = 'misc'

    def _organize_by_section(self, changes):
        compound_sections = [(s, s.split(" ")) for s in
                                self.sections if " " in s]

        bysection = collections.defaultdict(list)
        all_sections = set()
        for rec in changes:
            inner_tag = rec['tags'].intersection(self.inner_tag_sort)
            if inner_tag:
                inner_tag = inner_tag.pop()
            else:
                inner_tag = ""

            for compound, comp_words in compound_sections:
                if rec['tags'].issuperset(comp_words):
                    bysection[(compound, inner_tag)].append(rec)
                    all_sections.add(compound)
                    break
            else:
                intersect = rec['tags'].intersection(self.sections)
                if intersect:
                    for sec in rec['sorted_tags']:
                        if sec in intersect:
                            bysection[(sec, inner_tag)].append(rec)
                            all_sections.add(sec)
                            break
                else:
                    bysection[(self.default_section, inner_tag)].append(rec)
        return bysection, all_sections

    @classmethod
    def changes(cls, env):
        return env.temp_data['ChangeLogDirective_%s_changes' % cls.type_]

    def _setup_run(self):
        self.sections = self.env.config.changelog_sections
        self.inner_tag_sort = self.env.config.changelog_inner_tag_sort + [""]
        self.env.temp_data['ChangeLogDirective_%s_changes' % self.type_] = []
        self._parsed_content = _parse_content(self.content)

        p = nodes.paragraph('', '',)
        self.state.nested_parse(self.content[1:], 0, p)

    def run(self):
        self._setup_run()
        changes = self.changes(self.env)
        output = []

        self.version = version = self._parsed_content.get('version', '')
        id_prefix = "%s-%s" % (self.type_, version)
        topsection = self._run_top(id_prefix)
        output.append(topsection)

        bysection, all_sections = self._organize_by_section(changes)

        counter = itertools.count()

        sections_to_render = [s for s in self.sections if s in all_sections]
        if not sections_to_render:
            for cat in self.inner_tag_sort:
                append_sec = self._append_node()

                for rec in bysection[(self.default_section, cat)]:
                    rec["id"] = "%s-%s" % (id_prefix, next(counter))

                    self._render_rec(rec, None, cat, append_sec)

                if append_sec.children:
                    topsection.append(append_sec)
        else:
            for section in sections_to_render + [self.default_section]:
                sec = nodes.section('',
                        nodes.title(section, section),
                        ids=["%s-%s" % (id_prefix, section.replace(" ", "-"))]
                )

                append_sec = self._append_node()
                sec.append(append_sec)

                for cat in self.inner_tag_sort:
                    for rec in bysection[(section, cat)]:
                        rec["id"] = "%s-%s" % (id_prefix, next(counter))
                        self._render_rec(rec, section, cat, append_sec)

                if append_sec.children:
                    topsection.append(sec)

        return output

    def _append_node(self):
        return nodes.bullet_list()

    def _run_top(self, id_prefix):
        version = self._parsed_content.get('version', '')
        topsection = nodes.section('',
                nodes.title(version, version),
                ids=[id_prefix]
            )

        if self._parsed_content.get("released"):
            topsection.append(nodes.Text("Released: %s" %
                        self._parsed_content['released']))
        else:
            topsection.append(nodes.Text("no release date"))

        intro_para = nodes.paragraph('', '')
        for len_, text in enumerate(self._parsed_content['text']):
            if ".. change::" in text:
                break
        if len_:
            self.state.nested_parse(self._parsed_content['text'][0:len_], 0,
                            intro_para)
            topsection.append(intro_para)

        return topsection


    def _render_rec(self, rec, section, cat, append_sec):
        para = rec['node'].deepcopy()

        text = _text_rawsource_from_node(para)

        to_hash = "%s %s" % (self.version, text[0:100])
        targetid = "%s-%s" % (self.type_,
                        md5.md5(to_hash.encode('ascii', 'ignore')
                            ).hexdigest())
        targetnode = nodes.target('', '', ids=[targetid])
        para.insert(0, targetnode)
        permalink = nodes.reference('', '',
                        nodes.Text("(link)", "(link)"),
                        refid=targetid,
                        classes=['changeset-link']
                    )
        para.append(permalink)

        insert_ticket = nodes.paragraph('')
        para.append(insert_ticket)

        i = 0
        for collection, render, prefix in (
                (rec['tickets'], self.env.config.changelog_render_ticket, "#%s"),
                (rec['pullreq'], self.env.config.changelog_render_pullreq,
                                            "pull request %s"),
                (rec['changeset'], self.env.config.changelog_render_changeset, "r%s"),
            ):
            for refname in collection:
                if i > 0:
                    insert_ticket.append(nodes.Text(", ", ", "))
                else:
                    insert_ticket.append(nodes.Text(" ", " "))
                i += 1
                if render is not None:
                    refuri = render % refname
                    node = nodes.reference('', '',
                            nodes.Text(prefix % refname, prefix % refname),
                            refuri=refuri
                        )
                else:
                    node = nodes.Text(prefix % refname, prefix % refname)
                insert_ticket.append(node)

        if rec['tags']:
            tag_node = nodes.strong('',
                        " ".join("[%s]" % t for t
                            in
                                [t1 for t1 in [section, cat]
                                    if t1 in rec['tags']] +

                                list(rec['tags'].difference([section, cat]))
                        ) + " "
                    )
            para.children[0].insert(0, tag_node)

        append_sec.append(
            nodes.list_item('',
                nodes.target('', '', ids=[rec['id']]),
                para
            )
        )


class ChangeDirective(EnvDirective, Directive):
    has_content = True

    type_ = "change"
    parent_cls = ChangeLogDirective

    def run(self):
        content = _parse_content(self.content)
        p = nodes.paragraph('', '',)
        sorted_tags = _comma_list(content.get('tags', ''))
        rec = {
            'tags': set(sorted_tags).difference(['']),
            'tickets': set(_comma_list(content.get('tickets', ''))).difference(['']),
            'pullreq': set(_comma_list(content.get('pullreq', ''))).difference(['']),
            'changeset': set(_comma_list(content.get('changeset', ''))).difference(['']),
            'node': p,
            'type': self.type_,
            "title": content.get("title", None),
            'sorted_tags': sorted_tags
        }

        if "declarative" in rec['tags']:
            rec['tags'].add("orm")

        self.state.nested_parse(content['text'], 0, p)
        self.parent_cls.changes(self.env).append(rec)

        return []

def _text_rawsource_from_node(node):
    src = []
    stack = [node]
    while stack:
        n = stack.pop(0)
        if isinstance(n, nodes.Text):
            src.append(n.rawsource)
        stack.extend(n.children)
    return "".join(src)

def _rst2sphinx(text):
    return StringList(
        [line.strip() for line in textwrap.dedent(text).split("\n")]
    )


def make_ticket_link(name, rawtext, text, lineno, inliner,
                      options={}, content=[]):
    env = inliner.document.settings.env
    render_ticket = env.config.changelog_render_ticket or "%s"
    prefix = "#%s"
    if render_ticket:
        ref = render_ticket % text
        node = nodes.reference(rawtext, prefix % text, refuri=ref, **options)
    else:
        node = nodes.Text(prefix % text, prefix % text)
    return [node], []

def setup(app):
    app.add_directive('changelog', ChangeLogDirective)
    app.add_directive('change', ChangeDirective)
    app.add_config_value("changelog_sections", [], 'env')
    app.add_config_value("changelog_inner_tag_sort", [], 'env')
    app.add_config_value("changelog_render_ticket",
            None,
            'env'
        )
    app.add_config_value("changelog_render_pullreq",
            None,
            'env'
        )
    app.add_config_value("changelog_render_changeset",
            None,
            'env'
        )
    app.add_role('ticket', make_ticket_link)
