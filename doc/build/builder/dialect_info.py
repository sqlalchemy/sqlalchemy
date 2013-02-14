import re
from sphinx.util.compat import Directive
from docutils import nodes

class DialectDirective(Directive):
    has_content = True

    _dialects = {}

    def _parse_content(self):
        d = {}
        d['default'] = self.content[0]
        d['text'] = []
        idx = 0
        for line in self.content[1:]:
            idx += 1
            m = re.match(r'\:(.+?)\: +(.+)', line)
            if m:
                attrname, value = m.group(1, 2)
                d[attrname] = value
            else:
                break
        d["text"] = self.content[idx + 1:]
        return d

    def _dbapi_node(self):

        dialect_name, dbapi_name = self.dialect_name.split("+")

        try:
            dialect_directive = self._dialects[dialect_name]
        except KeyError:
            raise Exception("No .. dialect:: %s directive has been established"
                                    % dialect_name)

        output = []

        content = self._parse_content()

        parent_section_ref = self.state.parent.children[0]['ids'][0]
        self._append_dbapi_bullet(dialect_name, dbapi_name,
                                        content['name'], parent_section_ref)

        p = nodes.paragraph('', '',
                    nodes.Text(
                        "Support for the %s database via the %s driver." % (
                                dialect_directive.database_name,
                                content['name']
                        ),
                        "Support for the %s database via the %s driver." % (
                                dialect_directive.database_name,
                                content['name']
                        )
                    ),
        )

        self.state.nested_parse(content['text'], 0, p)
        output.append(p)

        if "url" in content or "driverurl" in content:
            sec = nodes.section(
                    '',
                    nodes.title("DBAPI", "DBAPI"),
                    ids=["dialect-%s-%s-url" % (dialect_name, dbapi_name)]
            )
            if "url" in content:
                text = "Documentation and download information (if applicable) "\
                        "for %s is available at:\n" % content["name"]
                uri = content['url']
                sec.append(
                    nodes.paragraph('', '',
                        nodes.Text(text, text),
                        nodes.reference('', '',
                            nodes.Text(uri, uri),
                            refuri=uri,
                        )
                    )
                )
            if "driverurl" in content:
                text = "Drivers for this database are available at:\n"
                sec.append(
                    nodes.paragraph('', '',
                        nodes.Text(text, text),
                        nodes.reference('', '',
                            nodes.Text(content['driverurl'], content['driverurl']),
                            refuri=content['driverurl']
                        )
                    )
                )
            output.append(sec)


        if "connectstring" in content:
            sec = nodes.section(
                    '',
                    nodes.title("Connecting", "Connecting"),
                    nodes.paragraph('', '',
                        nodes.Text("Connect String:", "Connect String:"),
                        nodes.literal_block(content['connectstring'],
                            content['connectstring'])
                    ),
                    ids=["dialect-%s-%s-connect" % (dialect_name, dbapi_name)]
            )
            output.append(sec)

        return output

    def _dialect_node(self):
        self._dialects[self.dialect_name] = self

        content = self._parse_content()
        self.database_name = content['name']

        self.bullets = nodes.bullet_list()
        text = "The following dialect/DBAPI options are available.  "\
                "Please refer to individual DBAPI sections for connect information."
        sec = nodes.section('',
                nodes.paragraph('', '',
                    nodes.Text(
                        "Support for the %s database." % content['name'],
                        "Support for the %s database." % content['name']
                    ),
                ),
                nodes.title("DBAPI Support", "DBAPI Support"),
                nodes.paragraph('', '',
                    nodes.Text(text, text),
                    self.bullets
                ),
                ids=["dialect-%s" % self.dialect_name]
            )

        return [sec]

    def _append_dbapi_bullet(self, dialect_name, dbapi_name, name, idname):
        env = self.state.document.settings.env
        dialect_directive = self._dialects[dialect_name]
        try:
            relative_uri = env.app.builder.get_relative_uri(dialect_directive.docname, self.docname) 
        except:
            relative_uri = ""
        list_node = nodes.list_item('',
                nodes.paragraph('', '',
                    nodes.reference('', '',
                                nodes.Text(name, name),
                                refdocname=self.docname,
                                refuri= relative_uri + "#" + idname
                            ),
                    #nodes.Text(" ", " "),
                    #nodes.reference('', '',
                    #            nodes.Text("(connectstring)", "(connectstring)"),
                    #            refdocname=self.docname,
                    #            refuri=env.app.builder.get_relative_uri(
                    #                    dialect_directive.docname, self.docname) +
                    ##                        "#" + ("dialect-%s-%s-connect" %
                    #                                (dialect_name, dbapi_name))
                    #        )
                    )
            )
        dialect_directive.bullets.append(list_node)

    def run(self):
        env = self.state.document.settings.env
        self.docname = env.docname

        self.dialect_name = dialect_name = self.content[0]

        has_dbapi = "+" in dialect_name
        if has_dbapi:
            return self._dbapi_node()
        else:
            return self._dialect_node()

def setup(app):
    app.add_directive('dialect', DialectDirective)

