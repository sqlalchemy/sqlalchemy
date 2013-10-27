from __future__ import absolute_import

from sphinx.application import TemplateBridge
from sphinx.jinja2glue import BuiltinTemplateLoader
from mako.lookup import TemplateLookup
import os

rtd = os.environ.get('READTHEDOCS', None) == 'True'

class MakoBridge(TemplateBridge):
    def init(self, builder, *args, **kw):
        self.jinja2_fallback = BuiltinTemplateLoader()
        self.jinja2_fallback.init(builder, *args, **kw)

        builder.config.html_context['release_date'] = builder.config['release_date']
        builder.config.html_context['site_base'] = builder.config['site_base']

        self.lookup = TemplateLookup(directories=builder.config.templates_path,
            #format_exceptions=True,
            imports=[
                "from builder import util"
            ]
        )

        if rtd:
            # RTD layout, imported from sqlalchemy.org
            import urllib2
            template = urllib2.urlopen(builder.config['site_base'] + "/docs_adapter.mako").read()
            self.lookup.put_string("docs_adapter.mako", template)

            setup_ctx = urllib2.urlopen(builder.config['site_base'] + "/docs_adapter.py").read()
            lcls = {}
            exec(setup_ctx, lcls)
            self.setup_ctx = lcls['setup_context']

    def setup_ctx(self, context):
        pass

    def render(self, template, context):
        template = template.replace(".html", ".mako")
        context['prevtopic'] = context.pop('prev', None)
        context['nexttopic'] = context.pop('next', None)

        # local docs layout
        context['rtd'] = False
        context['toolbar'] = False
        context['base'] = "static_base.mako"

        # override context attributes
        self.setup_ctx(context)

        context.setdefault('_', lambda x: x)
        return self.lookup.get_template(template).render_unicode(**context)

    def render_string(self, template, context):
        # this is used for  .js, .css etc. and we don't have
        # local copies of that stuff here so use the jinja render.
        return self.jinja2_fallback.render_string(template, context)

def setup(app):
    app.config['template_bridge'] = "builder.mako.MakoBridge"
    app.add_config_value('release_date', "", 'env')
    app.add_config_value('site_base', "", 'env')
    app.add_config_value('build_number', "", 'env')

