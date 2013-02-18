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
            import urllib2
            template_url = builder.config['site_base'] + "/docs_base.mako"
            template = urllib2.urlopen(template_url).read()
            self.lookup.put_string("/rtd_base.mako", template)

    def render(self, template, context):
        template = template.replace(".html", ".mako")
        context['prevtopic'] = context.pop('prev', None)
        context['nexttopic'] = context.pop('next', None)

        # RTD layout
        if rtd:
            # add variables if not present, such
            # as if local test of READTHEDOCS variable
            if 'MEDIA_URL' not in context:
                context['MEDIA_URL'] = "http://media.readthedocs.org/"
            if 'slug' not in context:
                context['slug'] = context['project'].lower()
            if 'url' not in context:
                context['url'] = "/some/test/url"
            if 'current_version' not in context:
                context['current_version'] = "latest"

            if 'name' not in context:
                context['name'] = context['project'].lower()

            context['rtd'] = True
            context['toolbar'] = True
            context['layout'] = "rtd_layout.mako"
            context['base'] = "rtd_base.mako"

            context['pdf_url'] = "%spdf/%s/%s/%s.pdf" % (
                    context['MEDIA_URL'],
                    context['slug'],
                    context['current_version'],
                    context['slug']
            )
        # local docs layout
        else:
            context['rtd'] = False
            context['toolbar'] = False
            context['layout'] = "layout.mako"
            context['base'] = "static_base.mako"

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

