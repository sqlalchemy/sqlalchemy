## coding: utf-8
<%inherit file="${context['mako_layout']}"/>

<%block name="headers">
    <link rel="stylesheet" href="${pathto('_static/pygments.css', 1)}" type="text/css" />
    <link rel="stylesheet" href="${pathto('_static/docs.css', 1)}" type="text/css" />

    <script type="text/javascript">
      var DOCUMENTATION_OPTIONS = {
          URL_ROOT:    '${pathto("", 1)}',
          VERSION:     '${release|h}',
          COLLAPSE_MODINDEX: false,
          FILE_SUFFIX: '${file_suffix}'
      };
    </script>
    % for scriptfile in script_files + self.attr.local_script_files:
        <script type="text/javascript" src="${pathto(scriptfile, 1)}"></script>
    % endfor
    <script type="text/javascript" src="${pathto('_static/init.js', 1)}"></script>
    % if hasdoc('about'):
        <link rel="author" title="${_('About these documents')}" href="${pathto('about')}" />
    % endif
    <link rel="index" title="${_('Index')}" href="${pathto('genindex')}" />
    <link rel="search" title="${_('Search')}" href="${pathto('search')}" />
    % if hasdoc('copyright'):
        <link rel="copyright" title="${_('Copyright')}" href="${pathto('copyright')}" />
    % endif
    <link rel="top" title="${docstitle|h}" href="${pathto('index')}" />
    % if parents:
        <link rel="up" title="${parents[-1]['title']|util.striptags}" href="${parents[-1]['link']|h}" />
    % endif
    % if nexttopic:
        <link rel="next" title="${nexttopic['title']|util.striptags}" href="${nexttopic['link']|h}" />
    % endif
    % if prevtopic:
        <link rel="prev" title="${prevtopic['title']|util.striptags}" href="${prevtopic['link']|h}" />
    % endif
</%block>

<h1>${docstitle|h}</h1>

<div id="search">
Search:
<form class="search" action="${pathto('search')}" method="get">
  <input type="text" name="q" size="18" /> <input type="submit" value="${_('Search')}" />
  <input type="hidden" name="check_keywords" value="yes" />
  <input type="hidden" name="area" value="default" />
</form>
</div>

<div class="versionheader">
    Version: <span class="versionnum">${release}</span> Last Updated: ${last_updated}
</div>
<div class="clearboth"></div>

<div id="topnav">
    <div id="pagecontrol">
        <ul>
        % if prevtopic:
            <li>Prev:
            <a href="${prevtopic['link']|h}" title="${_('previous chapter')}">${prevtopic['title']}</a>
            </li>
        % endif
        % if nexttopic:
            <li>Next:
            <a href="${nexttopic['link']|h}" title="${_('next chapter')}">${nexttopic['title']}</a>
            </li>
        % endif

        <li>
            <a href="${pathto('contents')}">Table of Contents</a> |
            <a href="${pathto('genindex')}">Index</a>
            % if sourcename:
            | <a href="${pathto('_sources/' + sourcename, True)|h}">${_('view source')}
            % endif
        </li>
        </ul>
    </div>
    <div id="navbanner">
        <a class="totoc" href="${pathto('index')}">${docstitle|h}</a>
        % if parents:
            % for parent in parents:
                » <a href="${parent['link']|h}" title="${parent['title']}">${parent['title']}</a>
            % endfor
        % endif
        % if current_page_name != 'index':
        » ${self.show_title()} 
        % endif

        <h2>
            <%block name="show_title">
            % if title:
                ${title}
            % endif
            </%block>
        </h2>
    % if display_toc and \
            not current_page_name.startswith('index') and \
            not current_page_name.startswith('contents'):
        ${toc}
    % endif
    </div>
    <div class="clearboth"></div>
</div>

<div class="document">
    <div class="body">
        ${next.body()}
    </div>
</div>

<%block name="footer">
    <div class="bottomnav">
        % if prevtopic:
            Previous:
            <a href="${prevtopic['link']|h}" title="${_('previous chapter')}">${prevtopic['title']}</a>
        % endif
        % if nexttopic:
            Next:
            <a href="${nexttopic['link']|h}" title="${_('next chapter')}">${nexttopic['title']}</a>
        % endif
        <div class="doc_copyright">
        % if hasdoc('copyright'):
            &copy; <a href="${pathto('copyright')}">Copyright</a> ${copyright|h}.
        % else:
            &copy; Copyright ${copyright|h}.
        % endif
        % if show_sphinx:
            Created using <a href="http://sphinx.pocoo.org/">Sphinx</a> ${sphinx_version|h}.
        % endif
        </div>
    </div>
</%block>


