<%doc>formatting.myt - Provides section formatting elements, syntax-highlighted code blocks, and other special filters.</%doc>

<%global>
    import string, re
    import highlight
</%global>

<%method section>
<%doc>Main section formatting element.</%doc>
<%args>
    toc
    path
    description=None
    onepage=False
</%args>
<%init>
    item = toc.get_by_path(path)
    if item is None:
        raise "path: " + path
</%init>

<A name="<% item.path %>"></a>

<div class="subsection" style="margin-left:<% repr(item.depth * 10) %>px;">

<%python>
    content = m.content()
    re2 = re.compile(r"'''PYESC(.+?)PYESC'''", re.S)
    content = re2.sub(lambda m: m.group(1), content)
</%python>

% if item.depth > 1:
<h3><% description or item.description %></h3>
%

    <div class="sectiontext">
    <% content %>
    </div>

% if onepage or item.depth > 1:
%   if (item.next and item.next.depth >= item.depth):
    <a href="#<% item.get_page_root().path %>" class="toclink">back to section top</a>
%
% else:
    <a href="#<% item.get_page_root().path %>" class="toclink">back to section top</a>
    <& nav.myt:pagenav, item=item, onepage=onepage &>
% 
</div>

</%method>


<%method formatplain>
    <%filter>
        import re
        f = re.sub(r'\n[\s\t]*\n[\s\t]*', '</p>\n<p>', f)
        f = "<p>" + f + "</p>"
        return f
    </%filter>
<% m.content() | h%>
</%method>




<%method codeline trim="both">
<span class="codeline"><% m.content() %></span>
</%method>

<%method code autoflush=False>
<%args>
    title = None
    syntaxtype = 'python'
    html_escape = False
    use_sliders = False
</%args>

<%init>
    def fix_indent(f):
        f =string.expandtabs(f, 4)
        g = ''
        lines = string.split(f, "\n")
        whitespace = None
        for line in lines:
            if whitespace is None:
                match = re.match(r"^([ ]*).+", line)
                if match is not None:
                    whitespace = match.group(1)

            if whitespace is not None:
                line = re.sub(r"^%s" % whitespace, "", line)

            if whitespace is not None or re.search(r"\w", line) is not None:
                g += (line + "\n")


        return g.rstrip()

    p = re.compile(r'<pre>(.*?)</pre>', re.S)
    def hlight(match):
        return "<pre>" + highlight.highlight(fix_indent(match.group(1)), html_escape = html_escape, syntaxtype = syntaxtype) + "</pre>"
    content = p.sub(hlight, "<pre>" + m.content() + "</pre>")
</%init>
<div class="<% use_sliders and "sliding_code" or "code" %>">
% if title is not None:
    <div class="codetitle"><% title %></div>
%
<% content %></div>
</%method>




<%method popboxlink trim="both"> 
    <%args>
        name=None
        show='show'
        hide='hide'
    </%args>
    <%init>
        if name is None:
            name = m.attributes.setdefault('popbox_name', 0)
        name += 1
        m.attributes['popbox_name'] = name
        name = "popbox_" + repr(name)
    </%init>
javascript:togglePopbox('<% name %>', '<% show %>', '<% hide %>')
</%method>

<%method popbox trim="both">
<%args>
    name = None
    class_ = None
</%args>
<%init>
    if name is None:
        name = 'popbox_' + repr(m.attributes['popbox_name'])
</%init>
<div id="<% name %>_div" class="<% class_ %>" style="display:none;"><% m.content().strip() %></div>
</%method>

<%method poplink trim="both">
    <%args>
        link='sql'
    </%args>
    <%init>
        href = m.scomp('SELF:popboxlink')
    </%init>
    '''PYESC<& nav.myt:link, href=href, text=link, class_="codepoplink" &>PYESC'''
</%method>

<%method codepopper trim="both">
	<%init>
		c = m.content()
		c = re.sub(r'\n', '<br/>\n', c.strip())
	</%init>
    </pre><&|SELF:popbox, class_="codepop" &><% c %></&><pre>
</%method>

<%method poppedcode trim="both">
	<%init>
		c = m.content()
		c = re.sub(r'\n', '<br/>\n', c.strip())
	</%init>
    </pre><div class="codepop"><% c %></div><pre>
</%method>
