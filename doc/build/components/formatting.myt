<%doc>formatting.myt - library of HTML formatting functions to operate on a TOCElement tree</%doc>

<%global>
    import string, re
    import highlight
</%global>


<%def printtocelement>
<%doc>prints a TOCElement as a table of contents item and prints its immediate child items</%doc>
    <%args>
        item
        includefile
        bold = False
        full = False
        children = True
    </%args>

    <div class="toclink">
        <A style="<% bold and "font-weight:bold;" or "" %>" href="<% item.get_link(includefile, anchor = (not includefile)) %>"><% item.description %></a>
    </div>
    
% if children:  
    <div class="toclinkcontainer">
%   for i in item.children:
        <& printsmtocelem, item=i, includefile = includefile, children=full &>
%
    </div>
%
</%def>

<%def printsmtocelem>
    <%args>
        item
        includefile
        children = False
    </%args>    
    <div class="toclinkcontainer">
    
    <div class="smalltoclink">
    <A href="<% item.get_link(includefile) %>"><% item.description %></a>
    </div>

% if children:
%   for i in item.children:
        <& printsmtocelem, item = i, includefile = includefile &>
%
%
    </div>

</%def>

<%method printtoc>
<%args> 
    root
    includefile
    current = None
    full = False
    children = True
</%args>

% header = False
% for i in root.children:

%   if i.header:
%       if header:
    </div>
%
%   header = True
    <b><% i.header %></b><br/>
    <div class="tocsection">
%
    <& printtocelement, item=i, includefile = includefile, bold = (i == current and includefile), full = full, children=children &>
%

%   if header:
    </div>
%
</%method>


<%method printitem>
<%doc>prints the description and contents of a TOC element and recursively prints its child items</%doc>

<%args>
    item
    indentlevel = 0
    includefile
    omitheader = False
    root = None
</%args>

% if root is None: root = item

% if not omitheader:
<A name="<% item.path %>"></a>
%

<div class="subsection" style="margin-left:<% repr(10 + indentlevel * 10) %>px;">

% if not omitheader:
    <span class="sectionheadertext"><% item.description %></span>
%
    <div class="sectiontext">

<%python>
    regexp = re.compile(r"__FORMAT:LINK{(?:\@path=(.+?))?(?:\@xtra=(.+?))?(?:\@text=(.+?))?(?:\@href=(.+?))?(?:\@class=(.+?))?}")
    def link(matchobj):
        path = matchobj.group(1)
        xtra = matchobj.group(2)
        text = matchobj.group(3)
        href = matchobj.group(4)
        class_ = matchobj.group(5)
        
        if class_ is not None:
            class_ = 'class="%s"' % class_
            
        if href:
            return '<a href="%s" %s>%s</a>' % (href, class_, text or href)
        else:
            try:
                element = item.lookup(path)
                if xtra is not None:
                    return '<a href="%s_%s" %s>%s</a>' % (element.get_link(includefile), xtra, class_, text or xtra)
                else:
                    return '<a href="%s" %s>%s</a>' % (element.get_link(includefile), class_, text or element.description)
            except KeyError:
                if xtra is not None:
                    return '<b>%s</b>' % (text or xtra)
                else:
                    return '<b>%s</b>' % text or path

    re2 = re.compile(r"'''PYESC(.+?)PYESC'''", re.S)
    content = regexp.sub(link, item.content)
    content = re2.sub(lambda m: m.group(1), content)
    #m.write(item.content)
    m.write(content)
</%python>

    </div>

%   for i in item.children:
        <& printitem, item=i, indentlevel=indentlevel + 1, includefile = includefile, root=root &>
%

% if root is not None and len(item.children) == 0:
    <a href="#<% root.path %>" class="toclink">back to section top</a>
%

% if indentlevel == 0:
%   if includefile:
    <& SELF:pagenav, item=item, includefile=includefile &>
%   else:
    <hr width="400px" align="left" />
% #
% 

</div>

</%method>

<%method pagenav>
<%args>
    item
    includefile
</%args>
<div class="sectionnavblock">
<div class="sectionnav">

% if not includefile:
    <a href="#top">Top</a> |
%

%       if item.previous is not None:
        Previous: <& SELF:itemlink, item=item.previous, includefile = includefile &>
%       # end if

%       if item.next is not None:
%               if item.previous is not None:
                |
%               # end if

        Next: <& SELF:itemlink, item=item.next, includefile = includefile &>
%       # end if

</div>
</div>
</%method>

<%method itemlink trim="both">
    <%args>
    item
    includefile
    </%args>
    <a href="<% item.get_link(includefile, anchor = (not includefile)) %>"><% item.description %></a>
</%method>



<%method paramtable>
    <table cellspacing="0" width="100%">
    <% m.content() %>
    </table>
</%method>

<%method param>
        <%args>
        name
        classname
        type
    users = 'all'
        default = None
    version = None
        </%args>
% if default is None: default = 'None'

<&|SELF:fliprow, flip=True &>
    <td valign="top">
    <A name="<% m.comp('doclib.myt:current').path %>_<% name %>"></a>
    <b><% name %></b> (<% type %>)</td>
    <td align="right" width="40%">
        <div style="text-align:left">
% if users is not None:
        for users: <% users %><br/>
%
        default: <% default %><br/>
        used by: <% classname %>
% if version:
        <br/>since version: <% version %>
%
        </div>
    </td>
</&>

<&|SELF:fliprow, flip=False &>
    <td colspan="2">
    <p style="margin-left:15px;margin-bottom:5px;margin-top:5px"><% m.content() %></p>

    </td>
</&>

</%method>

<%method function_doc>
    <%args>
        name = ""
        alt = None
        arglist = []
        rettype = None
    </%args>
    <&|SELF:fliprow, flip=True&>
    <td>
    <A name="<% m.comp('doclib.myt:current').path %>_<% name %>"></a>
    <b><% name %>(<% string.join(map(lambda k: "<i>%s</i>" % k, arglist), ", ")%>)</b></td>
    </td>
    </&>
    <&|SELF:fliprow, flip=False&>
    <td colspan="2"><div style="margin-left:15px;margin-bottom:5px;margin-top:5px"><% m.content() %>
%   if alt is not None:
    <br/><br/><b>Also called as:</b> <% alt %>
%
    </div>
    
    </td>
    </&>
</%method>

<%method member_doc>
    <%args>
        name = ""
        type = None
    </%args>
    <&|SELF:fliprow, flip=True&>
    <td>
    <A name="<% m.comp('doclib.myt:current').path %>_<% name %>"></a>
    <b><% name %></b></td>
    </td><td></td>
    </&>
    <&|SELF:fliprow, flip=False&>
    <td colspan="2"><div style="margin-left:15px;margin-bottom:5px;margin-top:5px"><% m.content() %>
    </div>
    
    </td>
    </&>
</%method>


<%method fliprow trim="both">
    <%args>flip=True</%args>
    <%python>
        flipper = m.get_attribute("formatflipper")
        if flipper is None: 
            flipper = Value("light")
            m.set_attribute("formatflipper", flipper)
    </%python>
    
% if flip: flipper({"light":"dark", "dark": "light"}[flipper()])
    <tr class="<% flipper() %>"><% m.content() %></tr>
   
</%method>

<%method codeline trim="both">
<span class="codeline"><% m.content() %></span>
</%method>

<%method code autoflush=False>
<%args>
    title = None
    syntaxtype = 'python'
    html_escape = False
</%args>

<%init>
    def fix_indent(f):
        f =string.expandtabs(f, 4)
        g = ''
        lines = string.split(f, "\n")
        whitespace = None
        for line in lines:
            if whitespace is None:
                match = re.match(r"^([ ]+)", line)
                if match is not None:
                    whitespace = match.group(1)

            if whitespace is not None:
                line = re.sub(r"^%s" % whitespace, "", line)

            if whitespace is not None or re.search(r"\w", line) is not None:
                g += (line + "\n")


        return g.rstrip()

    content = highlight.highlight(fix_indent(m.content()), html_escape = html_escape, syntaxtype = syntaxtype)

</%init>
<div class="code">
% if title is not None:
    <div class="codetitle"><% title %></div>
%
<pre><% content %></pre></div>
</%method>

<%method link trim="both">
    <%args>
        path = None
        param = None
        method = None
        member = None
        text = None
        href = None
        class_ = None
    </%args>
    <%init>
        if href is None and path is None:
            path = m.comp('doclib.myt:current').path
            
        extra = (param or method or member)
    </%init>
__FORMAT:LINK{<% path and "@path=" + path or "" %><% extra and "@xtra=" + extra or "" %><% text and "@text=" + text or "" %><% href and "@href=" + href or "" %><% class_ and "@class=" + class_ or "" %>}
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

<%method codepopper trim="both">
    <%args>
        link
    </%args>
    <%init>
        href = m.scomp('SELF:popboxlink')
    </%init>
    '''PYESC<& SELF:link, href=href, text=link, class_="codepoplink" &>PYESC'''
    '''PYESC<&|SELF:popbox, class_="codepop" &><% m.content() %></&>PYESC'''
</%method>