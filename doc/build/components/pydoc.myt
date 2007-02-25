<%doc>pydoc.myt - provides formatting functions for printing docstring.AbstractDoc generated python documentation objects.</%doc>

<%global>
import docstring
import sys

def trim(docstring):
    if not docstring:
        return ''
    # Convert tabs to spaces (following the normal Python rules)
    # and split into a list of lines:
    lines = docstring.expandtabs().splitlines()
    # Determine minimum indentation (first line doesn't count):
    indent = sys.maxint
    for line in lines[1:]:
        stripped = line.lstrip()
        if stripped:
            indent = min(indent, len(line) - len(stripped))
    # Remove indentation (first line is special):
    trimmed = [lines[0].strip()]
    if indent < sys.maxint:
        for line in lines[1:]:
            trimmed.append(line[indent:].rstrip())
    # Strip off trailing and leading blank lines:
    while trimmed and not trimmed[-1]:
        trimmed.pop()
    while trimmed and not trimmed[0]:
        trimmed.pop(0)
    # Return a single string:
    return '\n'.join(trimmed)
    
</%global>

<%method obj_doc>
    <%args>
        obj
        toc
        extension
    </%args>
<%init>
if obj.isclass:
    links = []
    for elem in obj.inherits:
        if isinstance(elem, docstring.ObjectDoc):
            links.append(m.scomp("nav.myt:toclink", toc=toc, path=elem.toc_path, extension=extension, description=elem.name))
        else:
            links.append(str(elem))
    htmldescription = "class " + obj.classname + "(%s)" % (','.join(links))
else:
    htmldescription = obj.description

</%init>

<&|formatting.myt:section, toc=toc, path=obj.toc_path, description=htmldescription &>
<pre><% trim(obj.doc) |h %></pre>

% if not obj.isclass and obj.functions:

<&|formatting.myt:section, toc=toc, path=obj.mod_path &>
%   for func in obj.functions:
    <& SELF:function_doc, func=func &>
%
</&>

% else:

% if obj.functions:
%   for func in obj.functions:
%   if isinstance(func, docstring.FunctionDoc):
    <& SELF:function_doc, func=func &>
%   elif isinstance(func, docstring.PropertyDoc):
    <& SELF:property_doc, prop=func &>
%
%
%
%

% if obj.classes:
%   for class_ in obj.classes:
      <& SELF:obj_doc, obj=class_, toc=toc, extension=extension &>
%   
%    
</&>
</%method>

<%method function_doc>
    <%args>func</%args>
        <div class="darkcell">
        <A name=""></a>
        <b><% func.name %>(<% ", ".join(map(lambda k: "<i>%s</i>" % k, func.arglist))%>)</b>
        <div class="docstring">
        <pre><% trim(func.doc) |h %></pre>
        </div>
        </div>
</%method>


<%method property_doc>
    <%args>
        prop
    </%args>
         <div class="darkcell">
         <A name=""></a>
         <b><% prop.name %></b>
         <div class="docstring">
         <pre><% trim(prop.doc) |h%></pre>
         </div> 
         </div>
</%method>


