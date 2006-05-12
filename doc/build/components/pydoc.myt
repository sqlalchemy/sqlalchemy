<%global>
    import re, types, string, docstring
    def format_paragraphs(text):
        return re.sub(r'([\w])\n([\w])', r'\1 \2', text or '', re.S)
</%global>

<%method obj_doc>
    <%args>
        obj
    </%args>
    
<&|doclib.myt:item, name=obj.name, description=obj.description &>
<&|formatting.myt:formatplain&><% format_paragraphs(obj.doc) %></&>

% if not obj.isclass and obj.functions:
<&|doclib.myt:item, name="modfunc", description="Module Functions" &>
<&|formatting.myt:paramtable&>
%   for func in obj.functions:
    <& SELF:function_doc, func=func &>
%
</&>
</&>
% else:
% if obj.functions:
<&|formatting.myt:paramtable&>
%   for func in obj.functions:
%   if isinstance(func, docstring.FunctionDoc):
    <& SELF:function_doc, func=func &>
%   elif isinstance(func, docstring.PropertyDoc):
    <& SELF:property_doc, prop=func &>
%
%
</&>
%
%

% if obj.classes:
<&|formatting.myt:paramtable&>
%   for class_ in obj.classes:
      <& SELF:obj_doc, obj=class_ &>
%   
</&>
%    
</&>

</%method>

<%method function_doc>
    <%args>func</%args>
    <&|formatting.myt:function_doc, name=func.name, link=func.link, arglist=func.arglist &>
    <&|formatting.myt:formatplain&><% format_paragraphs(func.doc) %></&>
    </&>
</%method>


<%method property_doc>
    <%args>
        prop
    </%args>
    <&|formatting.myt:member_doc, name=prop.name, link=prop.link &>
    <&|formatting.myt:formatplain&><% format_paragraphs(prop.doc) %></&>
    </&>    
</%method>