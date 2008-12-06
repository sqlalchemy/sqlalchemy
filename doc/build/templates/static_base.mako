<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">

<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        ${metatags and metatags or ''}
        <title>${capture(self.show_title)|util.striptags} &mdash; ${docstitle|h}</title>
        ${self.headers()}
    </head>
    <body>
        ${next.body()}
    </body>
</html>


<%!
    local_script_files = []
%>
