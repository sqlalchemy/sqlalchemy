<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">

<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
        ${metatags and metatags or ''}
        <title>
            <%block name="head_title">
            </%block>
        </title>
        <%block name="headers"/>
    </head>
    <body>
        ${next.body()}
        <%block name="footer"/>
    </body>
</html>


