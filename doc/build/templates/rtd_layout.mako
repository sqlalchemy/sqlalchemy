<%inherit file="/layout.mako"/>

<%
    newscript = []
    # strip out script files that RTD wants to provide
    for script in script_files:
        for token in ("jquery.js", "underscore.js", "doctools.js"):
            if token in script:
                break
        else:
            newscript.append(script)
    script_files[:] = newscript
%>

<%block name="headers">
<!-- RTD <head> -->
<script type="text/javascript" src="http://ajax.googleapis.com/ajax/libs/jquery/1.4/jquery.min.js"></script>
<script type="text/javascript" src="${MEDIA_URL}javascript/underscore.js"></script>
<script type="text/javascript" src="${MEDIA_URL}javascript/doctools.js"></script>
<script type="text/javascript" src="${MEDIA_URL}javascript/searchtools.js"></script>
  <script type="text/javascript">
    var doc_version = "${current_version}";
    var doc_slug = "${slug}";
    var static_root = "${pathto('_static', 1)}"
  </script>
<!-- end RTD <head> -->
    ${parent.headers()}
</%block>

${next.body()}

<%block name="footer">
${parent.footer()}
 <!-- End original user content -->
## Keep this here, so that the RTD logo doesn't stomp on the bottom of the theme.
<br>
<br>
<br>

<style type="text/css">
  .badge {
    position: fixed;
    display: block;
    bottom: 5px;
    height: 40px;
    text-indent: -9999em;
    border-radius: 3px;
    -moz-border-radius: 3px;
    -webkit-border-radius: 3px;
    box-shadow: 0 1px 0 rgba(0, 0, 0, 0.2), 0 1px 0 rgba(255, 255, 255, 0.2) inset;
    -moz-box-shadow: 0 1px 0 rgba(0, 0, 0, 0.2), 0 1px 0 rgba(255, 255, 255, 0.2) inset;
    -webkit-box-shadow: 0 1px 0 rgba(0, 0, 0, 0.2), 0 1px 0 rgba(255, 255, 255, 0.2) inset;
  }
  #version_menu {
    position: fixed;
    display: none;
    bottom: 11px;
    right: 166px;
    list-style-type: none;
    margin: 0;
  }
  .footer_popout:hover #version_menu {
    display: block;
  }
  #version_menu li {
    display: block;
    float: right;
  }
  #version_menu li a {
    display: block;
    padding: 6px 10px 4px 10px;
    margin: 7px 7px 0 0;
    font-weight: bold;
    font-size: 14px;
    height: 20px;
    line-height: 17px;
    text-decoration: none;
    color: #fff;
    background: #8ca1af url(http://media.readthedocs.org/images/gradient-light.png) bottom left repeat-x;
    border-radius: 3px;
    -moz-border-radius: 3px;
    -webkit-border-radius: 3px;
    box-shadow: 0 1px 1px #465158;
    -moz-box-shadow: 0 1px 1px #465158;
    -webkit-box-shadow: 0 1px 1px #465158;
    text-shadow: 0 1px 1px rgba(0, 0, 0, 0.5);
  }
  #version_menu li a:hover {
    text-decoration: none;
    background-color: #697983;
    box-shadow: 0 1px 0px #465158;
    -moz-box-shadow: 0 1px 0px #465158;
    -webkit-box-shadow: 0 1px 0px #465158;
  }
  .badge.rtd {
    background: #257597 url(http://media.readthedocs.org/images/badge-rtd.png) top left no-repeat;
    border: 1px solid #282E32;
    width: 160px;
    right: 5px;
  }
  .badge.revsys { background: #465158 url(http://media.readthedocs.org/images/badge-revsys.png) top left no-repeat;
    border: 1px solid #1C5871;
    width: 290px;
    right: 173px;
  }
  .badge.revsys-inline-sponsored {
    position: inherit;
    margin-left: auto;
    margin-right: 175px;
    margin-bottom: 5px;
    background: #465158 url(http://media.readthedocs.org/images/badge-revsys.png) top left no-repeat;
    border: 1px solid #1C5871;
    width: 290px;
    right: 173px;
  }
  .badge.revsys-inline {
    position: inherit;
    margin-left: auto;
    margin-right: 175px;
    margin-bottom: 5px;
    background: #465158 url(http://media.readthedocs.org/images/badge-revsys-sm.png) top left no-repeat;
    border: 1px solid #1C5871;
    width: 205px;
    right: 173px;
  }

</style>
<div class="rtd_doc_footer">
  <div class="footer_popout">
    <a href="http://readthedocs.org/projects/${slug}/?fromdocs=${slug}" class="badge rtd">Brought to you by Read the Docs</a>
    <ul id="version_menu">
         ## rtd fills this in client side
    </ul>
  </div>
</div>
<!-- RTD Analytics Code -->
<script type="text/javascript">
  var _gaq = _gaq || [];
  _gaq.push(['_setAccount', 'UA-17997319-1']);
  _gaq.push(['_trackPageview']);

  (function() {
    var ga = document.createElement('script'); ga.type = 'text/javascript'; ga.async = true;
    ga.src = ('https:' == document.location.protocol ? 'https://ssl' : 'http://www') + '.google-analytics.com/ga.js';
    var s = document.getElementsByTagName('script')[0]; s.parentNode.insertBefore(ga, s);
  })();
</script>

% if analytics_code:
<!-- User Analytics Code -->
<script type="text/javascript">
  var _gaq = _gaq || [];
  _gaq.push(['_setAccount', '${analytics_code}']);
  _gaq.push(['_trackPageview']);

  (function() {
    var ga = document.createElement('script'); ga.type = 'text/javascript'; ga.async = true;
    ga.src = ('https:' == document.location.protocol ? 'https://ssl' : 'http://www') + '.google-analytics.com/ga.js';
    var s = document.getElementsByTagName('script')[0]; s.parentNode.insertBefore(ga, s);
  })();
</script>
% endif

</%block>
