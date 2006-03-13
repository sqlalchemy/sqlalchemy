<%flags>inherit="doclib.myt"</%flags>

<%python scope="global">

    files = [
        'trailmap',
        'pooling',
        'dbengine',
        'metadata',
        'sqlconstruction',
        'datamapping',
        'unitofwork',
        'adv_datamapping',
        'types',
        'docstrings',
        ]

</%python>

<%attr>
    files=files
    wrapper='section_wrapper.myt'
    onepage='documentation'
    index='index'
    title='SQLAlchemy Documentation'
    version = '0.1.4'
</%attr>

<%method title>
% try:
#  avoid inheritance via attr instead of attributes
    <% m.base_component.attr['title'] %> - SQLAlchemy Documentation
% except KeyError:
    SQLAlchemy Documentation
%
</%method>





