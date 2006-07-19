<%flags>inherit="doclib.myt"</%flags>

<%python scope="global">

    files = [
        'tutorial',
        'dbengine',
        'metadata',
        'sqlconstruction',
        'datamapping',
        'unitofwork',
        'adv_datamapping',
        'types',
        'pooling',
        'plugins',
        'docstrings',
        ]

</%python>

<%attr>
    files=files
    wrapper='section_wrapper.myt'
    onepage='documentation'
    index='index'
    title='SQLAlchemy 0.2 Documentation'
    version = '0.2.6'
</%attr>

<%method title>
% try:
#  avoid inheritance via attr instead of attributes
    <% m.base_component.attr['title'] %> - <% self.owner.attr['title'] %>
% except KeyError:
    <% self.owner.attr['title'] %>
%
</%method>





