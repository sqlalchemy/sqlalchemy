<%flags>inherit="doclib.myt"</%flags>

<%python scope="global">

    files = [
        #'tutorial',
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
    title='SQLAlchemy 0.1 Documentation'
    version = '0.1.7'
</%attr>

<%method title>
% try:
#  avoid inheritance via attr instead of attributes
    <% m.base_component.attr['title'] %> - <% self.owner.attr['title'] %>
% except KeyError:
    <% self.owner.attr['title'] %>
%
</%method>






