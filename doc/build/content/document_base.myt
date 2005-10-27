<%flags>inherit="doclib.myt"</%flags>

<%python scope="global">

    files = [
        'roadmap',
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
    version = '0.91'
</%attr>






