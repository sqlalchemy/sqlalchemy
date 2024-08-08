This directory contains some routines for managing the SQLAlchemy source code.

It requires Fabric to run (https://fabfile.org), which can be installed via:

    pip install fabric
    
To list commands, from the main SQLAlchemy source directory:

    fab -l

To invoke a command, supply it as a commandline argument:

    fab examples-analyze
