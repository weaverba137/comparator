# Licensed under a 3-clause BSD style license - see LICENSE.rst.
# -*- coding: utf-8 -*-
"""
comparator.initialize
=====================

Obtain filesystem metadata necessary for comparing the same data set
at different locations.
"""
import os
from sqlalchemy import create_engine, func
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from .db import engine, Session, Base, FileSystem, Directory, File
from .find import directories, files


def _options():
    """Parse the command-line options.

    Returns
    -------
    The parsed options.
    """
    from sys import argv
    from argparse import ArgumentParser
    xct = os.path.basename(argv[0])
    desc = "Obtain filesystem metadata necessary for comparing the same data set at different locations."
    prsr = ArgumentParser(description=desc, prog=xct)
    prsr.add_argument('-f', '--filesystem', action='append',
                      dest='filesystem', metavar="DIR",
                      help='FileSystem(s) to examine.')
    prsr.add_argument('-F', '--skip-files', action='store_true',
                      dest='skip_files', help='Skip the file search stage.')
    # prsr.add_argument('-l', '--log-dir', dest='logging', metavar='DIR',
    #                   default=os.path.join(os.environ['HOME'], 'Documents', 'Logs'),
    #                   help='Log files in DIR (default %(default)s).')
    # prsr.add_argument('-R', '--root', dest='root', metavar='DIR',
    #                   default='/global/project/projectdirs',
    #                   help='Path containing metadata directory (default %(default)s).')
    # prsr.add_argument('-s', '--sql', dest='sql', action='store_true',
    #                   help='Output SQL statements instead of loading database.')
    prsr.add_argument('-o', '--overwrite', action='store_true',
                      dest='overwrite', help='Overwrite any existing database.')
    prsr.add_argument('-v', '--verbose', action='store_true', dest='verbose',
                      help='Log extra debugging information.')
    prsr.add_argument('release', metavar='RELEASE',
                      help='Release to examine, e.g. "dr15".')
    prsr.add_argument('database', metavar='DB',
                      help='Path to database file.')
    return prsr.parse_args()


def main():
    """Entry point for command-line scripts.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    #
    # Arguments
    #
    options = _options()
    #
    # Initialize database.
    #
    if options.overwrite and os.path.exists(options.database):
        os.remove(options.database)
    engine = create_engine('sqlite:///'+options.database, echo=options.verbose)
    Session.remove()
    Session.configure(bind=engine, autocommit=False,
                      autoflush=True, expire_on_commit=True)
    Base.metadata.create_all(engine)
    #
    # Add filesystems.
    #
    try:
        q = Session.query(FileSystem).one()
    except NoResultFound:
        Session.add_all([FileSystem(name=os.path.join(root, options.release))
                         for root in options.filesystem])
        Session.commit()
    #
    # Scan Directories.
    #
    last_id = 0
    for fs in Session.query(FileSystem).all():
        if os.path.exists(fs.name):
            try:
                q = Session.query(Directory).filter(Directory.filesystem_id == fs.id).one()
            except NoResultFound:
                last_id = directories(fs, last_id+1)
            except MultipleResultsFound:
                last_id = Session.query(func.max(Directory.id)).scalar()
            else:
                #
                # Apparently there was exactly one directory.
                # This is not as weird as it sounds, because the release
                # directory in the filesystem may be present but empty.
                #
                last_id = q.id
    #
    # Scan files.
    #
    if not options.skip_files:
        for fs in Session.query(FileSystem).all():
            if os.path.exists(fs.name):
                try:
                    q = Session.query(File).join(Directory).filter(Directory.filesystem_id == fs.id).one()
                except NoResultFound:
                    for d in Session.query(Directory).filter(Directory.filesystem_id == fs.id).filter(Directory.nfiles > 0).all():
                        files(d)
                except MultipleResultsFound:
                    #
                    # Already scanned.
                    #
                    pass
                else:
                    #
                    # Apparently there was exactly one file.  OK, fine.
                    #
                    pass
    #
    # Exit gracefully.
    #
    Session.close()
    return 0
