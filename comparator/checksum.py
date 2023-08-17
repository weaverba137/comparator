# Licensed under a 3-clause BSD style license - see LICENSE.rst.
# -*- coding: utf-8 -*-
"""
comparator.checksum
===================

Obtain or sync checksum files from an authoritative source.
"""
from argparse import ArgumentParser
import os
import subprocess as sp
import sys


def _rsync(s, d, checksum='sha256sum', test=False):
    """Set up rsync command.

    Parameters
    ----------
    s : :class:`str`
        Source directory.
    d : :class:`str`
        Destination directory.
    checksum : :class:`str`, optional
        Search for checksum files of this type, default 'sha256sum'.
    test : :class:`bool`, optional
        If ``True``, add ``--dry-run`` to the command.

    Returns
    -------
    :class:`list`
        A list suitable for passing to :class:`subprocess.Popen`.
    """
    c = ['/bin/rsync', '--verbose', '--recursive',
         '--checksum', '--times', '--omit-dir-times',
         f'--include *.{checksum}', '--exclude *']
    if s.endswith('/'):
        src = s
    else:
        src = s + '/'
    if d.endswith('/'):
        dst = d
    else:
        dst = d + '/'
    c += [src, dst]
    if test:
        c.insert(1, '--dry-run')
    return c


def _options():
    """Parse the command-line options.

    Returns
    -------
    The parsed options.
    """
    xct = os.path.basename(sys.argv[0])
    desc = "Obtain or sync checksum files from an authoritative source."
    prsr = ArgumentParser(description=desc, prog=xct)
    prsr.add_argument('-c', '--checksum', action='store', dest='checksum',
                      metavar='CHECKSUM', default='sha256sum',
                      help="Checksum files have type CHECKSUM (default '%(default)s').")
    # prsr.add_argument('-f', '--filesystem', action='append',
    #                   dest='filesystem', metavar="DIR",
    #                   help='FileSystem(s) to examine.')
    # prsr.add_argument('-F', '--skip-files', action='store_true',
    #                   dest='skip_files', help='Skip the file search stage.')
    # prsr.add_argument('-l', '--log-dir', dest='logging', metavar='DIR',
    #                   default=os.path.join(os.environ['HOME'], 'Documents', 'Logs'),
    #                   help='Log files in DIR (default %(default)s).')
    # prsr.add_argument('-R', '--root', dest='root', metavar='DIR',
    #                   default='/global/project/projectdirs',
    #                   help='Path containing metadata directory (default %(default)s).')
    # prsr.add_argument('-s', '--sql', dest='sql', action='store_true',
    #                   help='Output SQL statements instead of loading database.')
    # prsr.add_argument('-o', '--overwrite', action='store_true',
    #                   dest='overwrite', help='Overwrite any existing database.')
    prsr.add_argument('-t', '--test', action='store_true', dest='test',
                      help='Test mode; do not make any changes.')
    prsr.add_argument('-v', '--verbose', action='store_true', dest='verbose',
                      help='Log extra debugging information.')
    prsr.add_argument('source', metavar='SRC',
                      help='Use SRC as the authoritative source.')
    prsr.add_argument('destination', metavar='DST',
                      help='Sync with DST.')
    return prsr.parse_args()


def main():
    """Entry point for command-line scripts.

    Returns
    -------
    :class:`int`
        An integer suitable for passing to :func:`sys.exit`.
    """
    options = _options()
    r = _rsync(options.source, options.destination,
               checksum=options.checksum, test=options.test)
    print(r)
    return 0
