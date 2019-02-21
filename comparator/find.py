# Licensed under a 3-clause BSD style license - see LICENSE.rst.
# -*- coding: utf-8 -*-
"""
comparator.find
===============

Utilities for scanning a filesystem.
"""
import os
from .db import Session, Directory, File


def walk(top):
    """Simplified directory tree generator.

    Adapted from :func:`os.walk`, the yield is similar, but symbolic
    links are *always* treated as files, even if they point to directories,
    and never followed.

    For each directory in the directory tree rooted at `top` (including `top`
    itself, but excluding '.' and '..'), yields a 3-tuple::

        dirpath, dirnames, filenames

    ``dirpath`` is a string, the path to the directory.  ``dirnames`` is a
    list of :class:`os.DirEntry` objects for subdirectories in dirpath
    (excluding '.' and '..'). ``filenames`` is a list of :class:`os.DirEntry`
    objects for the non-directory files in ``dirpath``.
    """
    dirs = []
    nondirs = []

    # We may not have read permission for top, in which case we can't
    # get a list of the files the directory contains.  os.walk
    # always suppressed the exception then, rather than blow up for a
    # minor reason when (say) a thousand readable directories are still
    # left to visit.  That logic is copied here.
    try:
        scandir_it = os.scandir(top)
    except OSError as error:
        return

    with scandir_it:
        while True:
            try:
                try:
                    entry = next(scandir_it)
                except StopIteration:
                    break
            except OSError as error:
                return

            try:
                is_dir = entry.is_dir(follow_symlinks=False)
            except OSError:
                # If is_dir() raises an OSError, consider that the entry is not
                # a directory, same behaviour than os.path.isdir().
                is_dir = False

            if is_dir:
                dirs.append(entry)
            else:
                nondirs.append(entry)

    yield top, dirs, nondirs

    # Recurse into sub-directories
    for d in dirs:
        new_path = os.path.join(top, d.name)
        # Issue #23605: os.path.islink() is used instead of caching
        # entry.is_symlink() result during the loop on os.scandir() because
        # the caller can replace the directory entry during the "yield"
        # above.
        if not os.path.islink(new_path):
            yield from walk(new_path)


def directories(fs, directory_id=1):
    """Find all physical directories on filesystem `fs`.

    Parameters
    ----------
    fs : :class:`FileSystem`
        The filesystem to scan.
    directory_id : :class:`int`, optional
        The id number of the directory corresponding to the root of `fs`.

    Returns
    -------
    :class:`int`
        The id of the last directory found.  If scanning multiple filesystems,
        add one (1) to this number to set the `directory_id` for top of the
        next filesystem.
    """
    parents = {fs.name: directory_id}
    Session.add(Directory(id=directory_id, filesystem_id=fs.id,
                          parent_id=parents[fs.name], name=''))
    Session.commit()
    for dirpath, dirnames, filenames in walk(fs.name):
        p = Session.query(Directory).filter(Directory.id == parents[dirpath]).one()
        p.nfiles = len(filenames)
        for d in dirnames:
            directory_id += 1
            parents[os.path.join(dirpath, d.name)] = directory_id
            Session.add(Directory(id=directory_id, filesystem_id=fs.id,
                                  parent_id=parents[dirpath], name=d.name))
        Session.commit()
    return directory_id


def files(directory):
    """Find files in `directory`; identify symlinks.

    Parameters
    ----------
    directory : :class:`Directory`
        Directory to scan with :func:`os.scandir()`.
    """
    p = directory.fullpath
    with os.scandir(p) as it:
        for entry in it:
            if not entry.is_dir(follow_symlinks=False):
                if entry.is_symlink():
                    d = os.readlink(os.path.join(p, entry.name))
                    f = File(directory_id=directory.id,
                             size=0, mtime=0,
                             name=entry.name,
                             link=True, destination=d)
                else:
                    st = entry.stat(follow_symlinks=False)
                    f = File(directory_id=directory.id,
                             size=st.st_size,
                             mtime=int(st.st_mtime),
                             name=entry.name)
                Session.add(f)
    Session.commit()
