# Licensed under a 3-clause BSD style license - see LICENSE.rst.
# -*- coding: utf-8 -*-
#
# $Id: metadata.py 4596 2018-09-12 21:06:37Z baweaver $
#
"""Obtain filesystem metadata necessary for comparing the same data set
at different locations.
"""
import sqlite3
import os
from sqlalchemy import (create_engine, func, ForeignKey, Column,
                        Integer, String, Float, DateTime, Boolean)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import (scoped_session, sessionmaker, relationship,
                            backref, reconstructor)
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.types import TypeDecorator


Base = declarative_base()
engine = None
dbSession = scoped_session(sessionmaker())


_missing = object()   # sentinel object for missing values


class cached_hybrid_property(hybrid_property):
    def __get__(self, instance, owner):
        if instance is None:
            # getting the property for the class
            return self.expr(owner)
        else:
            # getting the property for an instance
            name = self.fget.__name__
            value = instance.__dict__.get(name, _missing)
            if value is _missing:
                value = self.fget(instance)
                instance.__dict__[name] = value
            return value


class FileSystem(Base):
    """Representation of a filesystem.
    """
    __tablename__ = 'filesystem'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)

    def __repr__(self):
        return ("<FileSystem(id={0.id:d}, name='{0.name}')>").format(self)


class Directory(Base):
    """Representation of a directory.

    Notes
    -----
    See https://docs.sqlalchemy.org/en/latest/_modules/examples/adjacency_list/adjacency_list.html
    """
    __tablename__ = 'directory'

    id = Column(Integer, primary_key=True)
    filesystem_id = Column(Integer, ForeignKey('filesystem.id'), nullable=False)
    parent_id = Column(Integer, ForeignKey(id), nullable=False, index=True)
    nfiles = Column(Integer, nullable=False, default=0)
    name = Column(String, nullable=False)

    filesystem = relationship('FileSystem', back_populates='directories')

    children = relationship("Directory",
                            cascade="all, delete-orphan",  # cascade deletions
                            # many to one + adjacency list - remote_side is
                            # required to reference the 'remote' column
                            # in the join condition.
                            backref=backref("parent", remote_side=id),
                            # children will be represented as a dictionary
                            # on the "name" attribute.
                            collection_class=attribute_mapped_collection("name"))

    def __repr__(self):
        return ("<Directory(id={0.id:d}, " +
                "filesystem_id={0.filesystem_id:d}, " +
                "parent_id={0.parent_id:d}, " +
                "nfiles={0.nfiles:d}, " +
                "name='{0.name}')>").format(self)

    @cached_hybrid_property
    def fullpath(self):
        """Full system directory path.
        """
        if not self.name:
            return self.filesystem.name
        fp = [self.name]
        parent = self.parent
        while parent.name:
            fp.insert(0, parent.name)
            parent = parent.parent
        fp.insert(0, self.filesystem.name)
        return os.path.join(*fp)


FileSystem.directories = relationship('Directory', back_populates='filesystem')


class File(Base):
    """Representation of an ordinary file or a symlink.
    """
    __tablename__ = 'file'

    id = Column(Integer, primary_key=True)
    directory_id = Column(Integer, ForeignKey('directory.id'), nullable=False)
    # mode = Column(String(10), nullable=False)
    # uid = Column(Integer, ForeignKey('users.uid'), nullable=False)
    # gid = Column(Integer, ForeignKey('groups.gid'), nullable=False)
    size = Column(Integer, nullable=False)
    # mtime = Column(AwareDateTime(timezone=True), nullable=False)
    mtime = Column(Integer, nullable=False)
    name = Column(String, nullable=False)
    link = Column(Boolean, nullable=False, default=False)
    destination = Column(String, nullable=False, default='')

    directory = relationship('Directory', back_populates='files')

    def __repr__(self):
        return ("<File(id={0.id:d}, " +
                "directory_id={0.directory_id:d}, " +
                # "mode='{0.mode}', " +
                # "uid={0.uid:d}, " +
                # "gid={0.gid:d}, " +
                "size={0.size:d}, " +
                # "mtime='{0.mtime}', " +
                "mtime={0.mtime:d}, " +
                "name='{0.name}', " +
                "link={0.link}" +
                "destination='{0.destination}')>").format(self)

    @property
    def path(self):
        """Full system path to the file.
        """
        return os.path.join(self.directory.fullpath, self.name)

    @property
    def realpath(self):
        """Full system path to the target of a symlink, if the file is a
        symlink.
        """
        if self.link:
            return os.path.realpath(self.path)
        else:
            return self.path


Directory.files = relationship('File', order_by=File.name,
                               back_populates='directory')


def fast_walk(top):
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
            yield from fast_walk(new_path)


def find_directories(fs, directory_id=1):
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
    dbSession.add(Directory(id=directory_id, filesystem_id=fs.id,
                            parent_id=parents[fs.name], name=''))
    dbSession.commit()
    for dirpath, dirnames, filenames in fast_walk(fs.name):
        p = dbSession.query(Directory).filter(Directory.id == parents[dirpath]).one()
        p.nfiles = len(filenames)
        for d in dirnames:
            directory_id += 1
            parents[os.path.join(dirpath, d.name)] = directory_id
            dbSession.add(Directory(id=directory_id, filesystem_id=fs.id,
                                    parent_id=parents[dirpath], name=d.name))
        dbSession.commit()
    return directory_id


def find_files(directory):
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
                dbSession.add(f)
    dbSession.commit()


def get_options():
    """Parse the command-line options.

    Returns
    -------
    The parsed options.
    """
    from sys import argv
    from argparse import ArgumentParser
    xct = os.path.basename(argv[0])
    prsr = ArgumentParser(description=__doc__, prog=xct)
    prsr.add_argument('-f', '--filesystem', action='append',
                      dest='filesystem', metavar="DIR",
                      help='Filesysem(s) to examine.')
    prsr.add_argument('-F', '--skip-files', action='store_true',
                      dest='skip_files', help='Skip the file scan stage.')
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
    global engine
    #
    # Arguments
    #
    options = get_options()
    #
    # Initialize database.
    #
    if options.overwrite and os.path.exists(options.database):
        os.remove(options.database)
    engine = create_engine('sqlite:///'+options.database, echo=options.verbose)
    dbSession.remove()
    dbSession.configure(bind=engine, autocommit=False,
                        autoflush=True, expire_on_commit=True)
    Base.metadata.create_all(engine)
    #
    # Add filesystems.
    #
    try:
        q = dbSession.query(FileSystem).one()
    except NoResultFound:
        dbSession.add_all([FileSystem(name=os.path.join(root, options.release))
                           for root in options.filesystem])
        dbSession.commit()
    #
    # Scan Directories.
    #
    last_id = 0
    for fs in dbSession.query(FileSystem).all():
        if os.path.exists(fs.name):
            try:
                q = dbSession.query(Directory).filter(Directory.filesystem_id == fs.id).one()
            except NoResultFound:
                last_id = find_directories(fs, last_id+1)
            except MultipleResultsFound:
                last_id = dbSession.query(func.max(Directory.id)).scalar()
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
        for fs in dbSession.query(FileSystem).all():
            if os.path.exists(fs.name):
                try:
                    q = dbSession.query(File).join(Directory).filter(Directory.filesystem_id == fs.id).one()
                except NoResultFound:
                    for d in dbSession.query(Directory).filter(Directory.filesystem_id == fs.id).filter(Directory.nfiles > 0).all():
                        find_files(d)
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
    dbSession.close()
    return 0


if __name__ == '__main__':
    from sys import exit
    exit(main())
