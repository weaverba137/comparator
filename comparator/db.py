# Licensed under a 3-clause BSD style license - see LICENSE.rst.
# -*- coding: utf-8 -*-
"""
comparator.db
=============

Contains SQLAlchemy classes.
"""
import os
from sqlalchemy import (ForeignKey, Column, Integer, String, Float,
                        DateTime, Boolean)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import (scoped_session, sessionmaker, relationship,
                            backref, reconstructor)
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy.types import TypeDecorator


Base = declarative_base()
engine = None
Session = scoped_session(sessionmaker())


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
