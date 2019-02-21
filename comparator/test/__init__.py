# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
comparator.test
===============

Used to initialize the unit test framework via ``python setup.py test``.
"""
import unittest
from os.path import dirname


def comparator_test_suite():
    """Returns unittest.TestSuite of comparator tests.

    This is factored out separately from runtests() so that it can be used by
    ``python setup.py test``.
    """
    py_dir = dirname(dirname(__file__))
    return unittest.defaultTestLoader.discover(py_dir,
                                               top_level_dir=dirname(py_dir))


def runtests():
    """Run all tests in comparator.test.test_*.
    """
    # Load all TestCase classes from comparator/test/test_*.py
    tests = comparator_test_suite()
    # Run them
    unittest.TextTestRunner(verbosity=2).run(tests)
