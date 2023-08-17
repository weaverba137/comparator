# Licensed under a 3-clause BSD style license - see LICENSE.rst
# -*- coding: utf-8 -*-
"""
comparator.test.test_top_level
==============================

Test top-level comparator functions.
"""
import re
from .. import __version__ as theVersion


def test_version_string():
    """Ensure the version conforms to PEP386/PEP440.
    """
    versionre = re.compile(r'([0-9]+!)?([0-9]+)(\.[0-9]+)*((a|b|rc|\.post|\.dev)[0-9]+)?')
    assert versionre.match(theVersion) is not None
