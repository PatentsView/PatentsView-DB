#!/usr/bin/env python


import unittest
import sys

sys.path.append( '.' )
sys.path.append( '../lib/' )

from geocode_setup import get_entry_from_row

class TestSepWrd(unittest.TestCase):

    def test_get_entry_from_row_comma(self):
        assert("foo" == get_entry_from_row("foo,bar", 0))
        assert("bar" == get_entry_from_row("foo,bar", 1))

    def test_get_entry_from_row_pipe(self):
        assert("foo" == get_entry_from_row("foo|bar", 0))
        assert("bar" == get_entry_from_row("foo|bar", 1))

    def test_nosplit(self):
        result = get_entry_from_row("foo bar", 0)
        assert("foo bar" == result)
        result = get_entry_from_row("foo bar", 1)
        assert("" == result)
        # Check out of bounds index, really ought to fail
        assert("" == get_entry_from_row("foo bar", 2))

    def test_seq_neg1(self):
        assert("foo bar" == get_entry_from_row("foo bar", -1))


if __name__ == '__main__':
    unittest.main()
