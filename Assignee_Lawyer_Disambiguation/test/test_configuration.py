#!/usr/bin/env python

import sys
import unittest

sys.path.append('..')
sys.path.append('../lib')

from start import get_year_list

class Test_Configuration(unittest.TestCase):

    def setUp(self):
        pass

    def test_get_year1(self):
        yearstring = '2013'
        expected = [2013]
        years = get_year_list(yearstring)
        self.assertTrue(expected == years, '\n{0} should be\n{1}'\
                        .format(years, expected))
    
    def test_get_year2(self):
        yearstring = '2010-2013'
        expected = [2010, 2011, 2012, 2013]
        years = get_year_list(yearstring)
        self.assertTrue(expected == years, '\n{0} should be\n{1}'\
                        .format(years, expected))

    def test_get_year3(self):
        yearstring = '2010-2013,2009'
        expected = [2010,2011,2012,2013,2009]
        years = get_year_list(yearstring)
        self.assertTrue(expected == years, '\n{0} should be\n{1}'\
                        .format(years, expected))

    def test_get_year4(self):
        yearstring = '2008,2010-2013,2009'
        expected = [2008,2010,2011,2012,2013,2009]
        years = get_year_list(yearstring)
        self.assertTrue(expected == years, '\n{0} should be\n{1}'\
                        .format(years, expected))

    def test_get_year5(self):
        yearstring = '1975-1978,2000-2002'
        expected = [1975,1976,1977,1978,2000,2001,2002]
        years = get_year_list(yearstring)
        self.assertTrue(expected == years, '\n{0} should be\n{1}'\
                        .format(years, expected))

unittest.main()
