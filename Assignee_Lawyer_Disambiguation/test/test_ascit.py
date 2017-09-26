#!/usr/bin/env python
# vim: set fileencoding=utf-8 :

# The `ascit` function is used during the cleaning phase as
# an sqlite3 function.

"""
Macos keycodes for common utf characters found in patents.

http://kb.iu.edu/data/anhf.html

Keystroke   Character
Option-e [letter]    acute (e.g., á)
Option-` [letter]    grave (e.g., è)
Option-i [letter]    circumflex (e.g., ô )
Option-u [letter]    umlaut or dieresis (e.g., ï )
Option-n [letter]    tilde (e.g.,  ñ )
Option-q             oe ligature ( œ )
Option-c             cedilla ( ç )
Option-Shift-/ (forward slash)  upside-down question mark ( ¿ )
Option-1 (the number 1)         upside-down exclamation point ( ¡ )
"""

import unittest
import sys
sys.path.append( '.' )
sys.path.append( '../lib/' )
from fwork import ascit
from fwork import remspace

class TestAscit(unittest.TestCase):

    def setUp(self):
        self.foo = 'bar'

    def test_toupper(self):
        assert('FOO' == ascit('FOO'))

    def test_retain_acute_verite(self):
        #print ascit('verité').rstrip('\r\n')
        assert('verité' == ascit('verité'))

    def test_retain_acute(self):
        #print 'ascit é' + ascit('é')
        assert('é' == ascit('é'))

    def test_retain_grave(self):
        assert('è' == ascit('è'))

    def test_retain_circumflex(self):
        assert('ô' == ascit('ô'))

    def test_retain_umlaut(self):
        assert('ü' == ascit('ü'))

    def test_retain_tilde(self):
        assert('ñ' == ascit('ñ'))

    def test_retain_oeligature(self):
        assert('œ' == ascit('œ'))

    def test_retain_cedilla(self):
        assert('ç' == ascit('ç'))

    def test_retain_usdq(self):
        assert('¿' == ascit('¿'))

    def test_int(self):
        assert('1' == ascit('1'))

    def test_float(self):
        # Default strict=True removes periods.
        result = ascit('1.0', strict=False)
        assert('1.0' == result)

    def test_remove_period(self):
        assert('10' == ascit('1.0', strict=True))

    def test_remove_ampersand(self):
        assert('foobar' == ascit('foo&bar', strict=True))

    def test_remove_punctuation(self):
        assert('foobar' == ascit('f+=_oo@b!#$%^&*(){}ar', strict=True))

    def test_remove_space_plus(self):
        assert('' == ascit(' +', strict=True))

    def test_remove_spaces(self):
        #print ascit('foo bar')
        assert('foobar' == ascit('foobar'))

    def test_remove_duplicates(self):
        #print ascit('foo, |||,,,  ,, |,,, bar')
        assert('foo     bar' == ascit('foo, |||,,,  ,, |,,, bar'))

    def test_remove_braces(self):
        #print ascit('{foo bar}', strict=True)
        assert('' == ascit('{foo bar}', strict=True))

    def test_remspace(self):
        assert('foobar' == remspace('foo bar'))

    def test_remove_parentheses(self):
        #print ascit('{foo bar}', strict=True)
        assert('' == ascit('(foo bar)', strict=True))

    def test_remove_period(self):
        assert('hello there' == ascit('hello. there'))
        assert('hello there' == ascit('hello. there',strict =True))

    def test_remove_comma(self):
        assert('hello there' == ascit('hello, there'))
        assert('hello there' == ascit('hello, there',strict =True))

if __name__ == '__main__':
    unittest.main()
