import re
import html

class HTMLEntities:
    """
    Class to abstract out custom html entity unescaping.  We use custom unescape due to the fact that the USPTO patent
    data includes what appears to be custom entities (or, at least, entities not included in the html specification).

    This script relies on the existence of a utf-8 htmlentities file that contains translations for html entities we
    care about.  The htmlentities file is, I believe, based on PHP htmlentities.

    This code originally based on:
        https://github.com/CSSIP-AIR/PatentsView-DB/blob/master/Scripts/Raw_Data_Parsers/uspto_parsers/generic_parser_2002_2004.py
    """

    # Pattern for locating html entities in a string.
    _entity_pattern = re.compile(r'&(\w+?);')

    # Pattern for breaking down lines in the htmlentites file.
    _line_pattern = re.compile(r"\s*\"&(?P<k1>.+?);\"\s+\"&(?P<k2>.+?);\"\s+--\s+\'(?P<v1>.+?)\'\s+\'(?P<v2>.+?)\'\s*$")

    def __init__(self):

        with open('htmlentities', encoding="utf-8") as inputfile:
            lines = inputfile.read().splitlines()

        # Start off with the built in html entity definitions.
        entities = html.entities.entitydefs

        # Now update and add entities in the original entity list with entities that we found in the htmlentites file.
        for line in lines:
            if not line.lstrip().startswith('#'):
                m = self._line_pattern.match(line)
                if m:
                    entities[m.group("k1")] = m.group("v2")
                    entities[m.group("k2")] = m.group("v2")
                else:
                    raise Exception("Invalid format. Was expecting \"key1\" \"key2\" -- 'value1' 'value2' but found: \"%s\"" % line)

        self.entities = entities

    def _unescape(self, match):
        try:
            return self.entities[match.group(1)]
        except:
            return match.group()

    def unescape(self, string):
        return self._entity_pattern.sub(self._unescape, string)
