##############################################################################
# Copyright (c) 2013, AMERICAN INSTITUTES FOR RESEARCH
# All rights reserved.
# Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
# 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
###############################################################################

from collections import namedtuple


class NameFormat:
    # Since enums aren't supported in Python 3.3 (they are in 3.4), we're going to use pseudo-constants
    EXPORTER, AUTHORITY, CITESEERX = range(3)

# After the name is parsed by the functions below, the parts are returned in this tuple.
NameComponents = namedtuple("NameComponents", "Prefix GivenName OtherName FamilyName Suffix NickName")

# The parsers ignore case and periods that appear in prefixes and suffixes.
_Suffixes = ["JR", "SR", "II", "2ND", "III", "3RD", "IV", "4TH", "V", "VI", "PHD", "MD", "RN", "DR", "JD", "MED",
             "MPH", "PHMD", "DRPH", "FAAN", "MD PHD", "MP"]
_Prefixes = ["DR", "MR", "MRS", "MS", "PROF"]
_FamilyNamePrefixes = ["DE", "VON", "VAN", "DER", "LA", "EL", "DI", "DEL", "ST", "SAINT", "MC", "MAC", "VONDER", "DA",
                       "LE", "DEN", "VANDE", "VANDEN", "DU"]


def parse_name(name_format, name_string):
    """
    Parses a string presumed to contain a person's name into multiple parts.

    Parameters:
        nameFormat: a value from the NameFormat class indicating the expected
            format of the nameString parameter.
        nameString: the string containing the name to be parsed.

    Returns: a NameComponent tuple

    """

    components = NameComponents(Prefix=None, FamilyName=None, GivenName=None, OtherName=None,
                                Suffix=None, NickName=None)
    if name_string is not None:
        name_string = name_string.replace("&nbsp;", " ")
        if name_format == NameFormat.EXPORTER:
            components = _parse_name_for_exporter(name_string)
        elif name_format == NameFormat.CITESEERX:
            components = _parse_name_for_citeseerx(name_string)
    return components


def _parse_name_for_citeseerx(name_string):
    """
    Parse a string of CiteSeerX format into constituent parts.

    Outside scripts should not be calling this function directly, but rather they
    should call the parse_name function above.

    The function is specifically written to parse names as they appear in the
    CiteSeerX data. That format is generally:
        [prefix] [givenname] [othername] [familyname] [suffix].

    Returns: a NameComponent tuple

    * Only one prefix and one suffix are supported.
    * If there is only one word, the function returns None for all parts.
    * The parts are expected to be separated by spaces.
    * If there is an unclosed left parentheses, it and everything to the right
        of it is ignored.
    If the last word in the string is a single character, and not a valid
        suffix, it is ignored.

    """

    prefix = None
    given_name = None
    other_name = None
    family_name = None
    suffix = None
    nick_name = None

    name_string = name_string.strip(",").strip()
    # If there is an unclosed left paren, the trash it and everything to the right of it
    if (name_string.count("(") > 0) and (name_string.count(")") == 0):
        name_string = name_string.split("(", 1)[0]
    # If there is a comma, check to see if everything after it is a suffix
    comma_separated = name_string.split(",", 1)
    if len(comma_separated) > 1:
        if _string_contains_only_suffixes(comma_separated[1]):
            suffix = comma_separated[1].strip()
            name_string = comma_separated[0]
    words = name_string.split(" ", 1)
    # Check to see if the first string is a prefix
    if _string_contains_only_prefixes(words[0]):
        prefix = words[0]
        words = words[1].strip().split(" ", 1)  # Strip off the prefix and continue on
    # If there is only one word, then don't return any results
    if len(words) == 1:
        prefix = None
    else:
        given_name = words[0]
        words = words[1].strip().rsplit(" ", 1)
        if len(words) == 1:
            family_name = words[0]
        else:
            if _string_contains_only_suffixes(words[1]):
                suffix = words[1]
                words = words[0].strip().rsplit(" ", 1)
            elif len(words[1]) == 1:
                # If the last piece is only one character, ignore it
                words = words[0].strip().rsplit(" ", 1)
                if (len(words) > 1) and _string_contains_only_suffixes(words[1]):
                    suffix = words[1]
                    words = words[0].strip().rsplit(" ", 1)
            if len(words) == 1:
                family_name = words[0]
            else:
                # If the last piece is only one character, ignore it
                if len(words[1]) == 1:
                    words = words[0].strip().rsplit(" ", 1)
                if len(words) == 1:
                    family_name = words[0]
                else:
                    other_name = words[0]
                    family_name = words[1]

    if prefix is not None:
        prefix = prefix.strip().strip(",")
    if given_name is not None:
        given_name = given_name.strip().strip(",")
    if other_name is not None:
        other_name = other_name.strip().strip(",")
    if family_name is not None:
        family_name = family_name.strip().strip(",")
    if suffix is not None:
        suffix = suffix.strip().strip(",")

    name_components = NameComponents(Prefix=prefix, GivenName=given_name, OtherName=other_name,
                                     FamilyName=family_name, Suffix=suffix, NickName=nick_name)
    name_components = _parse_family_name_prefixes(name_components)

    return name_components


def _parse_name_for_exporter(name_string):
    """
    Parses a string of ExPORTER format into constituent parts.

    Outside scripts should not be calling this function directly, but rather they
    should call the parse_name function above.

    The	function is specifically written to parse names as they appear in the NIH
    ExPORTER data. That format can be any of the following:
        [familyname], [givenname] [othername]
        [familyname], [suffix], [givenname] [othername]
        [familyname] [suffix], [givenname] [othername]
        [familyname], [givenname]
        [familyname], [suffix], [givenname]
        [familyname] [suffix], [givenname]
        [familyname], [givenname], [suffix]
        [familyname], [givenname] [othername], [suffix]

    Returns: a NameComponent tuple

    * Multiple suffixes are supported and can be separated by either spaces or
        commas.
    * If there is not at least one comma, all parts are returned as None.
    * [givenname] will always be just one word; [othername] and [familyname] can
        be multiple.

    """

    prefix = None
    given_name = None
    other_name = None
    family_name = None
    suffix = None
    nick_name = None

    name_string = name_string.strip(",").strip()  # get rid of leading and trailing commas and whitespace
    comma_separated = name_string.split(",", 1)
    remainder = ""

    # If there are 3 commas, then there are may be multiple suffixes. Those will either appear at the end
    # or after the family name.

    # If there are 3 or more commas, check everything between the first and last comma to see
    # if it contains only suffixes
    if (name_string.count(",") >= 3) and (_string_contains_only_suffixes(comma_separated[1].rsplit(",", 1)[0].strip())):
        suffix = comma_separated[1].rsplit(",", 1)[0]  # everything between the first and last comma
        family_name = comma_separated[0]  # everything before the first comma
        remainder = comma_separated[1].rsplit(",", 1)[1].strip()  # everything after the last comma
        words = remainder.split(" ", 1)
        given_name = words[0].strip(",")  # up to the first blank in remainder
        if len(words) > 1:  # if there is a blank...
            other_name = words[1]  # everything after the first blank in remainder
    else:
        comma_separated = name_string.split(",", 2)  # Only break into at most 3 parts by the first two commas
        # If there aren't any commas then this is the wrong function to call
        if len(comma_separated) > 1:
            # If there is only one comma, then the part before the comma contains familyname and optionally suffix,
            # and the part after the comma contains givenname and optionally othername
            if len(comma_separated) == 2:
                family_name_components = _parse_family_name(comma_separated[0])
                family_name = family_name_components.FamilyName
                suffix = family_name_components.Suffix
                remainder = comma_separated[1].strip()
            # If there are two commas (i.e. 3 parts)
            elif len(comma_separated) == 3:
                # It's possible that the third part is actually a suffix, so check that first
                if _string_contains_only_suffixes(comma_separated[2]):
                    suffix = comma_separated[2]
                    family_name = comma_separated[0]
                    remainder = comma_separated[1].strip()
                else:
                    family_name = comma_separated[0]
                    suffix = comma_separated[1]
                    remainder = comma_separated[2].strip()
            words = remainder.split(" ", 1)
            given_name = words[0].strip(",")
            if len(words) > 1:
                other_name = words[1]
    if family_name is not None:
        family_name = family_name.strip()
    if given_name is not None:
        given_name = given_name.strip()
    if other_name is not None:
        other_name = other_name.strip()
    if suffix is not None:
        suffix = suffix.strip()

    name_components = NameComponents(Prefix=prefix, GivenName=given_name, OtherName=other_name,
                                     FamilyName=family_name, Suffix=suffix, NickName=nick_name)
    name_components = _parse_family_name_prefixes(name_components)

    return name_components


def _parse_family_name(name_string):
    """Parses a string that contains a family name and perhaps a suffix."""
    family_name = name_string
    suffix = None
    #rsplit is important here. If there are multiple blanks, everything before the last one is considered familyname
    words = name_string.rsplit(" ", 1)
    if len(words) > 1:
        if any(x == words[1].upper().strip() for x in _Suffixes):
            family_name = words[0]
            suffix = words[1]
    if family_name is not None:
        family_name = family_name.strip()
    if suffix is not None:
        suffix = suffix.strip()
    return NameComponents(Prefix=None, GivenName=None, OtherName=None,
                          FamilyName=family_name, Suffix=suffix, NickName=None)


def _parse_family_name_prefixes(name_components):
    """Checks the OtherName for family name prefixes and moves to the front of the family name."""
    other_name = name_components.OtherName
    family_name = name_components.FamilyName
    if other_name is not None:
        words = other_name.split(" ")
        for word in reversed(words):
            if any(x == word.upper().strip() for x in _FamilyNamePrefixes):
                other_name = other_name.replace(word, "")
                family_name = word + " " + family_name
            else:
                break
        other_name = other_name.strip()

    if other_name == "":
        other_name = None

    return NameComponents(Prefix=name_components.Prefix, GivenName=name_components.GivenName, OtherName=other_name,
                          FamilyName=family_name, Suffix=name_components.Suffix, NickName=name_components.NickName)


def _string_contains_only_suffixes(suffix_string):
    """ Determines if a string contains only suffixes, either comma or space separated."""
    rc = True
    suffix_string = suffix_string.strip().replace(".", "")
    comma_separated = suffix_string.split(",")
    if len(comma_separated) > 1:
        for (part) in comma_separated:
            if all(x != part.upper().strip() for x in _Suffixes):
                rc = False
    else:
        space_separated = suffix_string.split(" ")
        for (part) in space_separated:
            if all(x != part.upper().strip() for x in _Suffixes):
                rc = False
    return rc


def _string_contains_only_prefixes(prefix_string):
    """Determines if a string contains only prefixes, either comma or space separated."""
    rc = True
    prefix_string = prefix_string.strip().replace(".", "")
    comma_separated = prefix_string.split(",")
    if len(comma_separated) > 1:
        for (part) in comma_separated:
            if all(x != part.upper().strip() for x in _Prefixes):
                rc = False
    else:
        space_separated = prefix_string.split(" ")
        for (part) in space_separated:
            if all(x != part.upper().strip() for x in _Prefixes):
                rc = False
    return rc


if __name__ == "__main__":
    # Test cases
    print("A\t", parse_name(NameFormat.EXPORTER, "FamilyName, GivenName OtherName")
                 == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName", OtherName="OtherName",
                                   Suffix=None, NickName=None))
    print("B\t", parse_name(NameFormat.EXPORTER, "FamilyName, GivenName Other Name")
                 == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName", OtherName="Other Name",
                                   Suffix=None, NickName=None))
    print("C\t", parse_name(NameFormat.EXPORTER, "Family Name, GivenName OtherName")
                 == NameComponents(Prefix=None, FamilyName="Family Name", GivenName="GivenName", OtherName="OtherName",
                                   Suffix=None, NickName=None))
    print("D\t", parse_name(NameFormat.EXPORTER, "Family VName, GivenName OtherName")
                 == NameComponents(Prefix=None, FamilyName="Family VName", GivenName="GivenName", OtherName="OtherName",
                                   Suffix=None, NickName=None))
    print("E\t", parse_name(NameFormat.EXPORTER, "Family Name, GivenName Other Name")
                 == NameComponents(Prefix=None, FamilyName="Family Name", GivenName="GivenName", OtherName="Other Name",
                                   Suffix=None, NickName=None))
    print("F\t", parse_name(NameFormat.EXPORTER, "FamilyName, GivenName Double Other Name")
                 == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName",
                                   OtherName="Double Other Name", Suffix=None, NickName=None))
    print("G\t", parse_name(NameFormat.EXPORTER, "FamilyName JR, GivenName OtherName")
                 == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName", OtherName="OtherName",
                                   Suffix="JR", NickName=None))
    print("H\t", parse_name(NameFormat.EXPORTER, "FamilyName, III, GivenName OtherName")
                 == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName", OtherName="OtherName",
                                   Suffix="III", NickName=None))
    print("I\t", parse_name(NameFormat.EXPORTER, "Family Name IV, GivenName OtherName")
                 == NameComponents(Prefix=None, FamilyName="Family Name", GivenName="GivenName", OtherName="OtherName",
                                   Suffix="IV", NickName=None))
    print("J\t", parse_name(NameFormat.EXPORTER, "Family Name, PhD, GivenName OtherName")
                 == NameComponents(Prefix=None, FamilyName="Family Name", GivenName="GivenName", OtherName="OtherName",
                                   Suffix="PhD", NickName=None))
    print("K\t", parse_name(NameFormat.EXPORTER, "FamilyName, GivenName OtherName,")
                 == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName", OtherName="OtherName",
                                   Suffix=None, NickName=None))
    print("L\t", parse_name(NameFormat.EXPORTER, "FamilyName, GivenName, jr.")
                 == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName", OtherName=None,
                                   Suffix="jr.", NickName=None))
    print("M\t", parse_name(NameFormat.EXPORTER, "Family Name, SR, GivenName OtherName")
                 == NameComponents(Prefix=None, FamilyName="Family Name", GivenName="GivenName", OtherName="OtherName",
                                   Suffix="SR", NickName=None))
    print("N\t", parse_name(NameFormat.EXPORTER, "Family Name, GivenName OtherName, MD")
                 == NameComponents(Prefix=None, FamilyName="Family Name", GivenName="GivenName", OtherName="OtherName",
                                   Suffix="MD", NickName=None))
    print("O\t", parse_name(NameFormat.EXPORTER, "Family Name, GivenName OtherName, M.D., PhD")
                 == NameComponents(Prefix=None, FamilyName="Family Name", GivenName="GivenName", OtherName="OtherName",
                                   Suffix="M.D., PhD", NickName=None))
    print("P\t", parse_name(NameFormat.EXPORTER, "Family Name, GivenName OtherName, MD Ph.D")
                 == NameComponents(Prefix=None, FamilyName="Family Name", GivenName="GivenName", OtherName="OtherName",
                                   Suffix="MD Ph.D", NickName=None))
    print("Q\t", parse_name(NameFormat.EXPORTER, "Family Name, MD, Ph.D., GivenName OtherName")
                 == NameComponents(Prefix=None, FamilyName="Family Name", GivenName="GivenName", OtherName="OtherName",
                                   Suffix="MD, Ph.D.", NickName=None))
    print("R\t", parse_name(NameFormat.EXPORTER, "Family Name, MD, Ph.D, GivenName Other Name")
                 == NameComponents(Prefix=None, FamilyName="Family Name", GivenName="GivenName", OtherName="Other Name",
                                   Suffix="MD, Ph.D", NickName=None))
    print("S\t", parse_name(NameFormat.EXPORTER, "Family Name, GivenName Other Name, MD Ph.D")
                 == NameComponents(Prefix=None, FamilyName="Family Name", GivenName="GivenName", OtherName="Other Name",
                                   Suffix="MD Ph.D", NickName=None))
    print("T\t", parse_name(NameFormat.EXPORTER, "Family Name, GivenName Other Name, III, MD, Ph.D")
                 == NameComponents(Prefix=None, FamilyName="Family Name", GivenName="GivenName", OtherName="Other Name",
                                   Suffix="III, MD, Ph.D", NickName=None))
    print("U\t", parse_name(NameFormat.EXPORTER, "Family Name, Sr., MD, Ph.D., GivenName Other Name")
                 == NameComponents(Prefix=None, FamilyName="Family Name", GivenName="GivenName", OtherName="Other Name",
                                   Suffix="Sr., MD, Ph.D.", NickName=None))
    print("V\t", parse_name(NameFormat.EXPORTER, "FamilyName, GivenName")
                 == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName", OtherName=None,
                                   Suffix=None, NickName=None))

    print("AA\t", parse_name(NameFormat.CITESEERX, "GivenName FamilyName")
                  == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName", OtherName=None,
                                    Suffix=None, NickName=None))
    print("BB\t", parse_name(NameFormat.CITESEERX, "GivenName OtherName FamilyName")
                  == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName", OtherName="OtherName",
                                    Suffix=None, NickName=None))
    print("CC\t", parse_name(NameFormat.CITESEERX, "Dr. GivenName OtherName FamilyName")
                  == NameComponents(Prefix="Dr.", FamilyName="FamilyName", GivenName="GivenName", OtherName="OtherName",
                                    Suffix=None, NickName=None))
    print("DD\t", parse_name(NameFormat.CITESEERX, "GivenName Multiple Other Name FamilyName")
                  == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName",
                                    OtherName="Multiple Other Name", Suffix=None, NickName=None))
    print("EE\t", parse_name(NameFormat.CITESEERX, "GivenName OtherName FamilyName Jr")
                  == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName", OtherName="OtherName",
                                    Suffix="Jr", NickName=None))
    print("FF\t", parse_name(NameFormat.CITESEERX, "Dr GivenName OtherName FamilyName III")
                  == NameComponents(Prefix="Dr", FamilyName="FamilyName", GivenName="GivenName", OtherName="OtherName",
                                    Suffix="III", NickName=None))
    print("GG\t", parse_name(NameFormat.CITESEERX, "GivenName OtherName FamilyName&nbsp;III")
                  == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName", OtherName="OtherName",
                                    Suffix="III", NickName=None))
    print("HH\t", parse_name(NameFormat.CITESEERX, "GivenName OtherName FamilyName (III")
                  == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName", OtherName="OtherName",
                                    Suffix=None, NickName=None))
    print("II\t", parse_name(NameFormat.CITESEERX, "WhoKnows")
                  == NameComponents(Prefix=None, FamilyName=None, GivenName=None, OtherName=None, Suffix=None,
                                    NickName=None))
    print("JJ\t", parse_name(NameFormat.CITESEERX, "Dr. WhoKnows")
                  == NameComponents(Prefix=None, FamilyName=None, GivenName=None, OtherName=None, Suffix=None,
                                    NickName=None))
    print("KK\t", parse_name(NameFormat.CITESEERX, "GivenName FamilyName V")
                  == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName", OtherName=None,
                                    Suffix="V", NickName=None))
    print("LL\t", parse_name(NameFormat.CITESEERX, "GivenName(s) FamilyName")
                  == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName(s)", OtherName=None,
                                    Suffix=None, NickName=None))
    print("MM\t", parse_name(NameFormat.CITESEERX, "GivenName FamilyName Q")
                  == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName", OtherName=None,
                                    Suffix=None, NickName=None))
    print("NN\t", parse_name(NameFormat.CITESEERX, "GivenName Other Name FamilyName Q")
                  == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName", OtherName="Other Name",
                                    Suffix=None, NickName=None))
    print("OO\t", parse_name(NameFormat.CITESEERX, " I.V. Basawa")
                  == NameComponents(Prefix=None, FamilyName="Basawa", GivenName="I.V.", OtherName=None, Suffix=None,
                                    NickName=None))
    print("PP\t", parse_name(NameFormat.CITESEERX, "GivenName M. FamilyName, III")
                  == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName", OtherName="M.",
                                    Suffix="III", NickName=None))
    print("QQ\t", parse_name(NameFormat.CITESEERX, "GivenName M. FamilyName, III PhD M.D.")
              == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName", OtherName="M.",
                                Suffix="III PhD M.D.", NickName=None))
    print("RR\t", parse_name(NameFormat.CITESEERX, "GivenName M. von FamilyName, III")
              == NameComponents(Prefix=None, FamilyName="von FamilyName", GivenName="GivenName", OtherName="M.",
                                Suffix="III", NickName=None))
    print("SS\t", parse_name(NameFormat.CITESEERX, "GivenName M. van der FamilyName, III")
          == NameComponents(Prefix=None, FamilyName="van der FamilyName", GivenName="GivenName", OtherName="M.",
                            Suffix="III", NickName=None))
    print("TT\t", parse_name(NameFormat.CITESEERX, "GivenName Ivan von FamilyName, III")
          == NameComponents(Prefix=None, FamilyName="von FamilyName", GivenName="GivenName", OtherName="Ivan",
                            Suffix="III", NickName=None))
    print("UU\t", parse_name(NameFormat.CITESEERX, "GivenName Ivan de la FamilyName, III")
          == NameComponents(Prefix=None, FamilyName="de la FamilyName", GivenName="GivenName", OtherName="Ivan",
                            Suffix="III", NickName=None))
    print("VV\t", parse_name(NameFormat.CITESEERX, "GivenName FamilyName Jr C")
          == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName", OtherName=None,
                            Suffix="Jr", NickName=None))
    print("WW\t", parse_name(NameFormat.CITESEERX, "GivenName de OtherName FamilyName Jr")
          == NameComponents(Prefix=None, FamilyName="FamilyName", GivenName="GivenName", OtherName="de OtherName",
                            Suffix="Jr", NickName=None))
