import unittest
import os
import sys
import shutil
sys.path.append('../lib/')
sys.path.append(os.path.dirname(os.path.realpath(__file__)))
import alchemy
from alchemy.schema import *


class TestAlchemy(unittest.TestCase):

    def setUp(self):
        # this basically resets our testing database
        path = config.get('sqlite').get('path')
        shutil.copyfile('{0}/alchemy.raw'.format(path), '{0}/test.db'.format(path))

    def tearDown(self):
        # we keep this to tidy up our database if it fails
        session.close()

    def test_raw_clean(self):
        # add a Clean record to mark something against
        asg0 = session.query(RawAssignee).limit(10)
        asg1 = session.query(RawAssignee).limit(10).offset(10)

        alchemy.match(asg0, session)
        alchemy.match(asg1, session)
        alchemy.match([asg0[0], asg1[0].assignee], session)

    def test_match_all(self):
        alchemy.match(session.query(RawAssignee), session)

    def test_set_default(self):
        # create two items
        loc = session.query(RawLocation)
        alchemy.match(loc, session)

        alchemy.match(loc[0], session, {"city": u"Frisco", "state": u"Cali", "country": u"US", "longitude": 10.0, "latitude": 10.0})
        self.assertEqual("Frisco, Cali, US", loc[0].location.address)

        alchemy.match(loc[0], session, keepexisting=True)
        self.assertEqual("Frisco, Cali, US", loc[0].location.address)
        self.assertEqual(10.0, loc[0].location.latitude)
        self.assertEqual(10.0, loc[0].location.longitude)

        alchemy.match(loc[0], session)
        self.assertEqual("Hong Kong, OH, US", loc[0].location.address)
        self.assertEqual(10.0, loc[0].location.latitude)
        self.assertEqual(10.0, loc[0].location.longitude)

        alchemy.match(loc[0], session, {"city": u"Frisco"}, keepexisting=True)
        self.assertEqual("Frisco, OH, US", loc[0].location.address)
        self.assertEqual(10.0, loc[0].location.latitude)
        self.assertEqual(10.0, loc[0].location.longitude)

    def test_unmatch_asgloc(self):
        loc = session.query(RawLocation).limit(20)
        asg = session.query(RawAssignee).limit(20)
        alchemy.match(asg, session)
        alchemy.match(loc[0:5], session)
        alchemy.match(loc[5:10], session)
        alchemy.match(loc[10:15], session)
        alchemy.match(loc[15:20], session)

        clean = asg[0].assignee
        alchemy.unmatch(asg[0], session)
        self.assertEqual(None, asg[0].assignee)
        self.assertEqual(19, len(clean.rawassignees))
        self.assertEqual(19, len(clean.patents))

        self.assertEqual(4, session.query(Location).count())
        self.assertEqual(4, session.query(locationassignee).count())

        clean = loc[0].location
        self.assertEqual(5, len(clean.rawlocations))
        alchemy.unmatch(loc[0], session)
        self.assertEqual(4, len(clean.rawlocations))
        alchemy.unmatch(loc[1], session)
        self.assertEqual(3, len(clean.rawlocations))
        alchemy.unmatch(loc[2:5], session)
        self.assertEqual(None, loc[0].location)
        self.assertEqual(3, session.query(Location).count())
        self.assertEqual(3, session.query(locationassignee).count())

        alchemy.unmatch(loc[5].location, session)
        self.assertEqual(2, session.query(Location).count())
        self.assertEqual(2, session.query(locationassignee).count())

        alchemy.unmatch(asg[3:20], session)
        alchemy.unmatch(loc[10].location, session)
        self.assertEqual(1, session.query(Location).count())
        self.assertEqual(0, session.query(locationassignee).count())

    def test_unmatch_invloc(self):
        loc = session.query(RawLocation).limit(20)
        inv = session.query(RawInventor).limit(20)
        alchemy.match(inv, session)
        alchemy.match(loc[0:5], session)
        alchemy.match(loc[5:10], session)
        alchemy.match(loc[10:15], session)
        alchemy.match(loc[15:20], session)

        clean = inv[0].inventor
        alchemy.unmatch(inv[0], session)
        self.assertEqual(None, inv[0].inventor)
        self.assertEqual(19, len(clean.rawinventors))
        self.assertEqual(10, len(clean.patents))

        self.assertEqual(4, session.query(Location).count())
        self.assertEqual(4, session.query(locationinventor).count())

        clean = loc[0].location
        self.assertEqual(5, len(clean.rawlocations))
        alchemy.unmatch(loc[0], session)
        self.assertEqual(4, len(clean.rawlocations))
        alchemy.unmatch(loc[1], session)
        self.assertEqual(3, len(clean.rawlocations))
        alchemy.unmatch(loc[2:5], session)
        self.assertEqual(None, loc[0].location)
        self.assertEqual(3, session.query(Location).count())
        self.assertEqual(3, session.query(locationinventor).count())

        clean = inv[5].inventor
        alchemy.unmatch(inv[1], session)
        self.assertEqual(None, inv[1].inventor)
        self.assertEqual(18, len(clean.rawinventors))
        # this patent is repeated
        self.assertEqual(10, len(clean.patents))

        alchemy.unmatch(inv[2], session)
        self.assertEqual(None, inv[2].inventor)
        self.assertEqual(17, len(clean.rawinventors))
        self.assertEqual(9, len(clean.patents))

        alchemy.unmatch(loc[5].location, session)
        self.assertEqual(2, session.query(Location).count())
        self.assertEqual(2, session.query(locationinventor).count())

        alchemy.unmatch(inv[3:20], session)
        alchemy.unmatch(loc[10].location, session)
        self.assertEqual(1, session.query(Location).count())
        self.assertEqual(0, session.query(locationinventor).count())

    def test_unmatch_lawyer(self):
        law = session.query(RawLawyer).limit(20)
        alchemy.match(law, session)

        alchemy.unmatch(law[0], session)
        self.assertEqual(None, law[0].lawyer)
        self.assertEqual(19, len(law[1].lawyer.rawlawyers))
        self.assertEqual(14, len(law[1].lawyer.patents))

    def test_assigneematch(self):
        # blindly assume first 10 are the same
        asg0 = session.query(RawAssignee).limit(10)
        asg1 = session.query(RawAssignee).limit(10).offset(10)
        asgs = session.query(Assignee)

        alchemy.match(asg0, session)
        alchemy.match(asg1, session)

        # create two items
        self.assertEqual(10, len(asg0[0].assignee.rawassignees))
        self.assertEqual(10, len(asg1[0].assignee.rawassignees))
        self.assertEqual(10, len(asg0[0].assignee.patents))
        self.assertEqual(2, asgs.count())
        self.assertEqual("CAFEPRESS.COM", asg0[0].assignee.organization)

        # merge the assignees together
        alchemy.match([asg0[0], asg1[0]], session)

        self.assertEqual(20, len(asg0[0].assignee.rawassignees))
        self.assertEqual(20, len(asg1[0].assignee.rawassignees))
        self.assertEqual(20, len(asg0[0].assignee.patents))
        self.assertEqual(1, asgs.count())


        # override the default values provided
        alchemy.match(asg0[0], session, {"organization": u"Kevin"})
        self.assertEqual("Kevin", asg0[0].assignee.organization)

        # determine the most common organization name
        alchemy.match(session.query(RawAssignee).limit(40).all(), session)
        self.assertEqual(40, len(asg1[0].assignee.rawassignees))
        self.assertEqual("The Procter & Gamble Company", asg0[0].assignee.organization)

    def test_inventormatch(self):
        # blindly assume first 10 are the same
        inv0 = session.query(RawInventor).limit(10)
        inv1 = session.query(RawInventor).limit(10).offset(10)
        invs = session.query(Inventor)

        alchemy.match(inv0, session)
        alchemy.match(inv1, session)

        # create two items
        self.assertEqual(10, len(inv0[0].inventor.rawinventors))
        self.assertEqual(10, len(inv1[0].inventor.rawinventors))
        self.assertEqual(2, invs.count())
        self.assertEqual(6, len(inv0[0].inventor.patents))
        self.assertEqual(5, len(inv1[0].inventor.patents))
        self.assertEqual("David C. Mattison", inv0[0].inventor.name_full)

        # merge the assignees together
        alchemy.match([inv0[0], inv1[0]], session)
        self.assertEqual(20, len(inv0[0].inventor.rawinventors))
        self.assertEqual(20, len(inv1[0].inventor.rawinventors))
        self.assertEqual(11, len(inv0[0].inventor.patents))
        self.assertEqual(1, invs.count())

        # override the default values provided
        alchemy.match(inv0[0], session, {"name_first": u"Kevin", "name_last": u"Yu"})
        self.assertEqual("Kevin Yu", inv0[0].inventor.name_full)

        # determine the most common organization name
        alchemy.match(session.query(RawInventor).all(), session)
        self.assertEqual(137, len(inv1[0].inventor.rawinventors))
        self.assertEqual("Robert Wang", inv0[0].inventor.name_full)

    def test_lawyermatch(self):
        # blindly assume first 10 are the same
        law0 = session.query(RawLawyer).limit(10)
        law1 = session.query(RawLawyer).limit(10).offset(10)
        laws = session.query(Lawyer)

        alchemy.match(law0, session)
        alchemy.match(law1, session)

        # create two items
        self.assertEqual(10, len(law0[0].lawyer.rawlawyers))
        self.assertEqual(10, len(law1[0].lawyer.rawlawyers))
        self.assertEqual(2, laws.count())
        self.assertEqual(7, len(law0[0].lawyer.patents))
        self.assertEqual(9, len(law1[0].lawyer.patents))
        self.assertEqual("Warner Norcross & Judd LLP", law0[0].lawyer.organization)

        # merge the assignees together
        alchemy.match([law0[0], law1[0]], session)
        self.assertEqual(20, len(law0[0].lawyer.rawlawyers))
        self.assertEqual(20, len(law1[0].lawyer.rawlawyers))
        self.assertEqual(15, len(law0[0].lawyer.patents))

        self.assertEqual(1, laws.count())

        # override the default values provided
        alchemy.match(law0[0], session, {"name_first": u"Devin", "name_last": u"Ko"})
        self.assertEqual("Devin Ko", law0[0].lawyer.name_full)

        # determine the most common organization name
        alchemy.match(session.query(RawLawyer).all(), session)

        self.assertEqual(57, len(law1[0].lawyer.rawlawyers))
        self.assertEqual("Robert Robert Chuey", law0[0].lawyer.name_full)

    def test_locationmatch(self):
        # blindly assume first 10 are the same
        loc0 = session.query(RawLocation).limit(10)
        loc1 = session.query(RawLocation).limit(10).offset(10)
        locs = session.query(Location)

        alchemy.match(loc0, session)
        alchemy.match(loc1, session)

        # create two items
        self.assertEqual(10, len(loc0[0].location.rawlocations))
        self.assertEqual(10, len(loc1[0].location.rawlocations))
        self.assertEqual(0, len(loc0[0].location.assignees))
        self.assertEqual(0, len(loc0[0].location.inventors))
        self.assertEqual(2, locs.count())
        self.assertEqual("Hong Kong, MN, NL", loc0[0].location.address)

        # merge the assignees together
        alchemy.match([loc0[0], loc1[0]], session)
        self.assertEqual(20, len(loc0[0].location.rawlocations))
        self.assertEqual(20, len(loc1[0].location.rawlocations))
        self.assertEqual(0, len(loc0[0].location.assignees))
        self.assertEqual(0, len(loc0[0].location.inventors))
        self.assertEqual(1, locs.count())
        self.assertEqual("Hong Kong, MN, US", loc0[0].location.address)
        self.assertEqual(None, loc0[0].location.latitude)
        self.assertEqual(None, loc0[0].location.longitude)

        # override the default values provided
        alchemy.match(loc0[0], session, {"city": u"Frisco", "state": u"Cali", "country": u"US", "longitude": 10.0, "latitude": 10.0})
        self.assertEqual("Frisco, Cali, US", loc0[0].location.address)
        self.assertEqual(10.0, loc0[0].location.latitude)
        self.assertEqual(10.0, loc0[0].location.longitude)

    def test_assignee_location(self):
        # insert an assignee first.
        # then location. make sure links ok
        asg = session.query(RawAssignee).limit(20)
        loc = session.query(RawLocation).limit(40)

        alchemy.match(asg[0:5], session)
        alchemy.match(asg[5:10], session)
        alchemy.match(asg[10:15], session)
        alchemy.match(asg[15:20], session)
        alchemy.match(loc[0:20], session)
        alchemy.match(loc[20:40], session)

        self.assertEqual(2, len(loc[19].location.assignees))
        self.assertEqual(1, len(asg[4].assignee.locations))
        self.assertEqual(2, len(asg[5].assignee.locations))

    def test_inventor_location(self):
        # insert an assignee first.
        # then location. make sure links ok
        inv = session.query(RawInventor).limit(20)
        loc = session.query(RawLocation).limit(40)

        alchemy.match(inv[0:5], session)
        alchemy.match(inv[5:10], session)
        alchemy.match(inv[10:15], session)
        alchemy.match(inv[15:20], session)
        alchemy.match(loc[0:20], session)
        alchemy.match(loc[20:40], session)

        self.assertEqual(1, len(inv[14].inventor.locations))
        self.assertEqual(2, len(inv[15].inventor.locations))
        self.assertEqual(4, len(loc[19].location.inventors))
        self.assertEqual(1, len(loc[20].location.inventors))

    def test_location_assignee(self):
        asg = session.query(RawAssignee).limit(20)
        loc = session.query(RawLocation).limit(40)

        alchemy.match(loc[0:20], session)
        alchemy.match(loc[20:40], session)
        alchemy.match(asg[0:5], session)
        alchemy.match(asg[5:10], session)
        alchemy.match(asg[10:15], session)
        alchemy.match(asg[15:20], session)

        self.assertEqual(2, len(loc[19].location.assignees))
        self.assertEqual(1, len(asg[4].assignee.locations))
        self.assertEqual(2, len(asg[5].assignee.locations))

    def test_location_inventor(self):
        # insert an assignee first.
        # then location. make sure links ok
        inv = session.query(RawInventor).limit(20)
        loc = session.query(RawLocation).limit(40)

        alchemy.match(loc[0:20], session)
        alchemy.match(loc[20:40], session)
        alchemy.match(inv[0:5], session)
        alchemy.match(inv[5:10], session)
        alchemy.match(inv[10:15], session)
        alchemy.match(inv[15:20], session)

        self.assertEqual(1, len(inv[14].inventor.locations))
        self.assertEqual(2, len(inv[15].inventor.locations))
        self.assertEqual(4, len(loc[19].location.inventors))
        self.assertEqual(1, len(loc[20].location.inventors))

if __name__ == '__main__':
    config = alchemy.get_config()
    session = alchemy.session
    unittest.main()
