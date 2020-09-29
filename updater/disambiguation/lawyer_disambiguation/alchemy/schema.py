"""
Copyright (c) 2013 The Regents of the University of California, AMERICAN INSTITUTES FOR RESEARCH
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
"""
@author Gabe Fierro gt.fierro@berkeley.edu github.com/gtfierro
"""
"""
Contains schemas for the database
"""
from sqlalchemy import func
from sqlalchemy import Column, Date, Integer, Float, Boolean, VARCHAR
from sqlalchemy import ForeignKey, Index
from sqlalchemy import Unicode, UnicodeText
from sqlalchemy.orm import deferred, relationship
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import MetaData
from unidecode import unidecode

from sqlalchemy import Table
from . import schema_func

cascade = "all, delete, delete-orphan"

def init(self, *args, **kwargs):
    for i, arg in enumerate(args):
        self.__dict__[self.kw[i]] = arg
    for k, v in kwargs.items():
        self.__dict__[k] = v

grantmetadata = MetaData()
appmetadata = MetaData()
GrantBase = declarative_base(metadata=grantmetadata)
ApplicationBase = declarative_base(metadata=appmetadata)
GrantBase.__init__ = init
ApplicationBase.__init__ = init

# table to handle joins for updates (see lib.tasks)
temporary_update = Table('temporary_update', grantmetadata, Column('pk', VARCHAR(length=36), primary_key=True), Column('update', VARCHAR(length=36), index=True))
app_temporary_update = Table('temporary_update', appmetadata, Column('pk', VARCHAR(length=36), primary_key=True), Column('update', VARCHAR(length=36), index=True))

# ASSOCIATION ----------------------

patentassignee = Table(
    'patent_assignee', GrantBase.metadata,
    Column('patent_id', Unicode(20), ForeignKey('patent.id')),
    Column('assignee_id', Unicode(36), ForeignKey('assignee.id')))

patentinventor = Table(
    'patent_inventor', GrantBase.metadata,
    Column('patent_id', Unicode(20), ForeignKey('patent.id')),
    Column('inventor_id', Unicode(36), ForeignKey('inventor.id')))

patentlawyer = Table(
    'patent_lawyer', GrantBase.metadata,
    Column('patent_id', Unicode(20), ForeignKey('patent.id')),
    Column('lawyer_id', Unicode(36), ForeignKey('lawyer.id')))

locationassignee = Table(
    'location_assignee', GrantBase.metadata,
    Column('location_id', Unicode(128), ForeignKey('location.id')),
    Column('assignee_id', Unicode(36), ForeignKey('assignee.id')))

locationinventor = Table(
    'location_inventor', GrantBase.metadata,
    Column('location_id', Unicode(128), ForeignKey('location.id')),
    Column('inventor_id', Unicode(36), ForeignKey('inventor.id')))

# PATENT ---------------------------


class Patent(GrantBase):
    __tablename__ = "patent"
    id = Column(Unicode(20), primary_key=True)
    type = Column(Unicode(100))
    number = Column(Unicode(64))
    country = Column(Unicode(20))
    date = Column(Date)
    abstract = deferred(Column(UnicodeText))
    title = deferred(Column(UnicodeText))
    kind = Column(Unicode(10))
    num_claims = Column(Integer)
    filename = Column(Unicode(120))

    application = relationship("Application", uselist=False, backref="patent", cascade=cascade)
    classes = relationship("USPC", backref="patent", cascade=cascade)
    current_classes = relationship("USPC_current", backref="patent", cascade=cascade)
    ipcrs = relationship("IPCR", backref="patent", cascade=cascade)

    rawassignees = relationship("RawAssignee", backref="patent", cascade=cascade)
    rawinventors = relationship("RawInventor", backref="patent", cascade=cascade)
    rawlawyers = relationship("RawLawyer", backref="patent", cascade=cascade)
    claims = relationship("Claim", backref="patent", cascade=cascade)
    uspatentcitations = relationship(
        "USPatentCitation",
        primaryjoin="Patent.id == USPatentCitation.patent_id",
        backref="patent", cascade=cascade)
    uspatentcitedby = relationship(
        "USPatentCitation",
        primaryjoin="Patent.id == USPatentCitation.citation_id",
        foreign_keys="USPatentCitation.citation_id",
        backref="citation", cascade=cascade)
    usapplicationcitations = relationship("USApplicationCitation", backref="patent", cascade=cascade)
    foreigncitations = relationship("ForeignCitation", backref="patent", cascade=cascade)
    otherreferences = relationship("OtherReference", backref="patent", cascade=cascade)
    usreldocs = relationship(
        "USRelDoc",
        primaryjoin="Patent.id == USRelDoc.patent_id",
        backref="patent", cascade=cascade)
    relpatents = relationship(
        "USRelDoc",
        primaryjoin="Patent.id == USRelDoc.rel_id",
        foreign_keys="USRelDoc.rel_id",
        backref="relpatent", cascade=cascade)
    assignees = relationship("Assignee", secondary=patentassignee, backref="patents")
    inventors = relationship("Inventor", secondary=patentinventor, backref="patents")
    lawyers = relationship("Lawyer", secondary=patentlawyer, backref="patents")

    __table_args__ = (
        Index("pat_idx1", "type", "number", unique=True),
        Index("pat_idx2", "date"),
    )

    @hybrid_property
    def citations(self):
        return self.uspatentcitations + self.usapplicationcitations +\
                self.foreigncitations + self.otherreferences

    def stats(self):
        return {
            "classes": len(self.classes),
            "current_classes": len(self.current_classes),
            "ipcrs": len(self.ipcrs),
            "rawassignees": len(self.rawassignees),
            "rawinventors": len(self.rawinventors),
            "rawlawyers": len(self.rawlawyers),
            "otherreferences": len(self.otherreferences),
            "uspatentcitations": len(self.uspatentcitations),
            "usapplicationcitations": len(self.usapplicationcitations),
            "foreigncitations": len(self.foreigncitations),
            "uspatentcitedby": len(self.uspatentcitedby),
            "usreldocs": len(self.usreldocs),
            "relpatents": len(self.relpatents),
        }

    def __repr__(self):
        return "<Patent('{0}, {1}')>".format(self.number, self.date)


class Application(GrantBase):
    __tablename__ = "application"
    id = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"), primary_key=True)
    type = Column(Unicode(20))
    number = Column(Unicode(64))
    country = Column(Unicode(20))
    date = Column(Date)
    
    usapplicationcitations = relationship(
        "USApplicationCitation",
        primaryjoin="Application.id == USApplicationCitation.application_id",
        foreign_keys="USApplicationCitation.application_id",
        backref="application", cascade=cascade)
    __table_args__ = (
        Index("app_idx1", "type", "number"),
        Index("app_idx2", "date"),
    )
    #granted = Column(Boolean)
    #num_claims = Column(Integer)

    def __repr__(self):
        return "<Application('{0}')>".format(self.id)


# SUPPORT --------------------------


class RawLocation(GrantBase):
    __tablename__ = "rawlocation"
    id = Column(Unicode(128), primary_key=True)
    location_id = Column(Unicode(128), ForeignKey("location.id"))
    city = Column(Unicode(128))
    state = Column(Unicode(20), index=True)
    country = Column(Unicode(10), index=True)
    rawinventors = relationship("RawInventor", backref="rawlocation")
    rawassignees = relationship("RawAssignee", backref="rawlocation")
    __table_args__ = (
        Index("loc_idx1", "city", "state", "country"),
    )

    @hybrid_property
    def address(self):
        addy = []
        if self.city:
            addy.append(self.city)
        if self.state:
            addy.append(self.state)
        if self.country:
            addy.append(self.country)
        return ", ".join(addy)

    # -- Functions for Disambiguation --

    @hybrid_property
    def summarize(self):
        return {
            "city": self.city,
            "state": self.state,
            "country": self.country}

    @hybrid_property
    def uuid(self):
        return self.id

    @hybrid_property
    def __clean__(self):
        return self.location

    @hybrid_property
    def __related__(self):
        return Location

    def unlink(self, session):
        # TODO: probably need to rebuild this
        # hrm (see others for examples)
        clean = self.__clean__
        clean.__raw__.pop(clean.__raw__.index(self))
        clean.assignees = []
        clean.inventors = []
        for raw in clean.__raw__:
            for obj in raw.rawassignees:
                if obj.assignee and obj.assignee not in clean.assignees:
                    clean.assignees.append(obj.assignee)
            for obj in raw.rawinventors:
                if obj.inventor and obj.inventor not in clean.inventors:
                    clean.inventors.append(obj.inventor)
        if len(clean.__raw__) == 0:
            session.delete(clean)

    # ----------------------------------

    def __repr__(self):
        return "<RawLocation('{0}')>".format(unidecode(self.address))


class Location(GrantBase):
    __tablename__ = "location"
    id = Column(Unicode(128), primary_key=True)
    city = Column(Unicode(128))
    state = Column(Unicode(20), index=True)
    country = Column(Unicode(10), index=True)
    latitude = Column(Float)
    longitude = Column(Float)
    assignees = relationship("Assignee", secondary=locationassignee, backref="locations")
    inventors = relationship("Inventor", secondary=locationinventor, backref="locations")
    rawlocations = relationship("RawLocation", backref="location")
    __table_args__ = (
        Index("dloc_idx1", "latitude", "longitude"),
        Index("dloc_idx2", "city", "state", "country"),
    )

    @hybrid_property
    def address(self):
        addy = []
        if self.city:
            addy.append(self.city)
        if self.state:
            addy.append(self.state)
        if self.country:
            addy.append(self.country)
        return ", ".join(addy)

    # -- Functions for Disambiguation --

    @hybrid_property
    def summarize(self):
        return {
            "id": self.id,
            "city": self.city,
            "state": self.state,
            "country": self.country,
            "latitude": self.latitude,
            "longitude": self.longitude}

    @hybrid_property
    def __raw__(self):
        return self.rawlocations

    @hybrid_property
    def __related__(self):
        return RawLocation

    def __rawgroup__(self, session, key):
        if key in RawLocation.__dict__:
            return session.query(RawLocation.__dict__[key], func.count()).filter(
                RawLocation.location_id == self.id).group_by(
                    RawLocation.__dict__[key]).all()
        else:
            return []

    def relink(self, session, obj):
        if obj == self:
            return
        if obj.__tablename__[:3] == "raw":
            self.assignees.extend([asg.assignee for asg in obj.rawassignees if asg.assignee])
            self.inventors.extend([inv.inventor for inv in obj.rawinventors if inv.inventor])
            if obj and obj not in self.__raw__:
                self.__raw__.append(obj)
            self.assignees = list(set(self.assignees))
            self.inventors = list(set(self.inventors))
        else:
            session.query(RawLocation).filter(
                RawLocation.location_id == obj.id).update(
                    {RawLocation.location_id: self.id},
                    synchronize_session=False)
            session.query(locationassignee).filter(
                locationassignee.c.location_id == obj.id).update(
                    {locationassignee.c.location_id: self.id},
                    synchronize_session=False)
            session.query(locationinventor).filter(
                locationinventor.c.location_id == obj.id).update(
                    {locationinventor.c.location_id: self.id},
                    synchronize_session=False)

    def update(self, **kwargs):
        if "city" in kwargs:
            self.city = kwargs["city"]
        if "state" in kwargs:
            self.state = kwargs["state"]
        if "country" in kwargs:
            self.country = kwargs["country"]
        if "latitude" in kwargs:
            self.latitude = kwargs["latitude"]
        if "longitude" in kwargs:
            self.longitude = kwargs["longitude"]

    @classmethod
    def fetch(self, session, default={}):
        return schema_func.fetch(
            Location,
            [["id"],
             ["city", "state", "country"],
             ["longitude", "latitude"]],
            session, default)

    # ----------------------------------

    def __repr__(self):
        return "<Location('{0}')>".format(self.address)


# OBJECTS --------------------------


class RawAssignee(GrantBase):
    __tablename__ = "rawassignee"
    uuid = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    assignee_id = Column(Unicode(36), ForeignKey("assignee.id"))
    rawlocation_id = Column(Unicode(128), ForeignKey("rawlocation.id"))
    type = Column(Unicode(10))
    name_first = Column(Unicode(64))
    name_last = Column(Unicode(64))
    organization = Column(Unicode(128))
    #residence = Column(Unicode(10))
    #nationality = Column(Unicode(10))
    sequence = Column(Integer, index=True)

    # -- Functions for Disambiguation --

    @hybrid_property
    def summarize(self):
        return {
            "type": self.type,
            "name_first": self.name_first,
            "name_last": self.name_last,
            "organization": self.organization}
            #"residence": self.residence,
            #"nationality": self.nationality}

    @hybrid_property
    def __clean__(self):
        return self.assignee

    @hybrid_property
    def __related__(self):
        return Assignee

    def unlink(self, session):
        clean = self.__clean__
        pats = [obj.patent_id for obj in clean.__raw__ if obj.patent_id == self.patent_id]
        locs = [obj.rawlocation.location_id for obj in clean.__raw__ if obj.rawlocation.location_id == self.rawlocation.location_id]
        if len(pats) == 1:
            session.query(patentassignee).filter(
                patentassignee.c.patent_id == self.patent_id).delete(
                    synchronize_session=False)
        if len(locs) == 1:
            session.query(locationassignee).filter(
                locationassignee.c.location_id == self.rawlocation.location_id).delete(
                    synchronize_session=False)
        session.query(RawAssignee).filter(
            RawAssignee.uuid == self.uuid).update(
                {RawAssignee.assignee_id: None},
                synchronize_session=False)
        if len(clean.__raw__) == 0:
            session.delete(clean)
        session.commit()

    # ----------------------------------

    def __repr__(self):
        if self.organization:
            return_string = self.organization
        else:
            return_string = "{0} {1}".format(self.name_first, self.name_last)
        return "<RawAssignee('{0}')>".format(unidecode(return_string))


class RawInventor(GrantBase):
    __tablename__ = "rawinventor"
    uuid = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    inventor_id = Column(Unicode(36), ForeignKey("inventor.id"))
    rawlocation_id = Column(Unicode(128), ForeignKey("rawlocation.id"))
    name_first = Column(Unicode(64))
    name_last = Column(Unicode(64))
    sequence = Column(Integer, index=True)

    # -- Functions for Disambiguation --

    @hybrid_property
    def summarize(self):
        return {
            "name_first": self.name_first,
            "name_last": self.name_last}

    @hybrid_property
    def __clean__(self):
        return self.inventor

    @hybrid_property
    def __related__(self):
        return Inventor

    def unlink(self, session):
        clean = self.__clean__
        pats = [obj.patent_id for obj in clean.__raw__ if obj.patent_id == self.patent_id]
        locs = [obj.rawlocation.location_id for obj in clean.__raw__ if obj.rawlocation.location_id == self.rawlocation.location_id]
        if len(pats) == 1:
            session.query(patentinventor).filter(
                patentinventor.c.patent_id == self.patent_id).delete(
                    synchronize_session=False)
        if len(locs) == 1:
            session.query(locationinventor).filter(
                locationinventor.c.location_id == self.rawlocation.location_id).delete(
                    synchronize_session=False)
        session.query(RawInventor).filter(
            RawInventor.uuid == self.uuid).update(
                {RawInventor.inventor_id: None},
                synchronize_session=False)
        if len(clean.__raw__) == 0:
            session.delete(clean)
        session.commit()

    # ----------------------------------

    @hybrid_property
    def name_full(self):
        return "{first} {last}".format(
            first=self.name_first,
            last=self.name_last)

    def __repr__(self):
        return "<RawInventor('{0}')>".format(unidecode(self.name_full))


class RawLawyer(GrantBase):
    __tablename__ = "rawlawyer"
    uuid = Column(Unicode(36), primary_key=True)
    lawyer_id = Column(Unicode(36), ForeignKey("lawyer.id"))
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    name_first = Column(Unicode(64))
    name_last = Column(Unicode(64))
    organization = Column(Unicode(64))
    alpha_lawyer_id = Column(Unicode(128))
    country = Column(Unicode(10))
    sequence = Column(Integer, index=True)

    @hybrid_property
    def name_full(self):
        return "{first} {last}".format(
            first=self.name_first,
            last=self.name_last)

    # -- Functions for Disambiguation --

    @hybrid_property
    def summarize(self):
        return {
            "name_first": self.name_first,
            "name_last": self.name_last,
            "organization": self.organization,
            "country": self.country,
            "sequence": self.sequence}

    @hybrid_property
    def __clean__(self):
        return self.lawyer

    @hybrid_property
    def __related__(self):
        return Lawyer

    def unlink(self, session):
        clean = self.__clean__
        pats = [obj.patent_id for obj in clean.__raw__ if obj.patent_id == self.patent_id]
        if len(pats) == 1:
            session.query(patentlawyer).filter(
                patentlawyer.c.patent_id == self.patent_id).delete(
                    synchronize_session=False)
        session.query(RawLawyer).filter(
            RawLawyer.uuid == self.uuid).update(
                {RawLawyer.lawyer_id: None},
                synchronize_session=False)
        if len(clean.__raw__) == 0:
            session.delete(clean)
        session.commit()

    # ----------------------------------

    def __repr__(self):
        data = []
        if self.name_first:
            data.append("{0} {1}".format(self.name_first, self.name_last))
        if self.organization:
            data.append(self.organization)
        return "<RawLawyer('{0}')>".format(unidecode(", ".join(data)))


# DISAMBIGUATED -----------------------


class Assignee(GrantBase):
    __tablename__ = "assignee"
    id = Column(Unicode(36), primary_key=True)
    type = Column(Unicode(10))
    name_first = Column(Unicode(64))
    name_last = Column(Unicode(64))
    organization = Column(Unicode(256))
    #residence = Column(Unicode(10))
    #nationality = Column(Unicode(10))
    rawassignees = relationship("RawAssignee", backref="assignee")

    # -- Functions for Disambiguation --

    @hybrid_property
    def summarize(self):
        return {
            "id": self.id,
            "type": self.type,
            "name_first": self.name_first,
            "name_last": self.name_last,
            "organization": self.organization}
            #"residence": self.residence,
            #"nationality": self.nationality}

    @hybrid_property
    def __raw__(self):
        return self.rawassignees

    @hybrid_property
    def __related__(self):
        return RawAssignee

    def __rawgroup__(self, session, key):
        if key in RawAssignee.__dict__:
            return session.query(RawAssignee.__dict__[key], func.count()).filter(
                RawAssignee.assignee_id == self.id).group_by(
                    RawAssignee.__dict__[key]).all()
        else:
            return []

    def relink(self, session, obj):
        if obj == self:
            return
        if obj.__tablename__[:3] == "raw":
            if obj.rawlocation:
                if obj.rawlocation.location:
                    self.locations.append(obj.rawlocation.location)
            if obj.patent and obj.patent not in self.patents:
                self.patents.append(obj.patent)
            if obj and obj not in self.__raw__:
                self.__raw__.append(obj)
        else:
            session.query(RawAssignee).filter(
                RawAssignee.assignee_id == obj.id).update(
                    {RawAssignee.assignee_id: self.id},
                    synchronize_session=False)
            session.query(patentassignee).filter(
                patentassignee.c.assignee_id == obj.id).update(
                    {patentassignee.c.assignee_id: self.id},
                    synchronize_session=False)
            session.query(locationassignee).filter(
                locationassignee.c.assignee_id == obj.id).update(
                    {locationassignee.c.assignee_id: self.id},
                    synchronize_session=False)

    def update(self, **kwargs):
        if "type" in kwargs:
            self.type = kwargs["type"]
        if "name_first" in kwargs:
            self.name_first = kwargs["name_first"]
        if "name_last" in kwargs:
            self.name_last = kwargs["name_last"]
        if "organization" in kwargs:
            self.organization = kwargs["organization"]
        #if "residence" in kwargs:
        #    self.residence = kwargs["residence"]
        #if "nationality" in kwargs:
        #    self.nationality = kwargs["nationality"]

    @classmethod
    def fetch(self, session, default={}):
        return schema_func.fetch(
            Assignee,
            [["id"]],
            session, default)

    # ----------------------------------

    def __repr__(self):
        if self.organization:
            return_string = self.organization
        else:
            return_string = "{0} {1}".format(self.name_first, self.name_last)
        return "<Assignee('{0}')>".format(unidecode(return_string))


class Inventor(GrantBase):
    __tablename__ = "inventor"
    id = Column(Unicode(36), primary_key=True)
    name_first = Column(Unicode(64))
    name_last = Column(Unicode(64))
    rawinventors = relationship("RawInventor", backref="inventor")

    @hybrid_property
    def name_full(self):
        return "{first} {last}".format(
            first=self.name_first,
            last=self.name_last)

    # -- Functions for Disambiguation --

    @hybrid_property
    def summarize(self):
        return {
            "id": self.id,
            "name_first": self.name_first,
            "name_last": self.name_last}

    @hybrid_property
    def __raw__(self):
        return self.rawinventors

    @hybrid_property
    def __related__(self):
        return RawInventor

    def __rawgroup__(self, session, key):
        if key in RawInventor.__dict__:
            return session.query(RawInventor.__dict__[key], func.count()).filter(
                RawInventor.inventor_id == self.id).group_by(
                    RawInventor.__dict__[key]).all()
        else:
            return []

    def relink(self, session, obj):
        if obj == self:
            return
        if obj.__tablename__[:3] == "raw":
            if obj.rawlocation.location:
                self.locations.append(obj.rawlocation.location)
            if obj.patent and obj.patent not in self.patents:
                self.patents.append(obj.patent)
            if obj and obj not in self.__raw__:
                self.__raw__.append(obj)
        else:
            session.query(RawInventor).filter(
                RawInventor.inventor_id == obj.id).update(
                    {RawInventor.inventor_id: self.id},
                    synchronize_session=False)
            session.query(patentinventor).filter(
                patentinventor.c.inventor_id == obj.id).update(
                    {patentinventor.c.inventor_id: self.id},
                    synchronize_session=False)
            session.query(locationinventor).filter(
                locationinventor.c.inventor_id == obj.id).update(
                    {locationinventor.c.inventor_id: self.id},
                    synchronize_session=False)

    def update(self, **kwargs):
        if "name_first" in kwargs:
            self.name_first = kwargs["name_first"]
        if "name_last" in kwargs:
            self.name_last = kwargs["name_last"]

    @classmethod
    def fetch(self, session, default={}):
        return schema_func.fetch(
            Inventor,
            [["id"]],
            session, default)

    # ----------------------------------

    def __repr__(self):
        return "<Inventor('{0}')>".format(unidecode(self.name_full))


class Lawyer(GrantBase):
    __tablename__ = "lawyer"
    id = Column(Unicode(36), primary_key=True)
    name_first = Column(Unicode(64))
    name_last = Column(Unicode(64))
    organization = Column(Unicode(64))
    country = Column(Unicode(10))
    rawlawyers = relationship("RawLawyer", backref="lawyer")

    @hybrid_property
    def name_full(self):
        return "{first} {last}".format(
            first=self.name_first,
            last=self.name_last)

    # -- Functions for Disambiguation --

    @hybrid_property
    def summarize(self):
        return {
            "id": self.id,
            "name_first": self.name_first,
            "name_last": self.name_last,
            "organization": self.organization,
            "country": self.country}

    @hybrid_property
    def __raw__(self):
        return self.rawlawyers

    @hybrid_property
    def __related__(self):
        return RawLawyer

    def __rawgroup__(self, session, key):
        if key in RawLawyer.__dict__:
            return session.query(RawLawyer.__dict__[key], func.count()).filter(
                RawLawyer.lawyer_id == self.id).group_by(
                    RawLawyer.__dict__[key]).all()
        else:
            return []

    def relink(self, session, obj):
        if obj == self:
            return
        if obj.__tablename__[:3] == "raw":
            if obj.patent and obj.patent not in self.patents:
                self.patents.append(obj.patent)
            if obj and obj not in self.__raw__:
                self.__raw__.append(obj)
        else:
            session.query(RawLawyer).filter(
                RawLawyer.lawyer_id == obj.id).update(
                    {RawLawyer.lawyer_id: self.id},
                    synchronize_session=False)
            session.query(patentlawyer).filter(
                patentlawyer.c.lawyer_id == obj.id).update(
                    {patentlawyer.c.lawyer_id: self.id},
                    synchronize_session=False)

    def update(self, **kwargs):
        if "name_first" in kwargs:
            self.name_first = kwargs["name_first"]
        if "name_last" in kwargs:
            self.name_last = kwargs["name_last"]
        if "organization" in kwargs:
            self.organization = kwargs["organization"]
        if "country" in kwargs:
            self.country = kwargs["country"]

    @classmethod
    def fetch(self, session, default={}):
        return schema_func.fetch(
            Lawyer,
            [["id"],
             ["organization"],
             ["name_first", "name_last"]],
            session, default)

    # ----------------------------------

    def __repr__(self):
        data = []
        if self.name_full:
            data.append(self.name_full)
        if self.organization:
            data.append(self.organization)
        return "<Lawyer('{0}')>".format(unidecode(", ".join(data)))


# CLASSIFICATIONS ------------------


class USPC(GrantBase):
    __tablename__ = "uspc"
    uuid = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    mainclass_id = Column(Unicode(20), ForeignKey("mainclass.id"))
    subclass_id = Column(Unicode(20), ForeignKey("subclass.id"))
    sequence = Column(Integer, index=True)

    def __repr__(self):
        return "<USPC('{1}')>".format(self.subclass_id)


class USPC_current(GrantBase):
    __tablename__ = "uspc_current"
    uuid = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    mainclass_id = Column(Unicode(20), ForeignKey("mainclass_current.id"))
    subclass_id = Column(Unicode(20), ForeignKey("subclass_current.id"))
    sequence = Column(Integer, index=True)

    def __repr__(self):
        return "<USPC_current('{1}')>".format(self.subclass_id)

class CPC_current(GrantBase):
    __tablename__ = "cpc_current"
    uuid = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    section_id = Column(Unicode(10))
    subsection_id = Column(Unicode(20), ForeignKey("cpc_subsection.id"))
    group_id = Column(Unicode(20), ForeignKey("cpc_group.id"))
    subgroup_id = Column(Unicode(20), ForeignKey("cpc_subgroup.id"))
    category = Column(Unicode(36))
    sequence = Column(Integer, index=True)

    def __repr__(self):
        return "<CPC_current('{1}')>".format(self.subgroup_id)
    
class IPCR(GrantBase):
    __tablename__ = "ipcr"
    uuid = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    classification_level = Column(Unicode(20))
    section = Column(Unicode(20))
    ipc_class = Column(Unicode(20))
    subclass = Column(Unicode(20))
    main_group = Column(Unicode(20))
    subgroup = Column(Unicode(20))
    symbol_position = Column(Unicode(20))
    classification_value = Column(Unicode(20))
    classification_status = Column(Unicode(20))
    classification_data_source = Column(Unicode(20))
    action_date = Column(Date, index=True)
    ipc_version_indicator = Column(Date, index=True)
    sequence = Column(Integer, index=True)


class MainClass(GrantBase):
    __tablename__ = "mainclass"
    id = Column(Unicode(20), primary_key=True)
    #title = Column(Unicode(256))
    #text = Column(Unicode(256))
    uspc = relationship("USPC", backref="mainclass")

    def __repr__(self):
        return "<MainClass('{0}')>".format(self.id)


class MainClass_current(GrantBase):
    __tablename__ = "mainclass_current"
    id = Column(Unicode(20), primary_key=True)
    title = Column(Unicode(256))
    #text = Column(Unicode(256))
    uspc_current = relationship("USPC_current", backref="mainclass_current")

    def __repr__(self):
        return "<MainClass_current('{0}')>".format(self.id)


class SubClass(GrantBase):
    __tablename__ = "subclass"
    id = Column(Unicode(20), primary_key=True)
    #title = Column(Unicode(256))
    #text = Column(Unicode(256))
    uspc = relationship("USPC", backref="subclass")

    def __repr__(self):
        return "<SubClass('{0}')>".format(self.id)


class SubClass_current(GrantBase):
    __tablename__ = "subclass_current"
    id = Column(Unicode(20), primary_key=True)
    title = Column(Unicode(512))
    #text = Column(Unicode(256))
    uspc_current = relationship("USPC_current", backref="subclass_current")

    def __repr__(self):
        return "<SubClass_current('{0}')>".format(self.id)


class CPC_subsection(GrantBase):
    __tablename__ = "cpc_subsection"
    id = Column(Unicode(20), primary_key=True)
    title = Column(Unicode(256))
    #text = Column(Unicode(256))
    cpc_current = relationship("CPC_current", backref="cpc_subsection")

    def __repr__(self):
        return "<CPC_subsection('{0}')>".format(self.id)

class CPC_group(GrantBase):
    __tablename__ = "cpc_group"
    id = Column(Unicode(20), primary_key=True)
    title = Column(Unicode(256))
    #text = Column(Unicode(256))
    cpc_current = relationship("CPC_current", backref="cpc_group")

    def __repr__(self):
        return "<CPC_group('{0}')>".format(self.id)

class CPC_subgroup(GrantBase):
    __tablename__ = "cpc_subgroup"
    id = Column(Unicode(20), primary_key=True)
    title = Column(Unicode(512))
    #text = Column(Unicode(256))
    cpc_current = relationship("CPC_current", backref="cpc_subgroup")

    def __repr__(self):
        return "<CPC_subgroup('{0}')>".format(self.id)

# REFERENCES -----------------------

class USPatentCitation(GrantBase):
    """
    US Patent Citation schema
    """
    __tablename__ = "uspatentcitation"
    uuid = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    citation_id = Column(Unicode(20), index=True)
    date = Column(Date)
    name = Column(Unicode(64))
    kind = Column(Unicode(10))
    #number = Column(Unicode(64))
    country = Column(Unicode(10))
    category = Column(Unicode(20))
    sequence = Column(Integer)

    def __repr__(self):
        return "<USPatentCitation('{0} {1}, {2}')>".format(self.patent_id, self.citation_id, self.date)

class USApplicationCitation(GrantBase):
    """
    US Application Citation schema
    """
    __tablename__ = "usapplicationcitation"
    uuid = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    application_id = Column(Unicode(20), index=True)
    date = Column(Date)
    name = Column(Unicode(64))
    kind = Column(Unicode(10))
    number = Column(Unicode(64))
    country = Column(Unicode(10))
    category = Column(Unicode(20))
    sequence = Column(Integer)

    def __repr__(self):
        return "<USApplicationCitation('{0} {1}, {2}')>".format(self.patent_id, self.application_id, self.date)

class ForeignCitation(GrantBase):
    """
    Foreign Citation schema
    """
    __tablename__ = "foreigncitation"
    uuid = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    date = Column(Date)
    #kind = Column(Unicode(10))
    number = Column(Unicode(64))
    country = Column(Unicode(10))
    category = Column(Unicode(20))
    sequence = Column(Integer)

    def __repr__(self):
        return "<ForeignCitation('{0} {1}, {2}')>".format(self.patent_id, self.number, self.date)


class OtherReference(GrantBase):
    __tablename__ = "otherreference"
    uuid = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    text = deferred(Column(UnicodeText))
    sequence = Column(Integer)

    def __repr__(self):
        return "<OtherReference('{0}')>".format(unidecode(self.text[:20]))


class USRelDoc(GrantBase):
    __tablename__ = "usreldoc"
    uuid = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey("patent.id"))
    rel_id = Column(Unicode(20), index=True)
    doctype = Column(Unicode(64), index=True)
    status = Column(Unicode(20))
    date = Column(Date, index=True)
    number = Column(Unicode(64), index=True)
    kind = Column(Unicode(10))
    country = Column(Unicode(20), index=True)
    relationship = Column(Unicode(64))
    sequence = Column(Integer, index=True)

    def __repr__(self):
        return "<USRelDoc('{0}, {1}')>".format(self.number, self.date)

class Claim(GrantBase):
    __tablename__ = "claim"
    uuid = Column(Unicode(36), primary_key=True)
    patent_id = Column(Unicode(20), ForeignKey('patent.id'))
    text = deferred(Column(UnicodeText))
    dependent = Column(Integer) # if -1, independent
    sequence = Column(Integer, index=True)

    def __repr__(self):
        return "<Claim('{0}')>".format(self.text)

## Application Tables

# ASSOCIATION ----------------------

applicationassignee = Table(
    'application_assignee', ApplicationBase.metadata,
    Column('application_id', Unicode(20), ForeignKey('application.id')),
    Column('assignee_id', Unicode(36), ForeignKey('assignee.id')))

applicationinventor = Table(
    'application_inventor', ApplicationBase.metadata,
    Column('application_id', Unicode(20), ForeignKey('application.id')),
    Column('inventor_id', Unicode(36), ForeignKey('inventor.id')))

app_locationassignee = Table(
    'location_assignee', ApplicationBase.metadata,
    Column('location_id', Unicode(128), ForeignKey('location.id')),
    Column('assignee_id', Unicode(36), ForeignKey('assignee.id')))

app_locationinventor = Table(
    'location_inventor', ApplicationBase.metadata,
    Column('location_id', Unicode(128), ForeignKey('location.id')),
    Column('inventor_id', Unicode(36), ForeignKey('inventor.id')))

# PATENT ---------------------------

class App_Application(ApplicationBase):
    __tablename__ = "application"
    id = Column(Unicode(36), primary_key=True)
    type = Column(Unicode(20))
    number = Column(Unicode(64))
    country = Column(Unicode(20))
    date = Column(Date)
    abstract = deferred(Column(UnicodeText))
    title = deferred(Column(UnicodeText))
    granted = Column(Boolean)
    num_claims = Column(Integer)
    filename = Column(Unicode(120))
    __table_args__ = (
        Index("app_idx1", "type", "number"),
        Index("app_idx2", "date"),
    )

    classes = relationship("App_USPC", backref="application", cascade=cascade)
    current_classes = relationship("App_USPC_current", backref="application", cascade=cascade)

    rawassignees = relationship("App_RawAssignee", backref="application", cascade=cascade)
    rawinventors = relationship("App_RawInventor", backref="application", cascade=cascade)
    claims = relationship("App_Claim", backref="application", cascade=cascade)
    assignees = relationship("App_Assignee", secondary=applicationassignee, backref="applications")
    inventors = relationship("App_Inventor", secondary=applicationinventor, backref="applications")

    __table_args__ = (
        Index("pat_idx1", "type", "number", unique=True),
        Index("pat_idx2", "date"),
    )

    def __repr__(self):
        return "<Application('{0}')>".format(self.id)

    @hybrid_property
    def citations(self):
        return self.uspatentcitations + self.usapplicationcitations +\
                self.foreigncitations + self.otherreferences

    def stats(self):
        return {
            "classes": len(self.classes),
            "current_classes": len(self.current_classes),
            "ipcrs": len(self.ipcrs),
            "rawassignees": len(self.rawassignees),
            "rawinventors": len(self.rawinventors),
            "otherreferences": len(self.otherreferences),
            "uspatentcitations": len(self.uspatentcitations),
            "usapplicationcitations": len(self.usapplicationcitations),
            "foreigncitations": len(self.foreigncitations),
            "uspatentcitedby": len(self.uspatentcitedby),
            "usreldocs": len(self.usreldocs),
            "relpatents": len(self.relpatents),
        }

# SUPPORT --------------------------


class App_RawLocation(ApplicationBase):
    __tablename__ = "rawlocation"
    id = Column(Unicode(128), primary_key=True)
    location_id = Column(Unicode(128), ForeignKey("location.id"))
    city = Column(Unicode(128))
    state = Column(Unicode(20), index=True)
    country = Column(Unicode(10), index=True)
    rawinventors = relationship("App_RawInventor", backref="rawlocation")
    rawassignees = relationship("App_RawAssignee", backref="rawlocation")
    __table_args__ = (
        Index("loc_idx1", "city", "state", "country"),
    )

    @hybrid_property
    def address(self):
        addy = []
        if self.city:
            addy.append(self.city)
        if self.state:
            addy.append(self.state)
        if self.country:
            addy.append(self.country)
        return ", ".join(addy)

    # -- Functions for Disambiguation --

    @hybrid_property
    def summarize(self):
        return {
            "city": self.city,
            "state": self.state,
            "country": self.country}

    @hybrid_property
    def uuid(self):
        return self.id

    @hybrid_property
    def __clean__(self):
        return self.location

    @hybrid_property
    def __related__(self):
        return App_Location

    def unlink(self, session):
        # TODO: probably need to rebuild this
        # hrm (see others for examples)
        clean = self.__clean__
        clean.__raw__.pop(clean.__raw__.index(self))
        clean.assignees = []
        clean.inventors = []
        for raw in clean.__raw__:
            for obj in raw.rawassignees:
                if obj.assignee and obj.assignee not in clean.assignees:
                    clean.assignees.append(obj.assignee)
            for obj in raw.rawinventors:
                if obj.inventor and obj.inventor not in clean.inventors:
                    clean.inventors.append(obj.inventor)
        if len(clean.__raw__) == 0:
            session.delete(clean)

    # ----------------------------------

    def __repr__(self):
        return "<RawLocation('{0}')>".format(unidecode(self.address))


class App_Location(ApplicationBase):
    __tablename__ = "location"
    id = Column(Unicode(128), primary_key=True)
    city = Column(Unicode(128))
    state = Column(Unicode(20), index=True)
    country = Column(Unicode(10), index=True)
    latitude = Column(Float)
    longitude = Column(Float)
    assignees = relationship("App_Assignee", secondary=app_locationassignee, backref="locations")
    inventors = relationship("App_Inventor", secondary=app_locationinventor, backref="locations")
    rawlocations = relationship("App_RawLocation", backref="location")
    __table_args__ = (
        Index("dloc_idx1", "latitude", "longitude"),
        Index("dloc_idx2", "city", "state", "country"),
    )

    @hybrid_property
    def address(self):
        addy = []
        if self.city:
            addy.append(self.city)
        if self.state:
            addy.append(self.state)
        if self.country:
            addy.append(self.country)
        return ", ".join(addy)

    # -- Functions for Disambiguation --

    @hybrid_property
    def summarize(self):
        return {
            "id": self.id,
            "city": self.city,
            "state": self.state,
            "country": self.country,
            "latitude": self.latitude,
            "longitude": self.longitude}

    @hybrid_property
    def __raw__(self):
        return self.rawlocations

    @hybrid_property
    def __related__(self):
        return App_RawLocation

    def __rawgroup__(self, session, key):
        if key in App_RawLocation.__dict__:
            return session.query(App_RawLocation.__dict__[key], func.count()).filter(
                App_RawLocation.location_id == self.id).group_by(
                    RawLocation.__dict__[key]).all()
        else:
            return []

    def relink(self, session, obj):
        if obj == self:
            return
        if obj.__tablename__[:3] == "raw":
            self.assignees.extend([asg.assignee for asg in obj.rawassignees if asg.assignee])
            self.inventors.extend([inv.inventor for inv in obj.rawinventors if inv.inventor])
            if obj and obj not in self.__raw__:
                self.__raw__.append(obj)
            self.assignees = list(set(self.assignees))
            self.inventors = list(set(self.inventors))
        else:
            session.query(App_RawLocation).filter(
                App_RawLocation.location_id == obj.id).update(
                    {App_RawLocation.location_id: self.id},
                    synchronize_session=False)
            session.query(app_locationassignee).filter(
                app_locationassignee.c.location_id == obj.id).update(
                    {app_locationassignee.c.location_id: self.id},
                    synchronize_session=False)
            session.query(app_locationinventor).filter(
                app_locationinventor.c.location_id == obj.id).update(
                    {app_locationinventor.c.location_id: self.id},
                    synchronize_session=False)

    def update(self, **kwargs):
        if "city" in kwargs:
            self.city = kwargs["city"]
        if "state" in kwargs:
            self.state = kwargs["state"]
        if "country" in kwargs:
            self.country = kwargs["country"]
        if "latitude" in kwargs:
            self.latitude = kwargs["latitude"]
        if "longitude" in kwargs:
            self.longitude = kwargs["longitude"]

    @classmethod
    def fetch(self, session, default={}):
        return schema_func.fetch(
            App_Location,
            [["id"],
             ["city", "state", "country"],
             ["longitude", "latitude"]],
            session, default)

    # ----------------------------------

    def __repr__(self):
        return "<Location('{0}')>".format(self.address)


# OBJECTS --------------------------


class App_RawAssignee(ApplicationBase):
    __tablename__ = "rawassignee"
    uuid = Column(Unicode(36), primary_key=True)
    application_id = Column(Unicode(20), ForeignKey("application.id"))
    assignee_id = Column(Unicode(36), ForeignKey("assignee.id"))
    rawlocation_id = Column(Unicode(128), ForeignKey("rawlocation.id"))
    type = Column(Unicode(10))
    name_first = Column(Unicode(64))
    name_last = Column(Unicode(64))
    organization = Column(Unicode(256))
    #residence = Column(Unicode(10))
    #nationality = Column(Unicode(10))
    sequence = Column(Integer, index=True)

    # -- Functions for Disambiguation --

    @hybrid_property
    def summarize(self):
        return {
            "type": self.type,
            "name_first": self.name_first,
            "name_last": self.name_last,
            "organization": self.organization}
            #"residence": self.residence,
            #"nationality": self.nationality}

    @hybrid_property
    def __clean__(self):
        return self.assignee

    @hybrid_property
    def __related__(self):
        return App_Assignee

    def unlink(self, session):
        clean = self.__clean__
        pats = [obj.application_id for obj in clean.__raw__ if obj.application_id == self.application_id]
        locs = [obj.rawlocation.location_id for obj in clean.__raw__ if obj.rawlocation.location_id == self.rawlocation.location_id]
        if len(pats) == 1:
            session.query(applicationassignee).filter(
                applicationassignee.c.application_id == self.application_id).delete(
                    synchronize_session=False)
        if len(locs) == 1:
            session.query(app_locationassignee).filter(
                app_locationassignee.c.location_id == self.rawlocation.location_id).delete(
                    synchronize_session=False)
        session.query(App_RawAssignee).filter(
            App_RawAssignee.uuid == self.uuid).update(
                {App_RawAssignee.assignee_id: None},
                synchronize_session=False)
        if len(clean.__raw__) == 0:
            session.delete(clean)
        session.commit()

    # ----------------------------------

    def __repr__(self):
        if self.organization:
            return_string = self.organization
        else:
            return_string = "{0} {1}".format(self.name_first, self.name_last)
        return "<RawAssignee('{0}')>".format(unidecode(return_string))


class App_RawInventor(ApplicationBase):
    __tablename__ = "rawinventor"
    uuid = Column(Unicode(36), primary_key=True)
    application_id = Column(Unicode(20), ForeignKey("application.id"))
    inventor_id = Column(Unicode(36), ForeignKey("inventor.id"))
    rawlocation_id = Column(Unicode(128), ForeignKey("rawlocation.id"))
    name_first = Column(Unicode(64))
    name_last = Column(Unicode(64))
    sequence = Column(Integer, index=True)

    # -- Functions for Disambiguation --

    @hybrid_property
    def summarize(self):
        return {
            "name_first": self.name_first,
            "name_last": self.name_last}

    @hybrid_property
    def __clean__(self):
        return self.inventor

    @hybrid_property
    def __related__(self):
        return App_Inventor

    def unlink(self, session):
        clean = self.__clean__
        pats = [obj.application_id for obj in clean.__raw__ if obj.application_id == self.application_id]
        locs = [obj.rawlocation.location_id for obj in clean.__raw__ if obj.rawlocation.location_id == self.rawlocation.location_id]
        if len(pats) == 1:
            session.query(applicationinventor).filter(
                applicationinventor.c.application_id == self.application_id).delete(
                    synchronize_session=False)
        if len(locs) == 1:
            session.query(app_locationinventor).filter(
                app_locationinventor.c.location_id == self.rawlocation.location_id).delete(
                    synchronize_session=False)
        session.query(App_RawInventor).filter(
            App_RawInventor.uuid == self.uuid).update(
                {App_RawInventor.inventor_id: None},
                synchronize_session=False)
        if len(clean.__raw__) == 0:
            session.delete(clean)
        session.commit()

    # ----------------------------------

    @hybrid_property
    def name_full(self):
        return "{first} {last}".format(
            first=self.name_first,
            last=self.name_last)

    def __repr__(self):
        return "<RawInventor('{0}')>".format(unidecode(self.name_full))


# DISAMBIGUATED -----------------------


class App_Assignee(ApplicationBase):
    __tablename__ = "assignee"
    id = Column(Unicode(36), primary_key=True)
    type = Column(Unicode(10))
    name_first = Column(Unicode(64))
    name_last = Column(Unicode(64))
    organization = Column(Unicode(256))
    #residence = Column(Unicode(10))
    #nationality = Column(Unicode(10))
    rawassignees = relationship("App_RawAssignee", backref="assignee")

    # -- Functions for Disambiguation --

    @hybrid_property
    def summarize(self):
        return {
            "id": self.id,
            "type": self.type,
            "name_first": self.name_first,
            "name_last": self.name_last,
            "organization": self.organization}
            #"residence": self.residence,
            #"nationality": self.nationality}

    @hybrid_property
    def __raw__(self):
        return self.rawassignees

    @hybrid_property
    def __related__(self):
        return App_RawAssignee

    def __rawgroup__(self, session, key):
        if key in App_RawAssignee.__dict__:
            return session.query(App_RawAssignee.__dict__[key], func.count()).filter(
                App_RawAssignee.assignee_id == self.id).group_by(
                    App_RawAssignee.__dict__[key]).all()
        else:
            return []

    def relink(self, session, obj):
        if obj == self:
            return
        if obj.__tablename__[:3] == "raw":
            if obj.rawlocation.location:
                self.locations.append(obj.rawlocation.location)
            if obj.application and obj.application not in self.applications:
                self.applications.append(obj.application)
            if obj and obj not in self.__raw__:
                self.__raw__.append(obj)
        else:
            session.query(App_RawAssignee).filter(
                App_RawAssignee.assignee_id == obj.id).update(
                    {App_RawAssignee.assignee_id: self.id},
                    synchronize_session=False)
            session.query(applicationassignee).filter(
                applicationassignee.c.assignee_id == obj.id).update(
                    {applicationassignee.c.assignee_id: self.id},
                    synchronize_session=False)
            session.query(app_locationassignee).filter(
                app_locationassignee.c.assignee_id == obj.id).update(
                    {app_locationassignee.c.assignee_id: self.id},
                    synchronize_session=False)

    def update(self, **kwargs):
        if "type" in kwargs:
            self.type = kwargs["type"]
        if "name_first" in kwargs:
            self.name_first = kwargs["name_first"]
        if "name_last" in kwargs:
            self.name_last = kwargs["name_last"]
        if "organization" in kwargs:
            self.organization = kwargs["organization"]
        #if "residence" in kwargs:
        #    self.residence = kwargs["residence"]
        #if "nationality" in kwargs:
        #    self.nationality = kwargs["nationality"]

    @classmethod
    def fetch(self, session, default={}):
        return schema_func.fetch(
            App_Assignee,
            [["id"]],
            session, default)

    # ----------------------------------

    def __repr__(self):
        if self.organization:
            return_string = self.organization
        else:
            return_string = "{0} {1}".format(self.name_first, self.name_last)
        return "<Assignee('{0}')>".format(unidecode(return_string))


class App_Inventor(ApplicationBase):
    __tablename__ = "inventor"
    id = Column(Unicode(36), primary_key=True)
    name_first = Column(Unicode(64))
    name_last = Column(Unicode(64))
    #nationality = Column(Unicode(10))
    rawinventors = relationship("App_RawInventor", backref="inventor")

    @hybrid_property
    def name_full(self):
        return "{first} {last}".format(
            first=self.name_first,
            last=self.name_last)

    # -- Functions for Disambiguation --

    @hybrid_property
    def summarize(self):
        return {
            "id": self.id,
            "name_first": self.name_first,
            "name_last": self.name_last,
            "nationality": self.nationality}

    @hybrid_property
    def __raw__(self):
        return self.rawinventors

    @hybrid_property
    def __related__(self):
        return App_RawInventor

    def __rawgroup__(self, session, key):
        if key in App_RawInventor.__dict__:
            return session.query(App_RawInventor.__dict__[key], func.count()).filter(
                App_RawInventor.inventor_id == self.id).group_by(
                    App_RawInventor.__dict__[key]).all()
        else:
            return []

    def relink(self, session, obj):
        if obj == self:
            return
        if obj.__tablename__[:3] == "raw":
            if obj.rawlocation.location:
                self.locations.append(obj.rawlocation.location)
            if obj.application and obj.application not in self.applications:
                self.applications.append(obj.application)
            if obj and obj not in self.__raw__:
                self.__raw__.append(obj)
        else:
            session.query(App_RawInventor).filter(
                App_RawInventor.inventor_id == obj.id).update(
                    {App_RawInventor.inventor_id: self.id},
                    synchronize_session=False)
            session.query(applicationinventor).filter(
                applicationinventor.c.inventor_id == obj.id).update(
                    {applicationinventor.c.inventor_id: self.id},
                    synchronize_session=False)
            session.query(app_locationinventor).filter(
                app_locationinventor.c.inventor_id == obj.id).update(
                    {app_locationinventor.c.inventor_id: self.id},
                    synchronize_session=False)

    def update(self, **kwargs):
        if "name_first" in kwargs:
            self.name_first = kwargs["name_first"]
        if "name_last" in kwargs:
            self.name_last = kwargs["name_last"]
        if "nationality" in kwargs:
            self.nationality = kwargs["nationality"]

    @classmethod
    def fetch(self, session, default={}):
        return schema_func.fetch(
            App_Inventor,
            [["id"]],
            session, default)

    # ----------------------------------

    def __repr__(self):
        return "<Inventor('{0}')>".format(unidecode(self.name_full))


# CLASSIFICATIONS ------------------


class App_USPC(ApplicationBase):
    __tablename__ = "uspc"
    uuid = Column(Unicode(36), primary_key=True)
    application_id = Column(Unicode(20), ForeignKey("application.id"))
    mainclass_id = Column(Unicode(20), ForeignKey("mainclass.id"))
    subclass_id = Column(Unicode(20), ForeignKey("subclass.id"))
    sequence = Column(Integer, index=True)

    def __repr__(self):
        return "<USPC('{1}')>".format(self.subclass_id)


class App_USPC_current(ApplicationBase):
    __tablename__ = "uspc_current"
    uuid = Column(Unicode(36), primary_key=True)
    application_id = Column(Unicode(20), ForeignKey("application.id"))
    mainclass_id = Column(Unicode(20), ForeignKey("mainclass_current.id"))
    subclass_id = Column(Unicode(20), ForeignKey("subclass_current.id"))
    sequence = Column(Integer, index=True)

    def __repr__(self):
        return "<USPC_current('{1}')>".format(self.subclass_id)

class App_CPC_current(ApplicationBase):
    __tablename__ = "cpc_current"
    uuid = Column(Unicode(36), primary_key=True)
    application_id = Column(Unicode(20), ForeignKey("application.id"))
    section_id = Column(Unicode(10))
    subsection_id = Column(Unicode(20), ForeignKey("cpc_subsection.id"))
    group_id = Column(Unicode(20), ForeignKey("cpc_group.id"))
    subgroup_id = Column(Unicode(20), ForeignKey("cpc_subgroup.id"))
    category = Column(Unicode(36))
    sequence = Column(Integer, index=True)

    def __repr__(self):
        return "<CPC_current('{1}')>".format(self.subgroup_id)
    

class App_MainClass(ApplicationBase):
    __tablename__ = "mainclass"
    id = Column(Unicode(20), primary_key=True)
    #title = Column(Unicode(256))
    #text = Column(Unicode(256))
    uspc = relationship("App_USPC", backref="mainclass")

    def __repr__(self):
        return "<MainClass('{0}')>".format(self.id)


class App_MainClass_current(ApplicationBase):
    __tablename__ = "mainclass_current"
    id = Column(Unicode(20), primary_key=True)
    title = Column(Unicode(256))
    #text = Column(Unicode(256))
    uspc = relationship("App_USPC_current", backref="mainclass_current")

    def __repr__(self):
        return "<MainClass_current('{0}')>".format(self.id)


class App_SubClass(ApplicationBase):
    __tablename__ = "subclass"
    id = Column(Unicode(20), primary_key=True)
    #title = Column(Unicode(256))
    #text = Column(Unicode(256))
    uspc = relationship("App_USPC", backref="subclass")

    def __repr__(self):
        return "<SubClass('{0}')>".format(self.id)


class App_SubClass_current(ApplicationBase):
    __tablename__ = "subclass_current"
    id = Column(Unicode(20), primary_key=True)
    title = Column(Unicode(512))
    #text = Column(Unicode(256))
    uspc = relationship("App_USPC_current", backref="subclass_current")

    def __repr__(self):
        return "<SubClass_current('{0}')>".format(self.id)

class App_CPC_subsection(ApplicationBase):
    __tablename__ = "cpc_subsection"
    id = Column(Unicode(20), primary_key=True)
    title = Column(Unicode(256))
    #text = Column(Unicode(256))
    cpc_current = relationship("App_CPC_current", backref="cpc_subsection")

    def __repr__(self):
        return "<CPC_subsection('{0}')>".format(self.id)

class AppCPC_group(ApplicationBase):
    __tablename__ = "cpc_group"
    id = Column(Unicode(20), primary_key=True)
    title = Column(Unicode(256))
    #text = Column(Unicode(256))
    cpc_current = relationship("App_CPC_current", backref="cpc_group")

    def __repr__(self):
        return "<CPC_group('{0}')>".format(self.id)

class App_CPC_subgroup(ApplicationBase):
    __tablename__ = "cpc_subgroup"
    id = Column(Unicode(20), primary_key=True)
    title = Column(Unicode(512))
    #text = Column(Unicode(256))
    cpc_current = relationship("App_CPC_current", backref="cpc_subgroup")

    def __repr__(self):
        return "<CPC_subgroup('{0}')>".format(self.id)

# REFERENCES -----------------------

class App_Claim(ApplicationBase):
    __tablename__ = "claim"
    uuid = Column(Unicode(36), primary_key=True)
    application_id = Column(Unicode(20), ForeignKey('application.id'))
    text = deferred(Column(UnicodeText))
    dependent = Column(Integer) # if -1, independent
    sequence = Column(Integer, index=True)

    def __repr__(self):
        return "<Claim('{0}')>".format(self.text)
