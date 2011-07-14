--
-- Table used to hold people eligible for the ePrint service.  This table is maintained by dirxml.
--

use eprint
GO

create table dbo.dirxml_people
(
	GUID varchar(64) primary key,
	DUKEID varchar(16),
	FIRST_NAME varchar(64),
	LAST_NAME varchar(64),
	NETID varchar(16),
	PRIMARY_AFFILIATION varchar(9) check (PRIMARY_AFFILIATION IN ('staff', 'faculty', 'emeritus', 'noncomp', 'alumni', 'student', 'affiliate')),
	ACAD_CAREER varchar(64),
	ACAD_PROG varchar(16),
	DUKECARD_NUMBER varchar(16),
	OU varchar(128),
	CREATETIMESTAMP datetime default getdate(),
	MODIFYTIMESTAMP datetime default getdate()
)


--
-- Table used to hold people who have become ineligible to use the ePrint service.
--
create table dbo.dirxml_deletions
(
	NETID varchar(16),
	CREATETIMESTAMP datetime default getdate()
)

--
-- Table used to hold runtime information of scheduled scripts.
--
create table dbo.import_runtimes
(
	id varchar(64) primary key,
	timestamp datetime
)


--
-- Table used to hold nonhuman cards that are eligible for ePrint.
--
create table dbo.nonhuman_people
(
	ID varchar(16) primary key,
	ALIAS varchar(64),
	DUKECARD_NUMBER varchar(16),
	CARD_TYPE_1 varchar(64),
	CARD_TYPE_2 varchar(64),
	CREATETIMESTAMP datetime default getdate(),
	MODIFYTIMESTAMP datetime default getdate()
)

--
-- Table used to hold nonhuman card deletions.
--
create table dbo.nonhuman_deletions
(
	ID varchar(16),
	CREATETIMESTAMP datetime default getdate()
)


--
-- Adds the default runtimes.
--
INSERT INTO dbo.import_runtimes (id, timestamp) values ('updates', 0);
INSERT INTO dbo.import_runtimes (id, timestamp) values ('deletes', 0);

GO

--
-- Trigger for updates on the dirxml_people table.  This trigger updates the modifytimestamp
-- when an entry changes and adds old netids to the dirxml_deletions table.  Note that the 
-- select statement is used to avoid dirxml from getting an update count of 0.
--
CREATE TRIGGER dbo.trig_dirxml_people_updates
ON dbo.dirxml_people
FOR UPDATE
AS
IF NOT UPDATE(modifytimestamp) AND NOT UPDATE(createtimestamp)
	UPDATE dbo.dirxml_people SET modifytimestamp=getdate() WHERE guid IN (SELECT guid FROM inserted)

IF UPDATE(netid)
	INSERT INTO dbo.dirxml_deletions (netid) select netid from deleted

IF (SELECT count(*) FROM dbo.dirxml_deletions where netid IN (select netid from inserted)) > 0
	DELETE FROM dbo.dirxml_deletions where netid IN (select netid from inserted)

GO

--
-- Trigger for deletes on the dirxml_people table.
--
CREATE TRIGGER dbo.trig_dirxml_people_deletes
ON dbo.dirxml_people
FOR DELETE
AS
INSERT INTO dbo.dirxml_deletions (netid) select netid from deleted

GO

--
-- Trigger for inserts on the dirxml_people table.  Note that the select statement is used to avoid
-- dirxml from getting an update count of 0.
--
CREATE TRIGGER dbo.trig_dirxml_people_inserts
ON dbo.dirxml_people
FOR INSERT
AS
IF (SELECT count(*) FROM dbo.dirxml_deletions where netid IN (select netid from inserted)) > 0
	DELETE FROM dbo.dirxml_deletions where netid IN (select netid from inserted)

GO

--
-- Instead Of trigger on the dirxml_deletions table.  When an entry is added here, this trigger verifies
-- that the netid does not exist in the dirxml_people table.  Two entries with the same netid can exist
-- in the dirxml_people table during a consolidation.  Note that the select statement is used to avoid 
-- dirxml from getting an update count of 0.
--
CREATE TRIGGER dbo.trig_dirxml_deletions_inserts
ON dbo.dirxml_deletions
INSTEAD OF INSERT
AS
IF (SELECT count(*) from dbo.dirxml_people where netid IN (select netid from inserted)) = 0
	INSERT INTO dbo.dirxml_deletions (netid) select netid from inserted

GO

--
-- Trigger for updates on the nonhuman_people table.  This trigger updates the modifytimestamp when
-- an entry is changed.
--
CREATE TRIGGER dbo.trig_nonhuman_people_updates
ON dbo.nonhuman_people
FOR UPDATE
AS
IF UPDATE(ID)
BEGIN
	RAISERROR('Updating the ID field is not supported.  You should delete the existing entry and add a new one.', 16, 1)
	ROLLBACK TRANSACTION
	RETURN
END

IF NOT UPDATE(modifytimestamp) AND NOT UPDATE(createtimestamp)
	UPDATE dbo.nonhuman_people SET modifytimestamp=getdate() WHERE id IN (SELECT id FROM inserted)

GO

--
-- Trigger for deletes on the nonhuman_people table.
--
CREATE TRIGGER dbo.trig_nonhuman_people_deletes
ON dbo.nonhuman_people
FOR DELETE
AS
INSERT INTO dbo.nonhuman_deletions (id) select id from deleted

GO

--
-- Trigger for inserts on the nonhuman_people table.
--
CREATE TRIGGER dbo.trig_nonhuman_people_inserts
ON dbo.nonhuman_people
FOR INSERT
AS
IF (SELECT count(*) FROM dbo.nonhuman_deletions where id IN (select id from inserted)) > 0
	DELETE FROM dbo.nonhuman_deletions where id IN (select id from inserted)

GO


