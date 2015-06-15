-- Script to perform quality control checks on the ARCN/CAKN sheep monitoring database
-- SDMiller April, 2015
use arcn_sheep
set nocount on

declare @uline  varchar(200)
set @uline = '---------------------------------------------------------------------------'

print 'NPS Arctic and Central Alaska Inventory and Monitoring Networks Sheep Monitoring Data Quality Check Report, ' 
print  convert(varchar(20),GETDATE()) + ' by ' + suser_name()
print ''
print 'Section 1. Dataset_Information'
print @uline





print 'Surveys'
SELECT 'Number of survey campaigns: ', COUNT(SurveyID) from Surveys

SELECT     Surveys.Survey AS [Survey Name], COUNT(Transect_or_Unit_Information.TransectID) AS Transects, MIN(Transect_or_Unit_Information.FlownDate) AS Began, 
                      MAX(Transect_or_Unit_Information.FlownDate) AS Ended, COUNT(Transect_or_Unit_Information.LegacyUnitID) AS [Count of Units]
FROM         Surveys LEFT OUTER JOIN
                      Transect_or_Unit_Information ON Surveys.SurveyID = Transect_or_Unit_Information.SurveyID
GROUP BY Surveys.Survey
ORDER BY [Survey Name]

print 'Survey Campaign Dates'
SELECT  Survey, StartDate, EndDate
FROM         Surveys ORDER BY StartDate

print 'Units'
SELECT 'Number of survey units: ', COUNT(LegacyUnitID) from LegacyUnits

print 'Data import status (accounting of records imported from each layer of NPS.gdb)'
	
	print 'Transects (NPS.gdb\TrnOrig)'
	SELECT     TOP (100) PERCENT Surveys.Survey, COUNT(Transect_or_Unit_Information.TransectID) AS Transects
	FROM         Surveys LEFT OUTER JOIN
						  Transect_or_Unit_Information ON Surveys.SurveyID = Transect_or_Unit_Information.SurveyID
	GROUP BY Surveys.Survey
	ORDER BY Surveys.Survey
	
	print 'Transect points (NPS.gdb\TrnPoints)'
	SELECT     TOP (100) PERCENT Surveys.Survey, COUNT(TransectPoints.TransectPointID) AS TransectPoints
	FROM         Surveys LEFT OUTER JOIN
						  TransectPoints ON Surveys.SurveyID = TransectPoints.SurveyID
	GROUP BY Surveys.Survey
	ORDER BY Surveys.Survey
	
	print 'Sheep groups (NPS.gdb\Animals)'
	SELECT     TOP (100) PERCENT Surveys.Survey, COUNT(Animals.SheepGroupID) AS SheepGroups
	FROM         Surveys LEFT OUTER JOIN
						  Transect_or_Unit_Information ON Surveys.SurveyID = Transect_or_Unit_Information.SurveyID left outer JOIN
						  Animals ON Transect_or_Unit_Information.TransectID = Animals.TransectID
	GROUP BY Surveys.Survey
	ORDER BY Surveys.Survey

	print 'Buffers (NPS.gdb\Buffers)'
	SELECT     Surveys.Survey, COUNT(Buffers.BufferID) AS Buffers
	FROM         Buffers INNER JOIN
						  Transect_or_Unit_Information ON Buffers.TransectID = Transect_or_Unit_Information.TransectID RIGHT OUTER JOIN
						  Surveys ON Transect_or_Unit_Information.SurveyID = Surveys.SurveyID
	GROUP BY Surveys.Survey
	ORDER BY Surveys.Survey
	
	print 'Flat areas (NPS.gdb\FlatAreas)'
	SELECT     Surveys.Survey, COUNT(FlatAreas.FlatAreaID) AS FlatAreas
	FROM         Surveys LEFT OUTER JOIN
						  FlatAreas ON Surveys.SurveyID = FlatAreas.SurveyID
	GROUP BY Surveys.Survey
	ORDER BY Surveys.Survey

	print 'Track points (GPS Tracklog points)'
	SELECT     Surveys.Survey, COUNT(GPSTracks.TrackPointID) AS TrackPoints
	FROM         Surveys LEFT OUTER JOIN
						  GPSTracks ON Surveys.SurveyID = GPSTracks.SurveyID
	GROUP BY Surveys.Survey
	ORDER BY Surveys.Survey

	print 'Pilot waypoints'
	SELECT     Surveys.Survey, COUNT(PilotWaypoints.WaypointID) AS PilotWaypoints
	FROM         Surveys LEFT OUTER JOIN
						  PilotWaypoints ON Surveys.SurveyID = PilotWaypoints.SurveyID
	GROUP BY Surveys.Survey
	ORDER BY Surveys.Survey

	print 'Tracklogs (NPS.gdb\Tracklog)'
	SELECT     Surveys.Survey, COUNT(TransectTracklog.TransectTracklogID) AS Tracklogs
	FROM         TransectTracklog INNER JOIN
						  Transect_or_Unit_Information ON TransectTracklog.TransectID = Transect_or_Unit_Information.TransectID RIGHT OUTER JOIN
						  Surveys ON Transect_or_Unit_Information.SurveyID = Surveys.SurveyID
	GROUP BY Surveys.Survey
	ORDER BY Surveys.Survey
	
print 'Transects and Flights'
SELECT DISTINCT 
                      Surveys.Survey, Transect_or_Unit_Information.FlownDate, Transect_or_Unit_Information.Aircraft, Transect_or_Unit_Information.LegacyUnitID, 
                      Transect_or_Unit_Information.PilotName, Transect_or_Unit_Information.ObserverName1
FROM         Surveys INNER JOIN
                      Transect_or_Unit_Information ON Surveys.SurveyID = Transect_or_Unit_Information.SurveyID
ORDER BY Survey, Transect_or_Unit_Information.FlownDate, Transect_or_Unit_Information.Aircraft








print 'Problems'
print @uline


print 'Transects with NULL LegacyUnitID'

SELECT     Surveys.Survey, Transect_or_Unit_Information.Aircraft, Transect_or_Unit_Information.PilotName, Transect_or_Unit_Information.ObserverName1, 
                      Transect_or_Unit_Information.FlownDate, Transect_or_Unit_Information.LegacyUnitID
FROM         Transect_or_Unit_Information INNER JOIN
                      Surveys ON Transect_or_Unit_Information.SurveyID = Surveys.SurveyID
WHERE     (Transect_or_Unit_Information.LegacyUnitID IS NULL)
ORDER BY Surveys.Survey, Transect_or_Unit_Information.LegacyUnitID

print 'Out of bounds animal groups'
print 'TO BE WRITTEN'