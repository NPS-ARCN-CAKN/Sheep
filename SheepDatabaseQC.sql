-- Script to perform quality control checks on the ARCN/CAKN sheep monitoring database
-- SDMiller April, 2015
use arcn_sheep
set nocount on

declare @uline  varchar(200)
set @uline = '---------------------------------------------------------------------------'

print 'ARCN/CAKN Sheep Monitoring Data Quality Check Report, ' 
print  convert(varchar(20),GETDATE()) + ' by ' + suser_name()
print ''
print 'Section 1. Dataset_Information'
print @uline

print 'Surveys'

SELECT 'Number of Survey Campaigns: ', COUNT(SurveyID) from Surveys

SELECT     Surveys.Survey as [Survey Name], COUNT(Transect_or_Unit_Information.TransectID) AS Transects, MIN(Transect_or_Unit_Information.FlownDate) AS Began, 
                      MAX(Transect_or_Unit_Information.FlownDate) AS Ended, COUNT(Transect_or_Unit_Information.LegacyUnitID) AS [Count of Units]
FROM         Surveys INNER JOIN
                      Transect_or_Unit_Information ON Surveys.SurveyID = Transect_or_Unit_Information.SurveyID
GROUP BY Surveys.Survey
ORDER BY Surveys.Survey

print 'Survey Campaign Dates'
SELECT  Distinct  Survey, StartDate
FROM         Surveys

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

