-- Script to perform quality control checks on the ARCN/CAKN sheep monitoring database
-- SDMiller April, 2015
use arcn_sheep
set nocount on

print 'ARCN/CAKN Sheep Monitoring Data Quality Check Report, ' 
print  convert(varchar(20),GETDATE()) + ' by ' + suser_name()
print ''
print 'Dataset_Information'
print '---------------------------------------------------------------------------'

print 'Surveys'
SELECT     Surveys.Survey as [Survey Name], COUNT(Transect_or_Unit_Information.TransectID) AS Transects, MIN(Transect_or_Unit_Information.FlownDate) AS Began, 
                      MAX(Transect_or_Unit_Information.FlownDate) AS Ended, COUNT(Transect_or_Unit_Information.LegacyUnitID) AS Units
FROM         Surveys INNER JOIN
                      Transect_or_Unit_Information ON Surveys.SurveyID = Transect_or_Unit_Information.SurveyID
GROUP BY Surveys.Survey
ORDER BY Surveys.Survey

print 'Transect_or_Unit_Information'
print '---------------------------------------------------------------------------'

print 'Null LegacyUnitID:'
select COUNT(transectid) as Count from Transect_or_Unit_Information where LegacyUnitID is null

print 'Null LegacyUnitID grouped by survey:'
SELECT     Surveys.Survey, COUNT(Transect_or_Unit_Information.TransectID) AS Count
FROM         Transect_or_Unit_Information INNER JOIN
                      Surveys ON Transect_or_Unit_Information.SurveyID = Surveys.SurveyID
WHERE     (Transect_or_Unit_Information.LegacyUnitID IS NULL)
GROUP BY Surveys.Survey