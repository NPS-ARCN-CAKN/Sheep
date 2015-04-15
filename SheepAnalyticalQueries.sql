-- Some sheep monitoring queries to summarize data for analysis
USE ARCN_Sheep
set nocount on



--print 'Composition counts by unit'
SELECT  * FROM (
SELECT Surveys.Survey, Transect_or_Unit_Information.LegacyUnitID AS UnitName, SUM(Animals.Ewes) AS Ewes, SUM(Animals.EweLike) AS EweLike, SUM(Animals.Lambs) AS Lambs, 
                      SUM(Animals.Rams_LessThanFullCurl) AS Rams_LessThanFullCurl, SUM(Animals.Rams_FullCurl) AS Rams_FullCurl, SUM(Animals.UnclassifiedRams) 
                      AS UnclassifiedRams, SUM(Animals.UnclassifiedSheep) AS UnclassifiedSheep, SUM(Animals.Yearlings) AS Yearlings, SUM(Animals.Rams1_2Curl) AS Rams1_2Curl, 
                      SUM(Animals.Rams3_4Curl) AS Rams3_4Curl, SUM(Animals.Rams7_8Curl) AS Rams7_8Curl, SUM(Animals.Rams1_4Curl) AS Rams1_4Curl, 
                      SUM(Animals.Rams_GT_7_8Curl) AS Rams_GT_7_8Curl
FROM         Surveys INNER JOIN
                      Transect_or_Unit_Information ON Surveys.SurveyID = Transect_or_Unit_Information.SurveyID LEFT OUTER JOIN
                      Animals ON Transect_or_Unit_Information.TransectID = Animals.TransectID
GROUP BY Transect_or_Unit_Information.LegacyUnitID, Surveys.Survey
) as CompCounts
join 
 (select LegacyUnitID, ARCNSubUnitName,PolygonFeature FROM LegacyUnits) as Units
 on Units.LegacyUnitID = CompCounts.UnitName



--SELECT * FROM (
--SELECT     Transect_or_Unit_Information.LegacyUnitID AS UnitName, SUM(Animals.Ewes) AS Ewes, SUM(Animals.EweLike) AS EweLike, SUM(Animals.Lambs) AS Lambs, 
--                      SUM(Animals.Rams_LessThanFullCurl) AS Rams_LessThanFullCurl, SUM(Animals.Rams_FullCurl) AS Rams_FullCurl, SUM(Animals.UnclassifiedRams) 
--                      AS UnclassifiedRams, SUM(Animals.UnclassifiedSheep) AS UnclassifiedSheep, SUM(Animals.Yearlings) AS Yearlings, SUM(Animals.Rams1_2Curl) AS Rams1_2Curl, 
--                      SUM(Animals.Rams3_4Curl) AS Rams3_4Curl, SUM(Animals.Rams7_8Curl) AS Rams7_8Curl, SUM(Animals.Rams1_4Curl) AS Rams1_4Curl, 
--                      SUM(Animals.Rams_GT_7_8Curl) AS Rams_GT_7_8Curl, Surveys.Survey
--FROM         Surveys INNER JOIN
--                      Transect_or_Unit_Information ON Surveys.SurveyID = Transect_or_Unit_Information.SurveyID LEFT OUTER JOIN
--                      Animals ON Transect_or_Unit_Information.TransectID = Animals.TransectID
--GROUP BY Transect_or_Unit_Information.LegacyUnitID, Surveys.Survey
--) as Units
-- join LegacyUnits on Legacyunits.LegacyUnitID = Units.UnitName