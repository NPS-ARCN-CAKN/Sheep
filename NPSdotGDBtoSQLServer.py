# NPSdotGDBtoSQLServer
# Purpose: A Python script used by the National Park Service Arctic and Central Alaska Networks to export aerial sheep survey
# field data from an ESRI geodatabase (NPS.gdb) into a series of SQL scripts containing insert queries.  The
# generated scripts can be executed against the main sheep monitoring Sql Server database (ARCN_Sheep) to import the field data.

# This Python script loops through the records in the various layers contained in NPS.gdb and writes an SQL insert query
# for each record.  Each insert query is sequentially appended to an output file with the name of the geodatabase layer it came from.
# For instance, the insert queries exported from the geodatabase layer 'Animals' will be written to an SQL output file 'Animals.sql'
# When executed, the insert queries will insert a record in the appropriate database table.

# Notes on using the script:
# This script does not interact with the ARCN_Sheep database in any way; it just exports .sql scripts, so there is
# no danger of database corruption to test-running the script.
# The script is designed to be run via an ArcGIS toolbox tool.
# Ensure the column mappings from the geodatabase to Sql Server tables are correct.
# Python requires forward slashes for directory delimiters contrary to Windows.  Replace '\' with '/' in any paths.
# IMPORTANT NOTE: The SQL insert queries are wrapped in unclosed transaction statements (the transaction is started,
# but not finished with either a COMMIT or ROLLBACK statement).  The transaction ensures that either all the records
# are inserted or none of the records are inserted (e.g. if any query fails then they all fail). You must COMMIT or
# ROLLBACK the transaction after running the script or the database will be left in a locked state.

# Written by Scott D. Miller, Data Manager, Arctic and Central Alaska Inventory and Monitoring Networks, October, 2014

# import the arcpy library
import arcpy
import os
import sys

# USER MUST SUPPLY THE VARIABLES BELOW --------------------------------------------

# Supply a path to the .mxd containing NPS.gdb workspace
NPSdotGdbMxd = arcpy.GetParameterAsText(0)

# Supply a directory to output the sql scripts to, the scripts will be named according to the layer they came from
sqlscriptpath = os.path.dirname(NPSdotGdbMxd) + '/'

# Supply the SurveyID from the Surveys table of the ARCN_Sheep database for this survey campaign.
# e.g. the Itkillik 2011 Survey's SurveyID is '1AC66891-5D1E-4749-B962-40AB1BCA577F'
# Contact the Network data manager for this value
SurveyID = arcpy.GetParameterAsText(1)
# -----------------------------------------------------------------------------

# echo the parameters
arcpy.AddMessage("Input geodatabase: " + NPSdotGdbMxd)
arcpy.AddMessage("Output directory: " + sqlscriptpath)
arcpy.AddMessage("SurveyID: " + SurveyID)


# spatial coordinate system
# the data in the output sql script will be in the reference system indicated below
epsg = 4326 # EPSG SRS code for WGS84
sr = arcpy.SpatialReference(epsg)

# gather some metadata to put in the sql scripts
# current time
import time
ow = time.strftime("%c") # date and time
executiontime = time.strftime("%c")

# username
import getpass
user = getpass.getuser()


# function fixArcGISNullString
# accepts: str, String to process. quote, Boolean, whether to surround the returned string with single quotes, nullToZero,
# whether to convert nulls to zeroes
# returns: String
# purpose: ArcGIS is all over the place with null values, sometimes returning blank strings, other times 'None' or '<Null>'
# These values won't work for SQL so we need look at the value of str and determine if it should be a NULL or not.
# If it's OK then surround with single quotes and return, otherwise return unquoted NULL.
def fixArcGISNull(inputString, quoted, nullToZero):
    "function fixArcGISNullString + \
    accepts: str, String to process. quote, Boolean, whether to surround the returned string with single quotes + \
    returns: String  + \
    purpose: ArcGIS is all over the place with null values, sometimes returning blank strings, other times 'None' or '<Null>'  + \
    These values won't work for SQL so we need look at the value of str and determine if it should be a NULL or not. + \
    If it's OK then surround with single quotes and return, otherwise return unquoted NULL." + "\'"

    # first replace single quotes in the string with '' so the quote doesn't foul up the SQL
    inputString = str(inputString).strip().replace("'", "''")
    # fix the nulls
    if inputString == "None" or inputString == "<Null>" or inputString == "NULL" or inputString == "": newStr = "NULL"
    else:
        if quoted == False: newStr = inputString
        else: newStr = "\'" + inputString + "\'"

    # some strings should return a zero instead of null, if so change the NULL to a zero
    if (newStr == "NULL") and (nullToZero == True):
        newStr = "0"

    # return the processed string
    return newStr





# EXPORT THE TRANSECTS ------------------------------------------------------------------------------------------------------------
def GenerateTrnOrigSQLScript(SurveyID):
    layer = "TrnOrig"
    fc = NPSdotGdbMxd + "/" + layer
    file = open(sqlscriptpath +  layer + ".sql", "w")
    arcpy.AddMessage('Processing ' + layer + "...")

    # write some metadata to the sql script
    file.write("-- Insert queries to transfer data from ARCN Sheep monitoring field geodatabase " + NPSdotGdbMxd + " into ARCN_Sheep database\n")
    file.write("-- File generated " + executiontime + " by " + user + "\n")
    file.write("-- If this file is too big to run in Sql Server Management Studio then run from a Windows Power Shell prompt:\n")
    file.write("-- sqlcmd /S SERVER\INSTANCE /i \"" + str(file.name) + "\"\n")
    file.write("USE ARCN_Sheep \n")
    file.write("BEGIN TRANSACTION -- Do not forget to COMMIT or ROLLBACK the changes after executing or the database will be in a locked state \n")
    file.write("SET QUOTED_IDENTIFIER ON\n\n")
    file.write("\n-- insert the generated transects from " + layer + " -----------------------------------------------------------\n")
    file.write("DECLARE @SurveyID nvarchar(50) -- SurveyID of the record in the Surveys table to which the transects below will be related\n")
    file.write("SET @SurveyID = '" + SurveyID + "'\n")

    # we'll need to create a searchcursor a little further on to access the records in the layer.  the cursor has a fields parameter
    # we could just submit a * to gather all columns except that we need the Shape column returned as a token, e.g. Shape@; see ArcGIS documentation,
    # so we have to submit all the columns as a list with Shape changed to the Shape@ token that will allow us to get at geometry info.
    # The easiest way to do this is to loop through the column names and load them into a list making our edits as needed
    fieldsList = arcpy.ListFields(fc) #get the fields
    fields = [] # create an empty list
    #  loop through the fields and change the Shape column (containing geometry) into a token, add columns to the list
    for field in fieldsList:
        if field.name == "Shape":
            fields.append("Shape@")
        elif field.name == "SHAPE":
            fields.append("SHAPE@")
        else:
            fields.append(field.name)

    # get the transect data into a cursor so we can translate it into sql to insert into the sheep sql server database
    # loop through the cursor and save fields as variables to be used later in insert queries
    cursor = arcpy.da.SearchCursor(fc,fields,"",sr)

    # Sometimes the columns change places.  The code below will output the column names and order numbers to
    # standard output.  You can then copy them back into the script to get the variable names synchronized with
    # the column positions.
    # i = 0
    # for field in fieldsList:
    #     arcpy.AddMessage(field.name + " = row[" + str(i) + "]")
    #     i = i + 1

    for row in cursor:
        OBJECTID_1 = row[0]
        Shape = row[1]
        if Shape == "None" or Shape == "<Null>" or Shape == "NULL" or Shape == "":
            sys.exit('ERROR: Script execution aborted at row ' + str(row) + '. Shape is required but is NULL')
        PT_ID = row[3]
        PROJTD_X1 = row[4]
        PROJTD_Y1 = row[5]
        PROJTD_X2 = row[6]
        PROJTD_Y2 = row[7]
        PROJECTION = row[8]
        DD_LATSEED = row[9]
        DD_LONGSEE = row[10]
        DD_LAT1 = row[11]
        DD_LONG1 = row[12]
        DD_LAT2 = row[13]
        DD_LONG2 = row[14]
        DM_LATSEED = row[15]
        DM_LONGSEE = row[16]
        DM_LAT1 = row[17]
        DM_LONG1 = row[18]
        DM_LAT2 = row[19]
        DM_LONG2 = row[20]
        LENGTH_MTR = row[21]
        ELEV_M = row[22]
        ELEVFT = row[23]
        CNTR_NOTE = row[24]
        LABEL_M = row[25]
        LABEL_FT = row[26]
        SurveyID = row[27]
        BATCH_ID = row[28]
        Flown = row[29]
        TransectID = row[30]
        Aircraft = row[31]
        OBSLNAM1 = row[32]
        OBSLNAM2 = row[33]
        FLOWNDATE = row[34]
        PILOTLNAM = row[35]
        CLOUDCOVER = row[36]
        PRECIP = row[37]
        TURBINT = row[38]
        TURBDUR = row[39]
        TEMPRTURE = row[40]
        TARGETLEN = row[41]
        Shape_Length = row[42]
        BuffArea_m = row[43]
        WBairds_Y_N = row[44]
        DeLongs_Y_N = row[45]
        Area2014_YN = row[46]
        HasSheep = row[47]
        EffLength_m = row[48]

        # we need to insert the feature into sql server as a geography item via the Well-Known Text representation of the feature
        if Shape.WKT == "None" or Shape.WKT == "<Null>" or Shape.WKT == "NULL" or Shape.WKT == "":
            errormessage = 'ERROR: Script execution aborted at row ' + str(row) + '. Shape.WKT is required but is NULL'
            arcpy.AddMessage(errormessage)
            sys.exit(errormessage)

        # build an insert query
        insertquery = "INSERT INTO [ARCN_Sheep].[dbo].[Transect_or_Unit_Information](" + \
        "SurveyID" + \
        ",Elevation_M" + \
        ",Aircraft" + \
        ",ObserverName1" + \
        ",ObserverName2" + \
        ",PilotName" + \
        ",Precipitation" + \
        ",TurbulenceIntensity" + \
        ",TurbulenceDuration" + \
        ",Temperature" + \
        ",TargetLength" + \
        ",Notes" + \
        ",GeneratedTransectID" + \
        ",FlownDate" + \
        ",Flown" + \
        ",CenterPoint" + \
        ",GeneratedTransect" + \
        ")" + \
        "VALUES(" + \
        "@SurveyID" + \
        "," + fixArcGISNull(ELEV_M,False, False) + \
        "," + fixArcGISNull(Aircraft,True, False) + \
        "," + fixArcGISNull(OBSLNAM1,True, False) + \
        "," + fixArcGISNull(OBSLNAM2,True, False) + \
        "," + fixArcGISNull(PILOTLNAM,True, False) + \
        "," + fixArcGISNull(PRECIP,True, False) + \
        "," + fixArcGISNull(TURBINT,True, False) + \
        "," + fixArcGISNull(TURBDUR,True, False) + \
        "," + fixArcGISNull(TEMPRTURE,False, False) + \
        "," + fixArcGISNull(TARGETLEN,False, False) + \
        "," + fixArcGISNull(CNTR_NOTE,True, False) + \
        "," + fixArcGISNull(TransectID,True, False) + \
        "," + fixArcGISNull(FLOWNDATE,True, False) + \
        "," + fixArcGISNull(Flown,True, False) + \
        ", geography::STPointFromText('POINT(" + str(DD_LONG1) + " " + str(DD_LAT1) + " " + str(ELEV_M) + ")', " + str(epsg) + ")" +  \
        ", geography::STGeomFromText('" + str(Shape.WKT) + "', " + str(epsg) + ")" + \
        ");\n"

        file.write(insertquery) # write the query to the output file
    #  close the output file
    file.write("\n-- Do not forget to COMMIT or ROLLBACK the changes after executing or the database will be in a locked state \n")
    file.close()
    arcpy.AddMessage('Done')










# EXPORT THE TRANSECT POINTS ------------------------------------------------------------------------------------------------------------
def GenerateTrnPointsSQLScript(SurveyID):
    layer = "TrnPoints"
    fc = NPSdotGdbMxd + "/" + layer
    file = open(sqlscriptpath +  layer + ".sql", "w")
    arcpy.AddMessage('Processing ' + layer + "...")

    # write some metadata to the sql script
    file.write("-- Insert queries to transfer data from ARCN Sheep monitoring field geodatabase " + NPSdotGdbMxd + " into ARCN_Sheep database\n")
    file.write("-- File generated " + executiontime + " by " + user + "\n")
    file.write("-- If this file is too big to run in Sql Server Management Studio then run from a Windows Power Shell prompt:\n")
    file.write("-- sqlcmd /S SERVER\INSTANCE /i \"" + str(file.name) + "\"\n")
    file.write("USE ARCN_Sheep \n")
    file.write("BEGIN TRANSACTION -- Do not forget to COMMIT or ROLLBACK the changes after executing or the database will be in a locked state \n")
    file.write("SET QUOTED_IDENTIFIER ON\n\n")
    file.write("\n-- insert the generated transects from " + layer + " -----------------------------------------------------------\n")
    file.write("DECLARE @SurveyID nvarchar(50) -- SurveyID of the record in the Surveys table to which the transects below will be related\n")
    file.write("SET @SurveyID = '" + str(SurveyID) + "'\n")

    # we'll need to create a searchcursor a little further on to access the records in the layer.  the cursor has a fields parameter
    # we could just submit a * to gather all columns except that we need the Shape column returned as a token, e.g. Shape@; see ArcGIS documentation,
    # so we have to submit all the columns as a list with Shape changed to the Shape@ token that will allow us to get at geometry info.
    # The easiest way to do this is to loop through the column names and load them into a list making our edits as needed
    fieldsList = arcpy.ListFields(fc) #get the fields
    fields = [] # create an empty list
    #  loop through the fields and change the Shape column (containing geometry) into a token, add columns to the list
    for field in fieldsList:
        if field.name == "Shape":
            fields.append("Shape@")
        elif field.name == "SHAPE":
            fields.append("SHAPE@")
        else:
            fields.append(field.name)

    # get the transect data into a cursor so we can translate it into sql to insert into the sheep sql server database
    # loop through the cursor and save fields as variables to be used later in insert queries
    cursor = arcpy.da.SearchCursor(fc,fields,"",sr)
    for row in cursor:
        OBJECTID_1 = row[0]
        Shape = row[1]
        OBJECTID = row[2]
        ELEV_FT = row[3]
        ELEV_M = row[4]
        PROJTD_X = row[5]
        PROJTD_Y = row[6]
        PROJECTION = row[7]
        PTDD_LAT = row[8]
        PTDD_LONG = row[9]
        PTDM_LAT = row[10]
        PTDM_LONG = row[11]
        GeneratedSurveyID = row[12]
        BATCH_ID = row[13]
        HASTRANS = row[14]

        # convert Y/N to bit
        if HASTRANS == 'Y':
            HASTRANS = 1
        else:
            HASTRANS = 0

        # build an insert query
        insertquery = "INSERT INTO [ARCN_Sheep].[dbo].[TransectPoints](" + \
            "[SurveyID]," + \
            "[Elev_M]," + \
            "[HasTransect]," + \
            "[GeneratedSurveyID]," + \
            "[TransectPoint]" + \
        ")" + \
        "VALUES(" + \
        "@SurveyID" + \
        "," + fixArcGISNull(ELEV_M,False, False) + \
        "," + fixArcGISNull(HASTRANS,False, True) + \
        "," + fixArcGISNull(GeneratedSurveyID,True, False) + \
        ",geography::STGeomFromText('" + Shape.WKT + "', " + str(epsg) + ")" + \
        ");\n"

        file.write(insertquery) # write the query to the output file
    #  close the output file
    file.write("\n-- Do not forget to COMMIT or ROLLBACK the changes after executing or the database will be in a locked state \n")
    file.close()
    arcpy.AddMessage('Done')














# ANIMALS - ------------------------------------------------------------------------------------------------------------
def GenerateAnimalsSQLScript(SurveyID):
    layer = "Animals"
    fc = NPSdotGdbMxd + "/" + layer
    file = open(sqlscriptpath +  layer + ".sql", "w")
    arcpy.AddMessage('Processing ' + layer + "...")

    # write some metadata to the sql script
    file.write("-- Insert queries to transfer data from ARCN Sheep monitoring field geodatabase " + NPSdotGdbMxd + " into ARCN_Sheep database\n")
    file.write("-- File generated " + executiontime + " by " + user + "\n")
    file.write("-- If this file is too big to run in Sql Server Management Studio then run from a Windows Power Shell prompt:\n")
    file.write("-- sqlcmd /S SERVER\INSTANCE /i \"" + str(file.name) + "\"\n")
    file.write("USE ARCN_Sheep \n")
    file.write("BEGIN TRANSACTION -- Do not forget to COMMIT or ROLLBACK the changes after executing or the database will be in a locked state \n")
    file.write("SET QUOTED_IDENTIFIER ON\n\n")
    file.write("\n-- insert the animals from " + layer + " -----------------------------------------------------------\n")

    # make a fields list for the layer
    fieldsList = arcpy.ListFields(fc) #get the fields
    fields = [] # create an empty list
    #  loop through the fields and change the Shape column (containing geometry) into a token, add columns to the list
    for field in fieldsList:
        if field.name == "Shape":
            fields.append("Shape@")
        else:
            fields.append(field.name)

    # get the data into a cursor so we can translate it into sql to insert into the sheep sql server database
    # loop through the cursor and save fields as variables to be used later in insert queries
    cursor = arcpy.da.SearchCursor(fc,fields,"",sr)
    for row in cursor:
        OBJECTID_1 = row[0]
        Shape = row[1]
        OBJECTID = row[2]
        SPECIES = row[3]
        GROUPSIZE = row[4]
        ACTIVITY = row[5]
        PCTCOVER = row[6]
        PCTSNOW = row[7]
        DATE_ = row[8]
        ALTITUDE = float(fixArcGISNull(str(row[9]), False, True))
        ALTITUDE = float(ALTITUDE) * 0.3048 # silly units to standard units
        LATITUDE = row[10]
        LONGITUDE = row[11]
        XCOORD = row[12]
        YCOORD = row[13]
        PDOP = row[14]
        PLANESPD = float(fixArcGISNull(str(row[15]), False, True))
        TIME_ = row[16]
        GeneratedSurveyID = row[17]
        TransectID = row[18]
        SegmentID = row[19]
        DIST2TRANS = row[20]
        AnimalID = row[21]
        OBS1LNAM = row[22]
        OBS1DIR = row[23]
        OBS1REPT = row[24]
        OBS2LNAM = row[25]
        OBS2DIR = row[26]
        OBS2REPT = row[27]
        PILOTLNAM = row[28]
        PILOTDIR = row[29]
        PILOTREPT = row[30]
        HorizonID = row[31]
        GROUPTYPE = row[32]
        LT_FCRAMS = row[33]
        GTE_FCRAMS = row[34]
        UNCLSSRAMS = row[35]
        UNCLSSHEEP = row[36]
        LAMBS = row[37]
        EWELIKE = row[38]
        TOTAL = row[39]
        ANIMALTYPE = row[40]
        Year = row[41]
        SurveyName = row[42]
        Park = row[43]
        Comments = row[44]
        CURL_1_2 = row[45]
        CURL_3_4 = row[46]
        CURL_7_8 = row[47]
        LT_1_2CURL = row[48]
        YEARLING = row[49]
        EWES = row[50]
        FORMNAME = row[51]

        # build an insert query
        # NOTE: There is a database column Rams1_4Curl defined as 'Number of rams with horns equal to or greater than 1/4 curl but less than 1/2 curl. These must be differentiated from ewes. They are usually 2-3 years old.'
        # NPS.gdb however has no column matching the database column so it has been set to 0 below.
        insertquery = "INSERT INTO [ARCN_Sheep].[dbo].[Animals](" + \
            "[TransectID]" + \
            ",[PDOP]" + \
            ",[Speed]" + \
            ",[SampleDate]" + \
            ",[DistanceToTransect]" + \
            ",[Ewes]" + \
            ",[EweLike]" + \
            ",[Lambs]" + \
            ",[Rams_LessThanFullCurl]" + \
            ",[Rams_FullCurl]" + \
            ",[UnclassifiedRams]" + \
            ",[UnclassifiedSheep]" + \
            ",[Activity]" + \
            ",[PlaneAltitude]" + \
            ",[Yearlings]" + \
            ",[GroupNumber]" + \
            ",[Comments]" + \
            ",[LongOrShortForm]" + \
            ",[Rams1_2Curl]" + \
            ",[Rams3_4Curl]" + \
            ",[Rams7_8Curl]" + \
            ",[Rams1_4Curl]" + \
            ",[Rams_GT_7_8Curl]" + \
            ",[Location]" + \
            ")" + \
            "VALUES(" + \
            "(SELECT TransectID FROM Transect_or_Unit_Information WHERE (SurveyID = '" + str(SurveyID) + "') AND (GeneratedTransectID = " + str(TransectID) + "))" + \
            "," + fixArcGISNull(str(PDOP), False, False) + \
            "," + fixArcGISNull(str(PLANESPD), False, False) + \
            "," + fixArcGISNull(str(DATE_), True, False) + \
            "," + fixArcGISNull(str(DIST2TRANS), False, False) + \
            "," + fixArcGISNull(str(EWES), False, True) + \
            "," + fixArcGISNull(str(EWELIKE), False, True) + \
            "," + fixArcGISNull(str(LAMBS), False, True) + \
            "," + fixArcGISNull(str(LT_FCRAMS), False, True) + \
            "," + fixArcGISNull(str(GTE_FCRAMS), False, True) + \
            "," + fixArcGISNull(str(UNCLSSRAMS), False, True) + \
            "," + fixArcGISNull(str(UNCLSSHEEP), False, True) + \
            "," + fixArcGISNull(str(ACTIVITY), True, False) + \
            "," + fixArcGISNull(str(ALTITUDE), False, False) + \
            "," + fixArcGISNull(str(YEARLING), False, True) + \
            "," + fixArcGISNull(str(OBJECTID_1),False,False) + \
            "," + fixArcGISNull(str(Comments), True, False) + \
            "," + fixArcGISNull(str(FORMNAME),True, False) + \
            "," + fixArcGISNull(str(LT_1_2CURL), False, True) + \
            "," + fixArcGISNull(str(CURL_3_4), False, True) + \
            "," + fixArcGISNull(str(CURL_7_8), False, True) + \
            ", 0" + \
            "," + str(GTE_FCRAMS) + \
            ",geography::STPointFromText('" + str(Shape.WKT) + "', " + str(epsg) + ")" + \
            ");\n"

        file.write(insertquery) # write the query to the output .sql file

    #  close the output file
    file.write("\n-- Do not forget to COMMIT or ROLLBACK the changes after executing or the database will be in a locked state \n")
    file.close()
    arcpy.AddMessage('Done')



# # TRACKLOG ------------------------------------------------------------------------------------------------------------
# layer = "Tracklog"
# fc = NPSdotGdbMxd + "/" + layer
# file = open(sqlscriptpath +  layer + ".sql", "w")
# arcpy.AddMessage('Processing ' + layer + "...")
#
# # write some metadata to the sql script
# file.write("-- Insert queries to transfer data from ARCN Sheep monitoring field geodatabase " + NPSdotGdbMxd + " into ARCN_Sheep database\n")
# file.write("-- File generated " + executiontime + " by " + user + "\n")
# file.write("-- If this file is too big to run in Sql Server Management Studio then run from a Windows Power Shell prompt:\n")
# file.write("-- sqlcmd /S SERVER\INSTANCE /i \"" + str(file.name) + "\"\n")
# file.write("USE ARCN_Sheep \n")
# file.write("BEGIN TRANSACTION -- Do not forget to COMMIT or ROLLBACK the changes after executing or the database will be in a locked state \n")
# file.write("SET QUOTED_IDENTIFIER ON\n\n")
# file.write("\n-- insert the tracklog lines from " + layer + " -----------------------------------------------------------\n")
#
# fieldsList = arcpy.ListFields(fc) #get the fields
# fields = [] # create an empty list
# #  loop through the fields and change the Shape column (containing geometry) into a token, add columns to the list
# for field in fieldsList:
#     if field.name == "Shape":
#         fields.append("Shape@")
#     elif field.name == "SHAPE":
#         fields.append("SHAPE@")
#     else:
#         fields.append(field.name)
#
# # get the data into a cursor so we can translate it into sql to insert into the sheep sql server database
# # loop through the cursor and save fields as variables to be used later in insert queries
# cursor = arcpy.da.SearchCursor(fc,fields,"",sr)
# for row in cursor:
#     OBJECTID = row[0]
#     SHAPE = row[1]
#     GeneratedSurveyID = row[2]
#     SHAPE_Length = row[3]
#     TransectID = row[4]
#     # arcpad app provides choices that conflict with sql server constraint on SegType
#     # SegType must be either 'On Transect' or 'Off Transect', not "OnTransect" or "OffTransect" so fix it here
#     if row[5] == "OnTransect":
#         SegType = "On Transect"
#     elif row[5] == "OffTransect":
#         SegType = "Off Transect"
#     else:
#         SegType = row[5] # if it's not covered above then it's a disallowed value, let sql server constraint bomb so it's brought to light for fixing
#     SegmentID = row[6]
#     PilotLNam = row[7]
#     Obs1LNam = row[8]
#     Obs1Dir = row[9]
#     PilotDir = row[10]
#     Obs2Dir = row[11]
#     Obs2LNam = row[12]
#     Comments = row[13]
#
#     # a single quote in the string will booger up the sql query, replace with double single quote
#     if Comments is not None:
#         Comments = Comments.replace("'", "''")
#
#     if SHAPE is None:
#         insertquery = ''
#     else:
#         # build an insert query
#         insertquery = "INSERT INTO [ARCN_Sheep].[dbo].[TransectTracklog](" + \
#             "[TransectID]," + \
#             "[SegmentType]," + \
#             "[Observer1Direction]," + \
#             "[SegmentLine]," + \
#             "[Comments]" + \
#             ")" + \
#             "VALUES(" + \
#             "(SELECT TransectID FROM Transect_or_Unit_Information WHERE (SurveyID = '" + str(SurveyID) + "') AND (GeneratedTransectID = " + str(TransectID) + "))," + \
#             "'" + SegType + "'," + \
#             "'" + OBS1DIR + "'," + \
#             "geography::STGeomFromText('" + str(Shape.WKT) + "', " + str(epsg) + ")," + \
#             "'" + str(Comments) + "'" + \
#             ");\n"
#         file.write(insertquery) # write the query to the output .sql file
#
# #  close the output file
# file.write("\n-- Do not forget to COMMIT or ROLLBACK the changes after executing or the database will be in a locked state \n")
# file.close()
# arcpy.AddMessage('Done')










# # Buffers ------------------------------------------------------------------------------------------------------------
# # NOTE: Buffers are ordinarily in a shapefile instead of NPS.gdb.  Uncomment the section below if they happen to be in the gdb.
# layer = "Buffer_Final" # standard name for the buffers layer
# fc = NPSdotGdbMxd + "/" + layer
# file = open(sqlscriptpath +  layer + ".sql", "w")
# arcpy.AddMessage('Processing ' + layer + "...")
#
# # write some metadata to the sql script
# file.write("-- Insert queries to transfer data from ARCN Sheep monitoring field geodatabase " + NPSdotGdbMxd + " into ARCN_Sheep database\n")
# file.write("-- File generated " + executiontime + " by " + user + "\n")
# file.write("-- If this file is too big to run in Sql Server Management Studio then run from a Windows Power Shell prompt:\n")
# file.write("-- sqlcmd /S SERVER\INSTANCE /i \"" + str(file.name) + "\"\n")
# file.write("USE ARCN_Sheep \n")
# file.write("BEGIN TRANSACTION -- Do not forget to COMMIT or ROLLBACK the changes after executing or the database will be in a locked state \n")
# file.write("SET QUOTED_IDENTIFIER ON\n\n")
# file.write("\n-- insert the GPS track points from " + layer + " -----------------------------------------------------------\n")
#
# fieldsList = arcpy.ListFields(fc) #get the fields
# fields = [] # create an empty list
# #  loop through the fields and change the Shape column (containing geometry) into a token, add columns to the list
# for field in fieldsList:
#     print field.name
#     if field.name == "SHAPE":
#         fields.append("SHAPE@")
#     elif field.name == "Shape":
#         fields.append("Shape@")
#     else:
#         fields.append(field.name)
#
# # get the data into a cursor so we can translate it into sql to insert into the sheep sql server database
# # loop through the cursor and save fields as variables to be used later in insert queries
# cursor = arcpy.da.SearchCursor(fc,fields,"",sr)
# for row in cursor:
#     OBJECTID = row[0]
#     SHAPE = row[1]
#     GeneratedTransectID = row[2]
#     GeneratedSurveyID = layer + "-" + str(GeneratedTransectID) # for lack of anything better
#
#     # build an insert query
#     insertquery = "INSERT INTO Buffers(" + \
#         "TransectID," + \
#         "GeneratedSurveyID," + \
#         "GeneratedTransectID," + \
#         "SegmentID," + \
#         "Obs1Dir," + \
#         "PolygonFeature," + \
#         "BufferFileDirectory" + \
#         ") VALUES(" + \
#         "(SELECT TransectID FROM Transect_or_Unit_Information WHERE (GeneratedTransectID = " + str(GeneratedTransectID) + ") And (SurveyID = '" + str(SurveyID) + "'))," + \
#         "'" + str(GeneratedSurveyID) + "'," + \
#         "'" + str(GeneratedTransectID) + "'," + \
#         "NULL," + \
#         "NULL," + \
#         "geography::STGeomFromText('" + str(Shape.WKT) + "', " + str(epsg) + ")," + \
#         "'" + fc + "/" + layer + "'" + \
#         ");\n"
#
#     file.write(insertquery) # write the query to the output .sql file
#
# #  close the output file
# file.write("\n-- Do not forget to COMMIT or ROLLBACK the changes after executing or the database will be in a locked state \n")
# file.close()
# arcpy.AddMessage('Done')




# # FlatAreas ------------------------------------------------------------------------------------------------------------
# layer = "FlatAreas"
# fc = NPSdotGdbMxd + "/" + layer
# file = open(sqlscriptpath +  layer + ".sql", "w")
# arcpy.AddMessage('Processing ' + layer + "...")
#
# # write some metadata to the sql script
# file.write("-- Insert queries to transfer data from ARCN Sheep monitoring field geodatabase " + NPSdotGdbMxd + " into ARCN_Sheep database\n")
# file.write("-- File generated " + executiontime + " by " + user + "\n")
# file.write("-- If this file is too big to run in Sql Server Management Studio then run from a Windows Power Shell prompt:\n")
# file.write("-- sqlcmd /S SERVER\INSTANCE /i \"" + str(file.name) + "\"\n")
# file.write("USE ARCN_Sheep \n")
# file.write("BEGIN TRANSACTION -- Do not forget to COMMIT or ROLLBACK the changes after executing or the database will be in a locked state \n")
# file.write("SET QUOTED_IDENTIFIER ON\n\n")
# file.write("\n-- insert the GPS track points from " + layer + " -----------------------------------------------------------\n")
# file.write("DECLARE @SurveyID nvarchar(50) -- SurveyID of the record in the Surveys table to which the transects below will be related\n")
# file.write("SET @SurveyID = '" + str(SurveyID) + "'\n")
#
#
# fieldsList = arcpy.ListFields(fc) #get the fields
# fields = [] # create an empty list
# #  loop through the fields and change the Shape column (containing geometry) into a token, add columns to the list
# for field in fieldsList:
#     if field.name == "Shape":
#         fields.append("Shape@")
#     else:
#         fields.append(field.name)
#
# # get the data into a cursor so we can translate it into sql to insert into the sheep sql server database
# # loop through the cursor and save fields as variables to be used later in insert queries
# cursor = arcpy.da.SearchCursor(fc,fields,"",sr)
# for row in cursor:
#     Shape = row[1]
#     GeneratedSurveyID = row[6]
#
#     # build an insert query
#     insertquery = "INSERT INTO FlatAreas(" + \
#         "GeneratedSurveyID," + \
#         "SurveyID," + \
#         "PolygonFeature" + \
#         ") VALUES(" + \
#         "'" + str(GeneratedSurveyID) + "'," + \
#         "@SurveyID," + \
#         "geography::STGeomFromText('" + str(Shape.WKT) + "', " + str(epsg) + ")" + \
#         ");\n"
#
#     file.write(insertquery) # write the query to the output .sql file
#
# #  close the output file
# file.write("\n-- Do not forget to COMMIT or ROLLBACK the changes after executing or the database will be in a locked state \n")
# file.close()









# NOTE: The code section for GPS Tracklog is commented out below because the layer can potentially contain many thousands of
# records which can take a long time to run.  Uncomment the code as needed.
# GPS Tracklog ------------------------------------------------------------------------------------------------------------
def GenerateGPSPointsLogSQLScript(SurveyID):
    layer = "GPSPointsLog"
    fc = NPSdotGdbMxd + "/" + layer
    file = open(sqlscriptpath +  layer + ".sql", "w")
    arcpy.AddMessage('Processing ' + layer + "...")

    # write some metadata to the sql script

    file.write("-- Insert queries to transfer data from ARCN Sheep monitoring field geodatabase " + NPSdotGdbMxd + " into ARCN_Sheep database\n")
    file.write("-- File generated " + executiontime + " by " + user + "\n")
    file.write("*************** \n")
    file.write("WARNING: The GPS points layer generates extremely large files that almost always cause Sql Server Management Studio to bog down and crash. \n")
    file.write("*************** \n")
    file.write("-- If this file is too big to run in Sql Server Management Studio then run from a Windows Power Shell prompt:\n")
    file.write("-- sqlcmd /S SERVER\INSTANCE /i \"" + str(file.name) + "\"\n")
    file.write("USE ARCN_Sheep \n")
    file.write("-- BEGIN TRANSACTION -- Do not forget to COMMIT or ROLLBACK the changes after executing or the database will be in a locked state \n")
    file.write("SET QUOTED_IDENTIFIER ON\n\n")
    file.write("\n-- insert the GPS track points from " + layer + " -----------------------------------------------------------\n")

    fieldsList = arcpy.ListFields(fc) #get the fields
    fields = [] # create an empty list
    #  loop through the fields and change the Shape column (containing geometry) into a token, add columns to the list
    for field in fieldsList:
        if field.name == "Shape":
            fields.append("Shape@")
        elif field.name == "SHAPE":
            fields.append("SHAPE@")
        else:
            fields.append(field.name)

    # get the data into a cursor so we can translate it into sql to insert into the sheep sql server database
    # loop through the cursor and save fields as variables to be used later in insert queries
    cursor = arcpy.da.SearchCursor(fc,fields,"",sr)
    i = 1
    for row in cursor:
        OBJECTID = row[0]
        SHAPE = row[1]
        DATE_ = row[2]
        ALTITUDE = row[3]
        LATITUDE = row[4]
        LONGITUDE = row[5]
        XCOORD = row[6]
        YCOORD = row[7]
        PDOP = row[8]
        PLANESPD = row[9]
        TIME_ = row[10]
        GeneratedSurveyID = row[11]
        PILOTLNAM = row[12]
        AIRCRAFT = row[13]
        HitDate =  DATE_ + " " + TIME_


        if not SHAPE is None:
            WKT = SHAPE.WKT
        else:
            WKT = "NULL"
        geog = "geography::STGeomFromText('" + WKT + "', " + str(epsg) + ")"

        # build an insert query
        # notes:
        # GPSModel,Source, SourceFileName, TracksFileDirectory and Comment don't appear in NPS.gdb
        # Most of the time GPS track logs will use point features.  If the tracklog is a line feature then
        # modify the script to put the line into LineFeature instead of PointFeature
        insertquery = "INSERT INTO GPSTracks(" + \
            "PilotName," + \
            "TailNo," + \
            "CaptureDate," + \
            "GPSModel," + \
            "Altitude," + \
            "Source," + \
            "SourceFileName," + \
            "TracksFileDirectory," + \
            "Comment," + \
            "PointFeature," + \
            "SurveyID" + \
            ")" + \
            "VALUES(" + \
            fixArcGISNull(PILOTLNAM, True,False) +  \
            "," + fixArcGISNull(AIRCRAFT, True,False) +  \
            "," + fixArcGISNull(HitDate, True, False)  + \
            ", NULL" + \
            "," + fixArcGISNull(str(ALTITUDE), False, True) + \
            ",'" + fc + "'" + \
            ",'" + fc + "'" + \
            ", NULL" + \
            "," + fixArcGISNull(str(Comments), True, False) + \
            "," + geog  + \
            ",'" + str(SurveyID) + "'" + \
            ");\n"

        # only write out the query if we have a geometry
        if not WKT == 'NULL':
            file.write("PRINT 'ROW " + str(i) + "';\n")
            file.write(insertquery) # write the query to the output .sql file
            file.write("GO\n\n")
            i = i + 1
    # close the output file
    file.close()

# Generate the SQL insert query scripts
GenerateTrnOrigSQLScript(SurveyID) #TrnOrig layer










arcpy.AddMessage("Done!")
arcpy.AddMessage("Finished processing " + NPSdotGdbMxd)
arcpy.AddMessage("Input geodatabase: " + NPSdotGdbMxd)
arcpy.AddMessage("Output directory: " + sqlscriptpath)
arcpy.AddMessage("SurveyID: " + str(SurveyID))
arcpy.AddMessage("")
arcpy.AddMessage("Your SQL insert query scripts are available at " + sqlscriptpath.replace("/","\\"))