# TracklogToSQL.py
# Purpose: A Python script used by the National Park Service Arctic and Central Alaska Networks
# Dall's sheep monitoring program.  Exports pilot's or observer's tracklog collected during aerial sheep surveys
# to SQL insert queries suitable for importing the data into the master sheep monitoring database.

# This Python script loops through the records in the tracklog shapefile and writes an SQL insert query
# for each record.  Each insert query is sequentially appended to an output sql script file that is
# created in the source file's directory and with the source file's name suffixed with '.sql'

# Notes on using the script:
# This script does not interact with the ARCN_Sheep database in any way; it just exports .sql scripts, so there is
# no danger of database corruption to test-running the script.
# The script is designed to be run via an ArcGIS toolbox tool.
# Ensure the column mappings in the script match the destination database table.
# Python requires forward slashes for directory delimiters contrary to Windows.  Replace '\' with '/' in any paths.
# IMPORTANT NOTE: The SQL insert queries are wrapped in unclosed transaction statements (the transaction is started,
# but not finished with either a COMMIT or ROLLBACK statement).  The transaction ensures that either all the records
# are inserted or none of the records are inserted (e.g. if any query fails then they all fail). You must COMMIT or
# ROLLBACK the transaction after running the script or the database will be left in a locked state.

# Written by Scott D. Miller, Data Manager, Arctic and Central Alaska Inventory and Monitoring Networks, October, 2015

# import libraries
import arcpy
import os

# USER MUST SUPPLY THE VARIABLES BELOW --------------------------------------------
TracklogFile = arcpy.GetParameterAsText(0)# Supply a path to the tracklog shapefile #
#  Supply the SurveyID from the Surveys table
# Tracklog will be related to this SurveyID in the sheep database
# Contact the Network data manager or project leader for this value
SurveyID = arcpy.GetParameterAsText(1) # SurveyID
PilotName = arcpy.GetParameterAsText(2)# Pilot's name
TailNo = arcpy.GetParameterAsText(3)# Aircraft tail number
TracklogSource = arcpy.GetParameterAsText(4)# Source of the GPS tracklog, usually 'Pilot GPS'
SOPNumber  = arcpy.GetParameterAsText(5)# Number of the SOP that guided the data collection
SOPVersion  = arcpy.GetParameterAsText(6)# Version of the SOP that guided the data collection
# -----------------------------------------------------------------------------


OutputFile = TracklogFile + '.sql'# SQL script file that will be written

# echo the parameters
arcpy.AddMessage("Input file: " + TracklogFile + "\n")
arcpy.AddMessage("Output directory: " + OutputFile + "\n")
arcpy.AddMessage("SurveyID: " + SurveyID + "\n")

# Assume the GPS data is in WGS84 spatial coordinate system
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

# routine to process the input shapefile and convert the data to SQL insert queries and write them to the output file
def GenerateSQLScript(Shapefile,SurveyID,PilotName,TailNo):
    arcpy.AddMessage('Processing ' + str(Shapefile) + "\n")

    # EXPORT THE WAYPOINTS ------------------------------------------------------------------------------------------------------------
    fc = TracklogFile
    file = open(OutputFile, "w")

    # write some metadata to the sql script
    file.write("-- Insert queries to transfer pilot tracklog to ARCN_Sheep database\n")
    file.write("-- File generated " + executiontime + " by " + user + "\n")
    file.write("USE ARCN_Sheep \n")
    file.write("BEGIN TRANSACTION -- Do not forget to COMMIT or ROLLBACK the changes after executing or the database will be in a locked state \n")
    file.write("SET QUOTED_IDENTIFIER ON\n\n")
    file.write("\n-- insert the generated transects from " + fc + " -----------------------------------------------------------\n")
    file.write("DECLARE @SurveyID nvarchar(50) -- SurveyID of the record in the Surveys table to which the transects below will be related\n")
    file.write("DECLARE @PilotName nvarchar(30) -- Pilot's name \n")
    file.write("DECLARE @TailNo nvarchar(20) -- Aircraft tail number\n")
    file.write("DECLARE @TracklogSource nvarchar(20) -- Source of the tracklog, usually pilot's GPS\n")
    file.write("DECLARE @SOPNumber int -- Standard operating procedure number\n")
    file.write("DECLARE @SOPVersion int -- Standard operating procedure version\n")
    file.write("SET @SurveyID = '" + SurveyID + "'\n")
    file.write("SET @PilotName = '" + PilotName + "'\n")
    file.write("SET @TailNo = '" + TailNo + "'\n")
    file.write("SET @TracklogSource = '" + TracklogSource + "'\n")
    file.write("SET @SOPNumber = " + str(SOPNumber) + "\n")
    file.write("SET @SOPVersion = " + str(SOPVersion) + "\n")

    # we'll need to create a searchcursor a little further on to access the records in the layer.  the cursor has a fields parameter
    # we could just submit a * to gather all columns except that we need the Shape column returned as a token, e.g. Shape@;
    # see ArcGIS documentation.  We also can't predict the case of the shape column,
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

    # get the data into a cursor so we can translate it into sql to insert into the sheep sql server database
    # loop through the cursor and save fields as variables to be used later in insert queries
    LineString = "LINESTRING("
    cursor = arcpy.da.SearchCursor(fc,fields,"",sr)
    for row in cursor:
        Shape = row[1]
        comment = row[9]
        altitude = row[14] #
        altitude = float(altitude) * 0.3048 # assume silly units, convert to meters
        model = row[18] #
        ltime = row[20]
        Lat = row[5]
        Lon = row[6]
        PointString = 'POINT(' + str(Lon) + ' ' + str(Lat) + '\n'
        LineString = LineString  + ' ' + str(Lon) + ' ' + str(Lat) + ',\n'

    LineString = LineString.rstrip()
    LineString = LineString[:-1] + ")" #trim trailing comma
    LineGeog = "geography::STGeomFromText('" + LineString + "', 4326)"
    LineGeogSql = "DECLARE @g geography\nSET @g = " + LineGeog + "\n" # send this to SSMS to troubleshoot geog problems

    # # generate an insert query
    insertquery = "INSERT INTO [PilotTracklogs](" + \
        "[PilotName]" + \
        ",[TailNo]" + \
        ",[CaptureDate]" + \
        ",[Altitude]" + \
        ",[GPSModel]" + \
        ",[SourceFilename]" + \
        ",[Source]" + \
        ",[SOPNumber]" + \
        ",[SOPVersion]" + \
        ",[Comments]" + \
        ",[Tracklog]" + \
        ",[SurveyID]" + \
        ") VALUES(" + \
        "@PilotName" + \
        ",@TailNo" + \
        ",'" + str(ltime) + "'" + \
        "," + str(altitude) + "" + \
        ",'" + str(model) + "'" + \
        ",'" + str(os.path.basename(TracklogFile)) + "'" + \
        ",@TracklogSource" + \
        ",@SOPNumber" + \
        ",@SOPVersion" + \
        ",'" + str(comment) + "'" + \
        "," + LineGeog + \
        ",@SurveyID);\n"

    file.write(insertquery) # write the query to the output file


    # close the output file
    file.close()
    arcpy.AddMessage('Done\n')

# process the tracklog shapefile using the GenerateSQLScript routine
GenerateSQLScript(TracklogFile,SurveyID,PilotName,TailNo)

#inform user that we're done
arcpy.AddMessage('TracklogToSQL finished successfully\n')
arcpy.AddMessage( 'Output file available at ' + OutputFile)
