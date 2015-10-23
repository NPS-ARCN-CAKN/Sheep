# ImportWaypoints.py
# Purpose: A Python script used by the National Park Service Arctic and Central Alaska Networks to export 
# waypoints collected during aerial sheep surveys by the pilot or observer.  This script processes a 
# shapefile of waypoints converting the data into SQL insert queries suitable for executing against the 
# master sheep monitoring SQL Server database.

# This Python script loops through the records in the waypoints shapefile and writes an SQL insert query
# for each record.  Each insert query is sequentially appended to an output sql script file

# Notes on using the script:
# This script does not interact with the ARCN_Sheep database in any way; it just exports .sql scripts, so there is
# no danger of database corruption to test-running the script.
# The script is designed to be run via an ArcGIS toolbox tool.
# Ensure the column mappings from the geodatabase to the destination database table are correct.
# Python requires forward slashes for directory delimiters contrary to Windows.  Replace '\' with '/' in any paths.
# IMPORTANT NOTE: The SQL insert queries are wrapped in unclosed transaction statements (the transaction is started,
# but not finished with either a COMMIT or ROLLBACK statement).  The transaction ensures that either all the records
# are inserted or none of the records are inserted (e.g. if any query fails then they all fail). You must COMMIT or
# ROLLBACK the transaction after running the script or the database will be left in a locked state.

# Written by Scott D. Miller, Data Manager, Arctic and Central Alaska Inventory and Monitoring Networks, October, 2014

# import the arcpy library
import arcpy
import os

# USER MUST SUPPLY THE VARIABLES BELOW --------------------------------------------

# Supply a path to the waypoints shapefile
WaypointsFile = arcpy.GetParameterAsText(0)

# user must supply the pilot's name
PilotName = arcpy.GetParameterAsText(1)

# Supply a directory to output the sql scripts to, the scripts will be named according to the layer they came from
OutputFile = WaypointsFile + '.sql'


# Supply the SurveyID from the Surveys table of the ARCN_Sheep database for this survey campaign.
# e.g. the Itkillik 2011 Survey's SurveyID is '1AC66891-5D1E-4749-B962-40AB1BCA577F'
# Contact the Network data manager for this value
SurveyID = arcpy.GetParameterAsText(2)
# -----------------------------------------------------------------------------

# echo the parameters
arcpy.AddMessage("Input file: " + WaypointsFile)
arcpy.AddMessage("Output directory: " + OutputFile)
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



# EXPORT THE WAYPOINTS ------------------------------------------------------------------------------------------------------------
fc = WaypointsFile
file = open(OutputFile, "w")
arcpy.AddMessage('Processing ' + WaypointsFile + "...")


# fieldsList = arcpy.ListFields(fc) #get the fields
# fields = [] # create an empty list
# #  loop through the fields and add the columns to the list, change the Shape column (containing geometry) into a token,
# for field in fieldsList:
#     if field.name == "Shape":
#         fields.append("Shape@")
#     else:
#         fields.append(field.name)
#
# # loop through the fields and output them
# i = 0
# for field in fieldsList:
#     print field.name + " = row[" + str(i) + "]"
#     i = i + 1



# write some metadata to the sql script
file.write("-- Insert queries to transfer pilot waypoints to ARCN_Sheep database\n")
file.write("-- File generated " + executiontime + " by " + user + "\n")
file.write("USE ARCN_Sheep \n")
file.write("BEGIN TRANSACTION -- Do not forget to COMMIT or ROLLBACK the changes after executing or the database will be in a locked state \n")
file.write("SET QUOTED_IDENTIFIER ON\n\n")
file.write("\n-- insert the generated transects from " + fc + " -----------------------------------------------------------\n")
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
for row in cursor:
    FID = row[0]
    Shape = row[1]
    type = row[2]
    ident = row[3]
    Latitude = row[4]
    Longitude = row[5]
    y_proj = row[6]
    x_proj = row[7]
    comment = row[8]
    display = row[9]
    symbol = row[10]
    dist = row[11]
    proximity = row[12]
    color = row[13]
    altitude = row[14]
    depth = row[15]
    temp = row[16]
    time = row[17]
    wpt_class = row[18]
    sub_class = row[19]
    attrib = row[20]
    link = row[21]
    state = row[22]
    country = row[23]
    city = row[24]
    address = row[25]
    zip = row[26]
    facility = row[27]
    crossroad = row[28]
    ete = row[29]
    dtype = row[30]
    model = row[31]
    filename = row[32]
    ltime = row[33]
    magvar = row[34]
    geoidheigh = row[35]
    desc = row[36]
    fix = row[37]
    sat = row[38]
    hdop = row[39]
    vdop = row[40]
    pdop = row[41]
    ageofdgpsd = row[42]
    dgpsid = row[43]
    dir = row[44]


    insertquery = "INSERT INTO [ARCN_Sheep].[dbo].[PilotWaypoints]" + \
           "([WaypointName]" + \
           ",[PilotName]" + \
           ",[CaptureDate]" + \
           ",[Altitude]" + \
           ",[GPSModel]" + \
           ",[SourceFilename]" + \
           ",[Source]" + \
           ",[Comments]" + \
           ",[SOPNumber]" + \
           ",[SOPVersion]" + \
           ",[PointFeature]" + \
           ",[SurveyID])" + \
            "VALUES" + \
           "('" + str(ident) + "'" + \
           ",'" + str(PilotName) + "'" + \
           ",'" + str(time) + "'" + \
           "," + str(altitude) + "" + \
           ",'" + str(model) + "'" + \
           ",'" + str(os.path.basename(WaypointsFile)) + "'" + \
           ",'PILOT GPS'" + \
           ", NULL" + \
           ",11" + \
           ",1" + \
           ",geography::STGeomFromText('" + Shape.WKT.replace(" Z", "") + "', " + str(epsg) + ")" + \
           ",@SurveyID);\n"

    file.write(insertquery) # write the query to the output file

# close the output file
file.close()

print 'Done'
print 'Output file available at ' + OutputFile
