# ImportGPSPointsFromNPSdotGDB.py
# Purpose:  Transfers the NPS.gdb/GPSPoints layer's data to the ARCN_Sheep database.
# NOTE: This script writes data directly to the database.  Do not 'test' the script without knowing
# what you are doing.
# NOTE: NPS.gdb collects prolific GPS points.  This script may take many hours to complete.

# Written by Scott D. Miller, Data Manager, Arctic and Central Alaska Inventory and Monitoring Networks, October, 2014

import arcpy # import the arcpy library
import pyodbc # import pyodbc library to allow database connections

# ArcToolbox parameters --------------------------------------------
NPSdotGdbMxd = arcpy.GetParameterAsText(0) # path to the NPS.gdb
server = arcpy.GetParameterAsText(1) # SQL Server
database = 'ARCN_Sheep'
SurveyID = arcpy.GetParameterAsText(2) # the SurveyID from the ARCN_Sheep database to which the GPS points will be related
connectionstring = 'DRIVER={SQL Server Native Client 10.0};SERVER=' + server + ';DATABASE=' + database + ';Trusted_Connection=yes'

# echo parameters
arcpy.AddMessage('NPS.gdb: ' + NPSdotGdbMxd)
arcpy.AddMessage('Server: ' + server)
arcpy.AddMessage('Database: ' + database)
arcpy.AddMessage('SurveyID: ' + SurveyID)
arcpy.AddMessage('Connection string: ' + connectionstring)

# spatial coordinate system
# the data in the output sql script will be in the reference system indicated below
epsg = 4326 # EPSG SRS code for WGS84
sr = arcpy.SpatialReference(epsg)



# function fixArcGISNullString
# accepts: str, String to process. quote, Boolean, whether to surround the returned string with single quotes
# returns: String
# purpose: ArcGIS is all over the place with null values, sometimes returning blank strings, other times 'None' or '<Null>'
# These values won't work for SQL so we need look at the value of str and determine if it should be a NULL or not.
# If it's OK then surround with single quotes and return, otherwise return unquoted NULL.
def fixArcGISNull(inputString, quoted, nullToZero):
    # "function fixArcGISNullString + \
    # accepts: str, String to process. quote, Boolean, whether to surround the returned string with single quotes + \
    # returns: String  + \
    # purpose: ArcGIS is all over the place with null values, sometimes returning blank strings, other times 'None' or '<Null>'  + \
    # These values won't work for SQL so we need look at the value of str and determine if it should be a NULL or not. + \
    # If it's OK then surround with single quotes and return, otherwise return unquoted NULL." + "\'"

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




# GPS Tracklog ------------------------------------------------------------------------------------------------------------
layer = "GPSPointsLog"
fc = NPSdotGdbMxd + "/" + layer
arcpy.AddMessage('Processing ' + layer + "...")

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
i = 1 # a counter; increments with each iteration
failedquerycount = 0 # increments with each failed insert query to give an idea of how many failed
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
        "," + fixArcGISNull(HitDate, True, False)  + \
        ", NULL" + \
        "," + fixArcGISNull(str(ALTITUDE), False, True) + \
        ",'" + fc + "'" + \
        ",'" + fc + "'" + \
        ", NULL" + \
        ", NULL" + \
        "," + geog  + \
        ",'" + SurveyID + "'" + \
        ");\n"



    # only write out the query if we have a geometry
    if not WKT == 'NULL' :
        # open a connection to the sql server
        connection = pyodbc.connect(connectionstring)
        # get a cursor
        sqlcursor = connection.cursor()

        # try to execute the insert query, if it fails report why
        try:
            sqlcursor.execute(insertquery)
            sqlcursor.commit()
            arcpy.AddMessage('Row: ' + str(i) + ' ' + insertquery)
        except Exception as ex:
            arcpy.AddMessage('****** FAILED, Row: ' + str(i) + '. ' + str(ex) + ' ' + insertquery)
            failedquerycount = failedquerycount + 1
    i = i + 1

# report done
arcpy.AddMessage('Done')
arcpy.AddMessage(failedquerycount + ' queries failed to execute')