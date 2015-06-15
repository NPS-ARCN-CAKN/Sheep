# BuffersToSqlServer.py
# Purpose: A Python script used by the National Park Service Arctic and Central Alaska Networks to export aerial sheep survey
# field data from an ESRI Shapefile into a series of SQL scripts containing insert queries.  The
# generated scripts can be executed against the main sheep monitoring Sql Server database (ARCN_Sheep) to import the field data.

# Written by Scott D. Miller, Data Manager, Arctic and Central Alaska Inventory and Monitoring Networks, April, 2015

# import the arcpy library
import arcpy

# USER MUST SUPPLY THE VARIABLES BELOW --------------------------------------------

# source shapefile of the survey buffers
bufferfile = arcpy.GetParameterAsText(0)

# directory where the sql script will be created
outputfile = arcpy.GetParameterAsText(1)

# Supply the SurveyID from the Surveys table of the ARCN_Sheep database for this survey campaign.
#  e.g. the Itkillik 2011 Survey's SurveyID is '1AC66891-5D1E-4749-B962-40AB1BCA577F'
SurveyID = arcpy.GetParameterAsText(2)

# echo the parameters
arcpy.AddMessage("Buffer file: " + bufferfile)
arcpy.AddMessage("Output file: " + outputfile)
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

# Buffers ------------------------------------------------------------------------------------------------------------
arcpy.AddMessage("Processing: " + outputfile)
file = open(outputfile, "w")

# write some metadata to the sql script
file.write("-- Insert queries to transfer data from ARCN Sheep monitoring buffers shapefile " + fc + " into ARCN_Sheep database\n")
file.write("-- File generated " + executiontime + " by " + user + "\n")

file.write("-- Input buffer file: " + bufferfile + "\n")
file.write("-- Output file: " + outputfile + "\n")
file.write("-- SurveyID: " + SurveyID + "\n")

file.write("-- If this file is too big to run in Sql Server Management Studio then run from a Windows Power Shell prompt: sqlcmd /S YOURSQLSERVER\INSTANCENAME /i ""C:\Your Script.sql""\n")
file.write("USE ARCN_Sheep \n")
file.write("BEGIN TRANSACTION -- Do not forget to COMMIT or ROLLBACK the changes after executing or the database will be in a locked state \n")
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
for row in cursor:
    SHAPE = row[1]
    GeneratedTransectID = row[2]
    GeneratedSurveyID = row[4]
    SegmentID = 0
    Obs1Dir = 'Unk'
    PilotLastname = row[3]
    SurveyName = row[4]
    BufferFileDirectory =  layer

    # build an insert query
    insertquery = "INSERT INTO Buffers(" + \
        "TransectID," + \
        "GeneratedTransectID," + \
        "GeneratedSurveyID," + \
        "SegmentID," + \
        "Obs1Dir," + \
        "PolygonFeature," + \
        "BufferFileDirectory" + \
        ") VALUES(" + \
        "(SELECT TransectID FROM Transect_or_Unit_Information WHERE (SurveyID = '" + SurveyID + "' And GeneratedTransectID = " + str(GeneratedTransectID) + "))," + \
        "'" + str(GeneratedTransectID) + "'," + \
        "'" + str(GeneratedSurveyID) + "'," + \
        "'" + str(SegmentID) + "'," + \
        "'" + str(Obs1Dir) + "'," + \
        "geography::STGeomFromText('" + SHAPE.WKT + "', " + str(epsg) + ")," + \
        "'" + BufferFileDirectory + "'" + \
        ");\n"

    # print insertquery # print the query to standard output
    file.write(insertquery) # write the query to the output .sql file

#  close the output file
file.write("\n-- Do not forget to COMMIT or ROLLBACK the changes after executing or the database will be in a locked state \n")
file.close()
arcpy.AddMessage('Output written to ' + outputfile)