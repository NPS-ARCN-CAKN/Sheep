# This python script was used to transfer sheep survey units from a shapefile
# into SQL Server insert queries suitable for entering data into the ARCN-CAKN sheep monitoring database
# Written by Scott Miller, Data Manager, National Park Service, Arctic and Central Alaska Inventory and Monitoring Networks
# April, 2015

import arcpy

# input shapefile
layername = "DENA_sheep_survey_NBS1996"
fc = "C:/Work/VitalSigns/ARCN-CAKN Dall Sheep/Data/LegacySurveyUnits/" + layername + ".shp"

# Supply an output file to which to export the insert queries
file = open("C:/Work/VitalSigns/ARCN-CAKN Dall Sheep/Data/LegacySurveyUnits/" + layername + ".sql", "w")

# we'll need to create a searchcursor a little further on to access the records in the layer.  the cursor has a fields parameter
# we could just submit a * to gather all columns except that we need the Shape column returned as a token, e.g. Shape@,
# so we have to submit all the columns as a list with Shape changed to the Shape@ token that will allow us to get at geometry info.
# The easiest way to do this is to loop through the column names and load them into a list making our edits as needed
fieldsList = arcpy.ListFields(fc) #get the fields
fields = [] # create an empty list

#  loop through the fields and add the columns to the list, change the Shape column (containing geometry) into a token,
for field in fieldsList:
    if field.name == "Shape":
        fields.append("Shape@")
    else:
        fields.append(field.name)

file.write("-- Insert queries to transfer data from " + fc + " into ARCN_Sheep database\n")
file.write("USE ARCN_Sheep \n")
file.write("BEGIN TRANSACTION -- Do not forget to COMMIT or ROLLBACK the changes after executing or the database will be in a locked state \n")
file.write("\n-- insert the generated transects from " + fc + " -----------------------------------------------------------\n")


# loop through the fields and output them
# i = 0
# for field in fieldsList:
#     print field.name + " = row[" + str(i) + "]"
#     i = i + 1

# get the transect data into a cursor so we can translate it into sql to insert into the sheep sql server database
# loop through the cursor and save fields as variables to be used later in insert queries
# spatial coordinate system
epsg = 4326 # WGS84
sr = arcpy.SpatialReference(epsg)
cursor = arcpy.da.SearchCursor(fc,fields,"",sr)
for row in cursor:
    FID = row[0]
    Shape = row[1]
    AREA = row[2]
    PERIMETER = row[3]
    SHPSVITM_ = row[4]
    SHPSVITM_I = row[5]
    ACRES = row[6]
    PERIM_MILE = row[7]
    MAJUNIT = row[8]
    SUBUNIT = row[9]
    NAME = "DENA92-" + str(MAJUNIT)
    geog = "geography::STGeomFromText('" + Shape.WKT + "', " + str(epsg) + ")"

    if SUBUNIT <> "":
        SUBUNIT = "SUBUNIT: " + SUBUNIT
    else:
        SUBUNIT = ""
    # build insert query
    sql = "INSERT INTO [LegacyUnits](" + \
        "[LegacyUnitID]," + \
        "[ARCNUnitName]," + \
        "[DENAUnitName]," + \
        "[Comments]," + \
        "[Source]," + \
        "[PolygonFeature]" + \
        ")VALUES(" + \
        "'" + NAME + "'," + \
        "'" + NAME + "'," + \
        "'" + NAME+ "'," + \
        "'" + str(SUBUNIT) + "'," + \
        "'" + layername+ "'," + \
        str(geog) + ")"
    file.write(sql + "\n")
file.close()
print "Output written to " + file.name


