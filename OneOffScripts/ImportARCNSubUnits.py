# This python script was used to transfer sheep survey units from a shapefile
# into SQL Server insert queries suitable for entering data into the ARCN-CAKN sheep monitoring database
# Written by Scott Miller, Data Manager, National Park Service, Arctic and Central Alaska Inventory and Monitoring Networks
# April, 2015

import arcpy

# input shapefile
fc = "C:/Work/VitalSigns/ARCN-CAKN Dall Sheep/Data/LegacySurveyUnits/ARCN_Subunits_Sheep_WGS84.shp"

# Supply an output file to which to export the insert queries
file = open("C:/Temp/ARCN_Subunits_Sheep_WGS84.sql", "w")

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

# get the transect data into a cursor so we can translate it into sql to insert into the sheep sql server database
# loop through the cursor and save fields as variables to be used later in insert queries
# spatial coordinate system
epsg = 4326 # WGS84
sr = arcpy.SpatialReference(epsg)
cursor = arcpy.da.SearchCursor(fc,fields,"",sr)
for row in cursor:
    FID = row[0]
    Shape = row[1]
    ARCN_UNITS = row[2]
    Acres = row[3]
    Perim_mile = row[4]
    Sq_km = row[5]
    PERIMETER_ = row[6]
    # 2007 = row[7]
    NewAreaKm2 = row[8]
    Area = row[9]
    # 2005 = row[10]
    # 2006 = row[11]
    Lawler2004 = row[12]
    BruWhi1998 = row[13]
    KugTrCrSU = row[14]
    Adams1988 = row[15]
    WBairdSU = row[16]
    AreaMi2 = row[17]
    geog = "geography::STGeomFromText('" + Shape.WKT + "', " + str(epsg) + ")"

    # build insert query
    sql = "INSERT INTO [LegacyUnits](" + \
        "[LegacyUnitID]," + \
        "[ARCNUnitName]," + \
        "[ARCNSubUnitName]," + \
        "[WBairdsUnitName]," + \
        "[BrubakerWhittenUnitName]," + \
        "[PolygonFeature]" + \
        ")VALUES(" + \
        "'" + ARCN_UNITS + "'," + \
        "'" + ARCN_UNITS + "'," + \
        "'" + ARCN_UNITS+ "'," + \
        "'" + WBairdSU+ "'," + \
        "'" + str(int(BruWhi1998)) + "'," + \
        str(geog) + ")"
    file.write(sql + "\n")
file.close()
print "Output written to " + file.name

