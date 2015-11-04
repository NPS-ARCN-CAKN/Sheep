
import os

# acquire variables from user --------------------------------------------------------------
# path to interrogate
directory = "C:/Work/VitalSigns/ARCN-CAKN Dall Sheep/zWorking/Waypoints" # arcpy.GetParameterAsText(0)

# ------------------------------------------------------------------------------------------

def renamefile(filename,newfilename):
    sourcefile = directory + '/' + filename
    destinationfile = directory + '/' + newfilename
    print sourcefile + " ---> " + destinationfile
    os.rename(sourcefile, destinationfile)
    #print filename + " will be renamed: " + newfilename

    #os.rename(directory + '/' + filename, newfilename)

for filename in os.listdir(directory):
    pilotinitials = filename[0 :2]
    initialtailno = filename[2:filename.index('_')]

    if pilotinitials == 'AG':
        pilotname = "Andy Greenblatt"
        tailno = 'N' + initialtailno
        newfilename = pilotname.replace(' ', '_') + '_' + tailno + '_' + filename
        renamefile(filename,newfilename)
    elif pilotinitials == 'BS':
        pilotname = "Brad Shults"
        tailno = 'N' + initialtailno
        newfilename = pilotname.replace(' ', '_') + '_' + tailno + '_' + filename
        renamefile(filename,newfilename)
    elif pilotinitials == 'CM':
        pilotname = "C. Milone"
        tailno = 'N' + initialtailno
        newfilename = pilotname.replace(' ', '_') + '_' + tailno + '_' + filename
        renamefile(filename,newfilename)
    elif pilotinitials == 'ES':
        pilotname = "Eric Sieh"
        tailno = 'N' + initialtailno
        newfilename = pilotname.replace(' ', '_') + '_' + tailno + '_' + filename
        renamefile(filename,newfilename)
    elif pilotinitials == 'HT':
        pilotname = "Hollis Twitchell"
        tailno = 'N' + initialtailno
        newfilename = pilotname.replace(' ', '_') + '_' + tailno + '_' + filename
        renamefile(filename,newfilename)
    elif pilotinitials == 'JC':
        pilotname = "Jesse Cummings"
        tailno = 'N' + initialtailno
        newfilename = pilotname.replace(' ', '_') + '_' + tailno + '_' + filename
        renamefile(filename,newfilename)
    elif pilotinitials == 'LW':
        pilotname = "Lance Williams"
        tailno = 'N' + initialtailno
        newfilename = pilotname.replace(' ', '_') + '_' + tailno + '_' + filename
        renamefile(filename,newfilename)
    elif pilotinitials == 'SH':
        pilotname = "Sandy Hamilton"
        tailno = 'N' + initialtailno
        newfilename = pilotname.replace(' ', '_') + '_' + tailno + '_' + filename
        renamefile(filename,newfilename)




    # if filename.startswith("CM",0,2):
    #     newfilename = "Charles_Manson_N21HY_" + filename
    #     renamefile(filename,newfilename)
    #     continue
    # elif filename.startswith("LW",0,2):
    #     newfilename = "Lance_Williams_N04D_" + filename
    #     renamefile(filename,newfilename)
    #     continue


#tailnumber = trimmedfilename[:trimmedfilename.index('_')]
