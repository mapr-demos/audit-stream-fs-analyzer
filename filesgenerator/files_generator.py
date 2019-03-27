import sys
import os


def create_directory(dir_path):
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)
        print "Directory " + dir_path + " Created "
    else:
        print "Directory " + dir_path + " already exists "


folder = sys.argv[1]
folder_amount = sys.argv[2]
files_amount = sys.argv[3]
print "Creating " + folder_amount + " folders and " + files_amount + " files in " + folder
for i in range(1, int(folder_amount) + 1):
    create_directory(folder + "/folder" + str(i))
print "\n"
for i in range(1, int(files_amount) + 1):
    file_path = folder + "/folder" + str(i % int(folder_amount) + 1) + "/file" + str(i)
    print "Create file " + file_path
    f = open(file_path, "w+")
    f.write("This is file %d" % i)
    f.close()
