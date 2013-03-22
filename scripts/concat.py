import os
import sys
import shutil

rootdir = sys.argv[1]
newfile = sys.argv[2]

f = open(newfile, "wb");
for root, subFolders, files in os.walk(rootdir):
    for filename in files:
            filePath = os.path.join(root, filename)
            print filePath
            shutil.copyfileobj(open(filePath,'rb'), f)
                   
f.close()
