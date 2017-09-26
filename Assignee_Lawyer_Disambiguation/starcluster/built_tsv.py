import os
import glob
import sys

# specify the input directory
# something like 20* is fine if we want to take
# care of the directories that begin in the 2000s

search = sys.argv[1]

os.system("rm *.txt")
for f in glob.glob("{0}/*.tar.gz".format(search)):
    s = f.split("/")[-1].split(".")[0]
    y = f.split("/")[0]
    os.system("tar -xzf {0}".format(f))
    for t in glob.glob("*.txt"):
        os.system("cat {0} >> new/{0}".format(t, y))
    os.system("rm *.txt")
