import os
import pickle
from datetime import datetime
from ConfigParser import ConfigParser
from IPython.parallel import Client

config = ConfigParser()
config.read('{0}/config.ini'.format(os.path.dirname(os.path.realpath(__file__))))

rc = Client(packer="pickle")
dview = rc[:]
print rc.ids


@dview.remote(block=True)
def fetch():
    import os
    os.chdir(node)
    for i, f in enumerate(files):
        fname = f.split("/")[-1].split(".")[0]
        if not os.path.exists("{0}.xml".format(fname)):
            os.system("wget {0}".format(f))
            os.system("unzip {0}.zip".format(fname))

fname = open("urls.pickle", "rb")
urls = pickle.load(fname)

master = config.get('directory', 'home')
node = config.get('directory', 'local')
if not os.path.exists("{0}/tar".format(master)):
    os.makedirs("{0}/tar".format(master))

dview["master"] = master
dview["node"] = node
full = []
for year in urls.keys():
    full.extend(urls[year])
dview.scatter("files", full)
fetch()
