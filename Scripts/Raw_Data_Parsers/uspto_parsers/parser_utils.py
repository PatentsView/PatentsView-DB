import csv
import requests
import os
from clint.textui import progress


def write_csv(rows, outputdir, filename):
    """ Write a list of lists to a csv file """
    writer = csv.writer(open(os.path.join(outputdir, filename), 'wb'))
    writer.writerows(rows)


def download(url, filepath):
    """ Download data from a URL with a handy progress bar """

    print("Downloading: {}".format(url))
    r = requests.get(url, stream=True)

    with open(filepath, 'wb') as f:
        content_length = int(r.headers.get('content-length'))
        for chunk in progress.bar(r.iter_content(chunk_size=1024),
                                  expected_size=(content_length/1024) + 1):
            if chunk:
                f.write(chunk)
                f.flush()
