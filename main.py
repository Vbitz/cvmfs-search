"""
CVMFS Search by Hash
Joshua D. Scarsbrook - The University of Queensland
License: Apache-2.0
"""

import urllib.parse
import urllib
import requests
import os.path
import zlib
import sqlite3
from struct import pack
import csv
import hashlib
from subprocess import check_output
import sys


def fetch_http_file(url):
    u = urllib.parse.urlparse(url)

    # Create a local cache path.
    path = os.path.join("cache", u.netloc, u.path[1:] + ".cache")

    # If the path already exists then return the existing file.
    if os.path.exists(path):
        return path

    # Create all directories in the tree.
    dirname = os.path.dirname(path)
    os.makedirs(dirname, exist_ok=True)

    print("downloading: %s", url)

    # Download the file.
    resp = requests.get(url)
    resp.raise_for_status()

    with open(path, "wb") as f:
        f.write(resp.content)

    return path


def fetch_http_bytes(url):
    return open(fetch_http_file(url), "rb").read()


def data_url_for(base_url, hash):
    # Given a hash get the URL for the file contents.
    return "{}data/{}/{}C".format(base_url, hash[:2], hash[2:])


def sha1(content):
    return hashlib.sha1(content).hexdigest()


def get_all_files_in_catalog(base_url, hash, ret):
    catalog_url = data_url_for(base_url, hash)

    catalog_file = fetch_http_file(catalog_url)

    # Decompress the catalog data.
    contents = zlib.decompress(open(catalog_file, "rb").read())

    with open(catalog_file + ".sqlite", "wb") as f:
        f.write(contents)

    # Open the sqlite database.
    catalog = sqlite3.connect(catalog_file + ".sqlite")

    # For every file in the catalog add it to the index.
    for p1, p2, parent1, parent2, name, content_hash in catalog.execute(
        "SELECT md5path_1, md5path_2, parent_1, parent_2, name, hash FROM catalog"
    ):
        path = pack(">qq", p1, p2).hex()
        parent = pack(">qq", parent1, parent2).hex()

        if content_hash != None:
            content_hash = content_hash.hex()
        else:
            content_hash = "0000000000000000000000000000000000000000"

        ret[path] = (parent, content_hash, name)

    print("loaded ", len(ret), " records")

    # Recurse into any nested catalogs.
    for path, sha1 in catalog.execute("SELECT path, sha1 FROM nested_catalogs"):
        get_all_files_in_catalog(base_url, sha1, ret)

    return ret


results_cache = {}


def do_search(hash, kind=""):
    out = []

    if hash in results_cache:
        out = results_cache[hash]
    else:
        # Search by calling grep to find the hash.
        out = check_output(["grep", hash, "db.csv"]).decode("utf8").splitlines()
        results_cache[hash] = out

    # We assume there are no full hash collisions so recurse on any lines in the output.
    ret = []
    for line in out:
        path_hash, parent_hash, content_hash, name = line.split(",")
        if kind == "parent" and parent_hash == hash:
            ret.append((path_hash, parent_hash, content_hash, name))
        elif kind == "content" and content_hash == hash:
            ret.append((path_hash, parent_hash, content_hash, name))
        elif kind == "path" and path_hash == hash:
            ret.append((path_hash, parent_hash, content_hash, name))

    return ret


def get_path(hash, first=True):
    # Build the final path of the file.

    kind = "content"
    if not first:
        kind = "path"

    results = do_search(hash, kind=kind)

    ret = []

    print(results)

    for path_hash, parent_hash, content_hash, name in results:
        if parent_hash == "00000000000000000000000000000000":
            ret.append("/")
            continue
        child_results = get_path(parent_hash, first=False)
        for result in child_results:
            ret.append(os.path.join(result, name))

    return ret


def main(args):
    if args[1] == "search":
        # Search for a given content hash.
        print(get_path(args[2]))
    elif args[1] == "index":
        # Index a existing CVMFS repository. Pass a URL to access the repo at.
        base_url = args[2]

        # Parse the cvmfspublished file to get the catalog hash.
        published_file = fetch_http_bytes(base_url + ".cvmfspublished")
        published = published_file.split(b"\n--\n")[0].decode("utf8").splitlines()
        published = {k[0]: k[1:] for k in published}

        ret = {}

        # Get all files in the catalog.
        files = get_all_files_in_catalog(base_url, published["C"], ret)

        print("finished loading")

        # Write the index to a CSV file for later searching.
        with open("db.csv", "w") as f:
            w = csv.writer(f)
            for k in files:
                w.writerow([k] + list(files[k]))

        print("finished")
    else:
        print("usage: search_by_hash.py [search <hash>|index <repo>]")


if __name__ == "__main__":
    main(sys.argv)
