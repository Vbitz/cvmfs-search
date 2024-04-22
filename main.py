"""
CVMFS Search by Hash
Joshua D. Scarsbrook - The University of Queensland
License: Apache-2.0
"""

import cvmfs
import sys
import os
import os.path
import sqlite3
import time


def index_repo(rev, output):
    db = sqlite3.connect(output + ".tmp")

    # Create the catalog table.
    db.execute(
        """CREATE TABLE catalog (
            md5path_1 INTEGER,
            md5path_2 INTEGER,
            parent_1 INTEGER,
            parent_2 INTEGER,
            hash BLOB,
            name TEXT
        )"""
    ).close()

    for clg in rev.catalogs():
        print(clg)

        res = clg.run_sql(
            "SELECT md5path_1, md5path_2, parent_1, parent_2, hash, name FROM catalog"
        )

        for md5path_1, md5path_2, parent_1, parent_2, content_hash, name in res:
            db.execute(
                "INSERT INTO catalog(md5path_1, md5path_2, parent_1, parent_2, hash, name) VALUES (?, ?, ?, ?, ?, ?)",
                (md5path_1, md5path_2, parent_1, parent_2, content_hash, name),
            ).close()

    db.commit()

    db.close()

    print("finished indexing")

    time.sleep(5)

    os.rename(output + ".tmp", output)


def get_path(db: sqlite3.Connection, parent_1, parent_2):
    if parent_1 == 0 and parent_2 == 0:
        return ""

    results = db.execute(
        "SELECT parent_1, parent_2, name FROM catalog WHERE md5path_1 = ? AND md5path_2 = ? LIMIT 1",
        (parent_1, parent_2),
    )

    if results.rowcount == 0:
        raise Exception("not found")

    for parent_1, parent_2, name in results:
        parent = get_path(db, parent_1, parent_2)
        return os.path.join(parent, name)


def main(args):
    repo_url, data_hash = args[0].split("/data/")
    data_hash = data_hash.replace("/", "")

    print(repo_url, data_hash)

    repo = cvmfs.open_repository(repo_url)

    revision = repo.get_current_revision()

    database_cache = os.path.join("cache", revision.root_hash + ".db")
    if not os.path.exists(database_cache):
        index_repo(revision, database_cache)

    content_hash = bytes.fromhex(data_hash)

    db = sqlite3.connect(database_cache)

    for parent_1, parent_2, name in db.execute(
        "SELECT parent_1, parent_2, name FROM catalog WHERE hash = ?",
        (content_hash,),
    ):
        path = get_path(db, parent_1, parent_2)
        print(os.path.join(path, name))


if __name__ == "__main__":
    main(sys.argv[1:])
