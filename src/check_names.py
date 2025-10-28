import os
import sys
from typing import List


def get_java_files(directory_path: str) -> List[str]:
    java_files: List[str] = []
    for root, dirs, files in os.walk(directory_path):
        for f in files:
            if f.endswith('.java'):
                java_files.append(f)
    return java_files


def verify_one_db(prefix: str, files: List[str]):
    print('checking database, name: {0}, files: {1}'.format(prefix, files))
    if len(files) == 0:
        print(prefix + ' directory does not contain any files!', file=sys.stderr)
        exit(-1)
    for f in files:
        if not f.startswith(prefix):
            print('The class name of ' + f + ' does not start with ' + prefix, file=sys.stderr)
            exit(-1)
    print('checking database pass: ', prefix)


def verify_all_dbs(name_to_files: dict[str:List[str]]):
    for db_name, files in name_to_files.items():
        verify_one_db(db_name, files)


if __name__ == '__main__':
    cwd = os.getcwd()
    print("Current working directory: {0}".format(cwd))
    name_to_files: dict[str:List[str]] = dict()
    name_to_files["DataFusion"] = get_java_files(os.path.join(cwd, "src", "joinequiv", "datafusion"))
    name_to_files["MariaDB"] = get_java_files(os.path.join(cwd, "src", "joinequiv", "mariadb"))
    name_to_files["MySQL"] = get_java_files(os.path.join(cwd, "src", "joinequiv", "mysql"))
    name_to_files["OceanBase"] = get_java_files(os.path.join(cwd, "src", "joinequiv", "oceanbase"))
    name_to_files["SQLite3"] = get_java_files(os.path.join(cwd, "src", "joinequiv", "sqlite3"))
    name_to_files["TiDB"] = get_java_files(os.path.join(cwd, "src", "joinequiv", "tidb"))
    name_to_files["Percona"] = get_java_files(os.path.join(cwd, "src", "joinequiv", "percona"))
    verify_all_dbs(name_to_files)
