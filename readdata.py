import json
import os;


def isExistsDirectory(dir):
    return os.path.isdir(dir)


def filelist(dir, file_list):
    for (path, dirname, files) in os.walk(dir):
        for filename in files:
            ext = os.path.splitext(filename)[-1]
            if ext == ".json":
                file_path = path + '/' + filename
                file_list.append(file_path)


def jsonLoad(file_path):
    with open(file_path) as json_file:
        json_data = json.load(json_file)
    return json_data