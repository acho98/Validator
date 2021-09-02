
from time import localtime, strftime


def error_json(project, file_path, collection_name, error_code, error_name, error_msg, work_time):
    return {
        "json_file"  : file_path,
        "project_name" : project,
        "schema_name" : collection_name,
        "error_code" : error_code,
        "error_name" : error_name,
        "error_msg"  : error_msg,
        "create_dt" : work_time
    }