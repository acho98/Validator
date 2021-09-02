#-*- coding:utf-8 -*-
import json
from json import JSONDecodeError
import os
import time
from time import localtime, strftime
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import OperationFailure
from pymongo.write_concern import WriteConcern

from tqdm import tqdm

import readdata
import error_process
import error_migrations


def process(project, datadir, dbname, collection_name, log_path):

    files = []
    work_time = strftime("%Y%m%d%H%M%S", localtime())
    # mongodb client
    # client = MongoClient('localhost', 27017)
    client = MongoClient('mongodb://admin:mongo@localhost:27011')
    db_name = dbname
    collection_name = collection_name
    db = client[db_name]
    val_logs = 'validate_logs_' + project

    readdata.filelist(datadir, files)
    
    errors = []
    err_0101 = []
    total = 0
    success = 0

    if len(files) == 0:
        print('Error: json file not found in the directories')
        quit()
    
    try:
        db[collection_name].delete_many({})
    except OperationFailure as ex:
        print(ex)


    for file_path in tqdm(files):
         
        error_logs = []
        
        filename, extension = os.path.splitext(file_path)
        if extension != '.json': continue
        e0101 = None
        error_result = None
        data = None
        
        try:
            data = readdata.jsonLoad(file_path)
            data["json_file"] = file_path 
            data["project_name"] = project
            data["create_dt"] = work_time
                        
        except UnicodeDecodeError as ex:
            e0101 = error_process.error_json(project, file_list, collection_name, "er-01-01", "파일 오류", "UnicodeDecodeError", work_time)
        except JSONDecodeError as ex:
            e0101 = error_process.error_json(project, file_path, collection_name, "er-01-01", "파일 오류", "JSONDecodeError", work_time)

        # mongodb 저장
        try:
            db[collection_name].insert_one(data)

        except TypeError as ex:
            e0101 = error_process.error_json(project, file_path, collection_name, "er-01-01", "파일 오류", "TypeError, Emptyfile", work_time)  
            err_0101.append(e0101)
            error_result = error_process.error_json(project, file_path, collection_name, "er-01-01", "파일 오류", "TypeError, Emptyfile", work_time)    
        except UnboundLocalError as ex:
            e101 = error_process.error_json(project, file_path, collection_name, "er-01-01", "파일 오류", "UnboundLocalError", work_time)
            err_0101.append(e0101)
            error_result = error_process.error_json(project, file_path, collection_name, "er-01-01", "파일 오류", "UnboundLocalError", work_time)
        except JSONDecodeError as ex:
            e0101 = error_process.error_json(project, file_path, collection_name, "er-01-01", "파일 오류", "JSONDecodeError", work_time)
            err_0101.append(e0101)
            error_result = error_process.error_json(project, file_path, collection_name, "er-01-01", "파일 오류", "JSONDecodeError", work_time)

        except OperationFailure as ex:
            error_result = error_process.error_json(project, file_path, collection_name, "errors_unclassified", "unclassified", ex.details['errInfo']['details'], work_time)
            
            error_logs = error_migrations.extractErrors(
                ex.details['errInfo']['details'], 
                project, 
                file_path, 
                collection_name,
                work_time)

            for e in error_logs:
                db[val_logs].insert_one(e)

        if error_result:
            errors.append(error_result)

        else:
            success += 1
        total += 1
    
    
    for e in err_0101:
        db[val_logs].insert_one(e)

    summary = {
        "project_name": project,
        "schema_name": collection_name,
        "total_cnt" : total,
        "pass_file_cnt" : success,
        "err_file_cnt" : len(errors),
        "err_ratio" : len(errors)/total * 100,
        "create_dt" : work_time
    }

    Path(log_path).mkdir(parents=True, exist_ok=True)
    log_file = "{}/{}-{}.log".format(log_path, work_time, time.time())
    with open(log_file, "w", encoding='utf-8') as f:
        f.write("{}\n".format(json.dumps(summary, ensure_ascii=False)))
    
    db['validate_logs_sum'].insert_one(summary)

    return summary