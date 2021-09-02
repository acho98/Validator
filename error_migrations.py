import itertools
import re
from flatten_json import flatten


def getLevelByKey(values):
    m = re.findall(r'_\d+_', values)
    result = [0]
    for level in m:
        d = level.replace("_", "")
        result.append(int(d))
    return result

def getMaxLevel(level):
    return len(level)

def makeLevelToString(level):
    tmp = []
    for i in level:
        tmp.append(str(i).zfill(2))
    return "".join(tmp)


def makeErrorData(errors):
    logs = []
    es = [flatten(d) for d in errors]
    id = 0
    group = 0
    for d in es:
        row = []
        for k, v in d.items():
            
            level = getLevelByKey(k)
            max_level = getMaxLevel(level)
            level_str = makeLevelToString(level)
            data = {
                'id' : id,
                'group': group,
                'node_type': 'leaf',
                'level' : level,
                'level_str': level_str,
                'max_level' : max_level,
                'value' : "{}:{}".format(k, v)
            }
            row.append(data)
            id += 1
        logs.append(row)
        group += 1
    return logs



def setNodeType(log_data):
    level_data = []
    for logs in log_data:
        tmp_data = []
        for log in logs:
            tmp_data.append({'id': log['id'], 'level_str' : log['level_str']})
        level_data.append(tmp_data)
    
    index = 0
    for logs in log_data:
        for log in logs:
            
            if log['level_str'] == "00": 
                log["node_type"] = "root"
            
            for l in level_data[index]:
                if len(log['level_str']) > 2:
                    if l['level_str'].startswith(log['level_str']):
                        str_length = len(l['level_str']) - len(log['level_str'])
                        if str_length == 2:
                            log["node_type"] = "node"
        index += 1
    return log_data


def getLeafNodesByGroup(log_data):
    levels = []
    group = 0
    for logs in log_data:
        level = []
        for log in logs:
            if log['node_type'] == "leaf":
                if log['level_str'] not in level:
                    level.append(log['level_str'])
        levels.append({'group': group, 'level' :level})
        group += 1
    return levels


def getTraverseNodes(log_data, leaf_nodes):
    data = []
    for elm in leaf_nodes:
        group = elm['group']
        levels = elm['level']
        if levels == []:
            info = []
            for log in log_data[group]:
                if log['group'] == group:
                    info.append(log['value'])
            data.append(info)
        else:
            for level in levels:
                tmp_level = level
                info = []
                while True:
                    tmp_info = []
                    if tmp_level == "":
                        break
                    for log in log_data[group]:
                        if log['level_str'] == tmp_level:
                            tmp_info.append(log['value'])
                    tmp_level = tmp_level[:-2]
                    info.insert(0, tmp_info)
                data.append(info)
    return data


def flattenLog(logs):
    tmp_log = []
    for log in logs:
        if any(isinstance(i, list) for i in log):
            tmp_log.append(list(itertools.chain.from_iterable(log)))
        else:
            tmp_log.append(log)
    return tmp_log


def shortenKey(data):
    result = []
    for group in data:
        shorten = {}
        for log in group:
            (key, value) = log.split(":")
            key = re.split('_\d+_', key)[-1]
            key = re.sub('_\d+$', '', key)
            if key not in shorten:
                shorten[key] = []
            shorten[key].append(value)
        result.append(shorten)
    return result


def makeErrors(raw_data, shorten_data, project, file_path, collection_name, work_time):
    i = 0
    errors = []
    for shorten in shorten_data:
        error = {}
        
        if 'propertyName' in shorten:
            error_field = ".".join(shorten['propertyName'])

        else:
            error_field = ".".join(shorten['missingProperties'])
        
        error_msg = raw_data[i]
        
        if 'required' in shorten['operatorName']:
            if 'missingProperties' in shorten:
                error_code = "er-02-01"
                error_name = "필수 키 누락"
        elif 'enum' in shorten['operatorName']:
            error_code = "er-03-01"
            error_name = "value 유효값 누락"
        elif 'maximum' in shorten['operatorName'] or 'minimum' in shorten['operatorName']:
            error_code = "er-03-02"
            error_name = "value 유효범위 오류"
        elif 'bsonType' in shorten['operatorName']:
            if 'type did not match' in shorten['reason']:
                error_code = "er-04-01"
                error_name = "value 타입 오류"
        else:
            error_code = "er-00-00"
            error_name = "unknown error, see detailed logs in validate_logs"

        error['project_name'] = project
        error['schema_name'] = collection_name
        error['json_file'] = file_path
        error['error_field'] = error_field
        error['error_code'] = error_code
        error['error_name'] = error_name
        error['error_msg'] = error_msg
        error['create_dt'] = work_time

        errors.append(error)
        i += 1
    return errors


def extractErrors(ex, project, file_path, collection_name, work_time):
    errors = ex['schemaRulesNotSatisfied']
    log_data = makeErrorData(errors)
    log_data = setNodeType(log_data)
    leaf_nodes = getLeafNodesByGroup(log_data)
    error_logs = getTraverseNodes(log_data, leaf_nodes)
    grouped_logs = flattenLog(error_logs)
    properties_errors = shortenKey(grouped_logs)
    
    detail_errors = makeErrors(
        grouped_logs, 
        properties_errors, 
        project, 
        file_path, 
        collection_name, 
        work_time)
    return detail_errors
    