#-*- coding:utf-8 -*-
import argparse
import os

import process_file


def main():
    parser = argparse.ArgumentParser(description='데이터 검증')

    parser.add_argument('--project', type=str, required=True,
                    help='프로젝트명')
    parser.add_argument('--datadir', type=str, required=True,
                    help='JSON 데이터가 저장된 디렉토리 path')
    parser.add_argument('--dbname', type=str, required=True,
                    help='데이터베이스 이름')
    parser.add_argument('--collection_name', type=str, required=True,
                    help='JSON 데이터를 저장할 컬렉션 이름')
    
    args = parser.parse_args()

    project = args.project
    datadir = args.datadir
    dbname = args.dbname
    collection_name = args.collection_name
    
    log_path = "./logs/{}/{}".format(project, collection_name)
    result = process_file.process(project, datadir, dbname, collection_name, log_path)
    print(result)


if __name__ == "__main__":
    main()