# 패키지 설치

```
$ pip install -r requirements.txt
```

# 실행 방법

```
$ cd main
$ python check.py --datadir [데이터저장 경로] --dbname [몽고디비 데이터베이스 이름] --collection [컬렉션 이름] --project [프로젝트 이름]
```

# 출력 결과

```
{'project_name': 'test', 'schema_name': 'test_bbox', 'total_cnt': 1, 'pass_file_cnt': 1, 'err_file_cnt': 0, 'err_ratio': 0.0, 'create_dt': '20210820160307', '_id': ObjectId('611f53abd19d68710ce3ea4b')}
```

# 로그 저장

## 1. 로그 파일

./logs/{프로젝트명}/{컬렉션명}/YYYMMDDHHMISS-unixtimestamp.log

해당 디렉토리는 없을 경우 자동 생성됨.

## 2. 정상 데이터 

몽고디비의 컬렉션에 저장

## 3. 에러 데이터

몽고디비의 validate_logs_{project_name} 컬렉션

## 4. 요약 데이터

몽고디비의 validate_logs_sum 컬렉션
