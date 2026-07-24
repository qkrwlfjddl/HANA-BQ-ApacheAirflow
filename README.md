# HANA-BQ-CloudComposer


## 프로젝트 구조

```text
dags/
├─ hana_bq_dag.py
└─ hana_bq/
   ├─ configs/
   │  ├─ ZTIF9001.yaml
   │  └─ ZTIF9009.yaml
   └─ queries/
      ├─ ZTIF9001.sql
      └─ ZTIF9009.sql
```

### 주요 파일 설명

* `main.py`
  HANA 데이터를 조회하고 BigQuery에 적재하는 파이프라인 실행 파일

* `Dockerfile`
  파이프라인을 컨테이너 환경에서 실행하기 위한 Docker 설정 파일

* `requirements.txt`
  Python 패키지 및 라이브러리 목록

* `pipeline/tables.yaml`
  적재 대상 HANA 테이블과 BigQuery 테이블 정보를 관리하는 설정 파일

* `pipeline/queries/`
  HANA 테이블별 조회 SQL 파일을 관리하는 디렉터리

* `dags/hana_bq_dag.py`
  HANA → BigQuery 적재 작업을 자동으로 실행하는 Airflow DAG 파일

* `tests/`
  파이프라인 테스트 코드를 관리하는 디렉터리

* `README.md`
  프로젝트 구성, 실행 방법, 배포 방법 등을 설명하는 문서
