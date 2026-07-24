
# 구축 과정과 기술적 의사결정

## 1. 요구사항 정의

초기 요구사항은 다음과 같았습니다.

- SAP HANA 데이터를 BigQuery에 정기 적재
- 테이블마다 서로 다른 실행 시간 지원
- 최근 N일 데이터 재조회
- 매월 전월 데이터 재조회
- 실패 재시도와 실행 이력 관리
- 신규 테이블 추가 시 코드 수정 최소화
- HANA 접속 정보를 코드에 저장하지 않음

## 2. 공통 로더 설계

처음에는 테이블별 Cloud Run Job을 만드는 방식을 검토했지만, 테이블이 늘어날 때마다 다음 작업이 반복되는 문제가 있었습니다.

- Python 코드 복사
- 환경변수 추가
- Docker 이미지 재빌드
- Cloud Run Job 생성
- DAG 코드 수정

이를 해결하기 위해 하나의 `main.py`가 환경변수와 YAML 설정을 읽어 여러 테이블을 처리하도록 변경했습니다.

```text
TABLE_ID
RUN_NAME
RUN_DATE
CONFIG_URI
```

Cloud Run Job은 위 값에 따라 실행할 테이블과 실행 방식을 선택합니다.

## 3. 설정과 SQL 분리

테이블마다 달라지는 값을 YAML로 분리했습니다.

- HANA SQL
- BigQuery 대상
- 날짜 기준 컬럼
- 조회 기간
- 실행 스케줄
- 적재 전략
- 빈 데이터 허용 여부

처음에는 하나의 `tables.yaml`에서 모든 테이블을 관리했습니다.

하지만 여러 사람이 동시에 관리하면 전체 파일을 내려받아 수정해야 하고 충돌이 발생할 수 있어, 최종적으로 테이블별 YAML 구조로 변경했습니다.

```text
configs/TABLE_A.yaml
configs/TABLE_B.yaml
```

## 4. Docker 및 Cloud Run 구성

공통 로더를 Docker 이미지로 패키징하고 Cloud Run Job으로 실행했습니다.

```text
Docker image
├─ main.py
├─ Python dependencies
└─ pipeline loader
```

Cloud Run Job에는 다음 실행 환경을 구성했습니다.

- HANA 접속 환경변수
- Secret Manager 비밀번호 연결
- BigQuery 접근 서비스 계정
- Composer 버킷 읽기 권한
- HANA 접근용 VPC 및 Subnet
- 실행 제한 시간

이미지는 공통 로더 코드가 변경될 때만 다시 빌드합니다.  
테이블 YAML이나 SQL 변경에는 이미지 빌드가 필요하지 않습니다.

## 5. BigQuery 적재 방식

데이터를 Target 테이블에 바로 쓰지 않고 임시 Staging 테이블을 사용했습니다.

```text
HANA
  → _stg_TABLE_UUID
  → 건수 검증
  → 날짜 구간 교체
  → Target
```

Staging 테이블에는 만료 시간을 설정하여 실패 후 남은 임시 테이블도 자동 정리되도록 했습니다.

적재되는 원본 컬럼은 초기 스키마 변동에 대응하기 위해 `STRING`으로 통일했습니다.

추가 메타데이터:

```text
_loaded_at
_run_name
_run_date
_load_start_yyyymmdd
_load_end_yyyymmdd
```

## 6. Cloud Composer 환경 구성

Google Cloud의 관리형 Airflow 서비스인 Cloud Composer를 사용했습니다.

구성 내용:

- Cloud Composer 환경 생성
- Airflow Pool 생성
- Cloud Run 실행 Operator 적용
- Deferrable Operator 사용
- 서울 시간대 스케줄 설정
- 재시도 정책 설정

Airflow가 Cloud Run Job의 성공·실패 상태를 기다리고, 실패하면 정해진 정책에 따라 재시도하도록 구성했습니다.

## 7. 동적 DAG Factory 구현

처음에는 하나의 `hana_bq_daily` DAG 내부에 여러 테이블 Task를 생성했습니다.

하지만 이 방식은 테이블마다 서로 다른 시간을 지정할 수 없었습니다. Airflow 스케줄은 Task가 아니라 DAG 단위로 적용되기 때문입니다.

최종적으로 YAML의 각 `run`을 별도 DAG로 생성하도록 변경했습니다.

```text
TABLE_A daily   → hana_bq_table_a_daily
TABLE_A monthly → hana_bq_table_a_monthly
TABLE_B daily   → hana_bq_table_b_daily
```

이를 통해 테이블마다 서로 다른 실행 시간을 지정할 수 있게 됐습니다.

## 8. 외부 설정 실시간 반영

초기 Cloud Run 이미지는 컨테이너 내부의 YAML과 SQL을 읽었습니다.

```text
/app/pipeline/tables.yaml
/app/pipeline/queries/TABLE.sql
```

이 방식은 설정을 변경할 때마다 이미지를 다시 빌드해야 했습니다.

이를 Composer Cloud Storage의 외부 파일을 읽는 방식으로 변경했습니다.

```text
gs://COMPOSER_BUCKET/dags/hana_bq/configs/
gs://COMPOSER_BUCKET/dags/hana_bq/queries/
```

이후 YAML과 SQL 변경에는 다음 작업이 필요하지 않습니다.

- Docker 재빌드
- Cloud Run Job 업데이트
- DAG 코드 수정

## 9. 구축 과정에서 해결한 문제

| 문제 | 원인 | 해결 |
|---|---|---|
| 빈 BigQuery 테이블에서 스키마 오류 | 컬럼이 없는 Target과 Staging 스키마가 다름 | Target이 없거나 스키마가 없을 때 Staging 기준으로 생성 |
| DAG Broken 오류 | Operator가 지원하지 않는 `verbose` 인자 사용 | Composer Provider 버전에 맞게 인자 제거 |
| 특정 테이블 실행 실패 | YAML의 `enabled: false` | 설정 검증 후 활성화 |
| GCS 설정을 읽지 못함 | `CONFIG_URI`가 로컬 경로로 잘못 설정됨 | 전체 `gs://` URI 전달 |
| 실행 버튼을 눌러도 대기 | 기존 작업 재시도 및 Pool 제한 | Task 상태와 Pool 점유 상태 확인 |
| 테이블별 스케줄 미적용 | 여러 Task가 하나의 DAG 스케줄 공유 | 실행 설정별 독립 DAG 생성 |
| YAML 동시 수정 불편 | 하나의 중앙 YAML 사용 | 테이블별 YAML로 분리 |

## 10. 최종 결과

최종 사용자는 다음 두 파일만 작성합니다.

```text
configs/테이블명.yaml
queries/테이블명.sql
```

파일을 Composer 버킷에 업로드하면 Airflow가 스케줄을 읽어 DAG를 생성하고, 지정한 시간에 Cloud Run Job을 실행합니다.

개발자는 새로운 테이블이 추가될 때마다 로더 코드나 DAG를 수정할 필요가 없습니다.
