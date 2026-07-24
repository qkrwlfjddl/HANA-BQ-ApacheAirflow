# 신규 테이블 등록 가이드

> Cloud Composer(Airflow)가 테이블별 YAML 설정을 읽고,
> Cloud Run Job을 실행하여 SAP HANA 데이터를 BigQuery에 적재합니다.

[← 메인 README로 돌아가기](../README.md)

---

## 전체 파일 구조 
```text
YAML 스케줄 → Airflow → Cloud Run → HANA SQL 실행 → BigQuery 적재
```
```text
dags/
├─ hana_bq_dag.py
└─ hana_bq/
   ├─ configs/
   │  ├─ table1.yaml
   │  └─ table2.yaml
   └─ queries/
      ├─ table1.sql
      └─ table2.sql
```
      
## 테이블 추가 방법

1. SQL 작성
2. YAML 작성
3. Composer에 업로드

테이블마다 다음 두 파일을 생성하고 Composer에 업로드합니다.

```text
configs/테이블명.yaml
queries/테이블명.sql
```
-----------------------

### 1. SQL 작성

예시: `pipeline/queries/ZTIF1234.sql`

```sql
SELECT *
FROM "HANA_SCHEMA"."ZTIF1234"
```

날짜 조건(INTEG_RDATE)은 공통 로더가 자동으로 추가합니다. 
❗SQL문에 추가할 필요없음

```sql
SELECT *
FROM (
    -- 작성한 SQL
) SRC
WHERE SRC.RDATE >= 시작일 -- yaml에서 설정한 날짜 기준
  AND SRC.RDATE < 종료일
```

-------------------------

### 2. YAML 작성

예시: `pipeline/configs/ZTIF1234.yaml`

```yaml
version: 1

defaults:
  schema_mode: raw_string
  chunk_size: 50000

tables:
  - id: ZTIF1234
    enabled: true
    query_file: ../queries/ZTIF1234.sql

    bq_project: bigquery
    bq_dataset: HANABQ
    bq_table: ZTIF1234

    window_column: INTEG_RDATE
    window_format: yyyymmdd

    runs:
      # 일별 기준으로 2일치의 데이터 업데이트
      - name: daily -- 일별 
        schedule: "0 5 * * *" -- 매일 오전 5시
        load_strategy: window_replace -- 날짜기준
        allow_empty: true
        window:
          type: rolling_days
          days: 2 -- 2일치

      # 월별 기준으로 이전 1개월치의 데이터 업데이트
      - name: monthly
        schedule: "0 6 10 * *" -- 매월 10일 오전 6시
        load_strategy: window_replace
        allow_empty: true
        window:
          type: previous_month 
```

❗YAML 파일명과 `id`(HANADB 테이블명)은 동일하게 작성합니다.

```text
ZTIF1234.yaml → id: ZTIF1234
```

<details>
<summary>YAML 설정 설명</summary>

| 설정 | 의미 |
|---|---|
| `id` | 테이블 ID이며 YAML 파일명과 동일하게 작성 |
| `enabled` | `true`: 실행, `false`: 실행 중지 |
| `query_file` | SQL 경로 (`../queries/테이블명.sql`) |
| `bq_project` | BigQuery 프로젝트 |
| `bq_dataset` | BigQuery 데이터세트 |
| `bq_table` | BigQuery 테이블 |
| `window_column` | 날짜 범위를 적용할 컬럼 |
| `schedule` | 한국 시간 기준 실행 스케줄 |
| `load_strategy` | 날짜 구간 삭제 후 다시 적재하는 방식 |
| `allow_empty` | `true`이면 조회 결과 0건도 정상 처리 |
| `window.type` | `rolling_days` 또는 `previous_month` |
| `window.days` | 실행일을 포함해 다시 조회할 일수 |

`allow_empty: true`인 상태에서 조회 결과가 0건이면 해당 날짜 구간의 BigQuery 데이터도 비워질 수 있습니다.

## 스케줄 예시

스케줄은 한국 시간 기준입니다.

```yaml
schedule: "0 2 * * *"    # 매일 오전 2시
schedule: "0 5 * * *"    # 매일 오전 5시
schedule: "0 6 10 * *"   # 매월 10일 오전 6시
```

최근 N일 재조회:

```yaml
window:
  type: rolling_days
  days: 4
```

전월 전체 재조회:

```yaml
window:
  type: previous_month
```
</details>

-------------

### 3. Composer에 업로드

1. GCP 콘솔에서 `Cloud Composer → hana-bq-composer → DAG 폴더`로 이동합니다.
2. YAML 파일을 `dags/hana_bq/configs/`에 업로드합니다.
3. SQL 파일을 `dags/hana_bq/queries/`에 업로드합니다.
4. 업로드 후 약 1~2분 뒤 Airflow에 다음 DAG가 자동 생성됩니다.

----------------------
## [끝] BigQuery 적재 결과를 확인합니다.
