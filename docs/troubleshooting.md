# 운영 및 장애 대응

## DAG가 나타나지 않는 경우

확인 항목:

- YAML이 `dags/hana_bq/configs/`에 있는지
- YAML 파일명과 `id`가 동일한지
- `enabled: true`인지
- Airflow에 Broken DAG 오류가 있는지


## Cloud Run Job이 실행되지 않는 경우

Airflow Task 상태를 확인합니다.

| 상태 | 의미 |
|---|---|
| `queued` | Pool 또는 Worker 자리를 기다리는 중 |
| `running` | 실행 중 |
| `deferred` | Cloud Run 완료를 비동기로 기다리는 중 |
| `up_for_retry` | 실패 후 재시도 대기 중 |
| `failed` | 재시도 후 최종 실패 |
| `success` | 정상 완료 |

`hana_extract_pool`의 사용 가능한 Slot도 확인합니다.

## CONFIG_URI 오류

다음과 같은 오류가 발생할 수 있습니다.

```text
FileNotFoundError: /hana_bq/configs/TABLE_A.yaml
```

`CONFIG_URI`가 `/hana_bq/...`가 아니라 전체 GCS 주소인지 확인합니다.

```text
gs://COMPOSER_BUCKET/dags/hana_bq/configs/TABLE_A.yaml
```

## 테이블이 비활성화된 경우

```text
ValueError: TABLE_ID is disabled
```

YAML을 확인합니다.

```yaml
enabled: true
```

## 스키마가 다른 경우

```text
Target and staging schemas are different
```

HANA SQL의 조회 컬럼과 기존 BigQuery 테이블의 컬럼을 비교합니다.

다음 변경이 있었는지 확인합니다.

- 컬럼 추가
- 컬럼 삭제
- 컬럼명 변경
- 컬럼 순서 변경

스키마 변경은 자동 적용하지 않고 검토 후 반영합니다.

## Staging 테이블이 남아 있는 경우

실패한 실행의 Staging 테이블은 디버깅을 위해 남을 수 있습니다.

```text
_stg_TABLE_UUID
```

Staging 테이블에는 만료 시간이 설정되어 있어 일정 시간 후 자동 삭제됩니다.

## 데이터가 0건인 경우

```yaml
allow_empty: true
```

0건도 정상 처리되며 대상 날짜 구간이 비워질 수 있습니다.

```yaml
allow_empty: false
```

작업이 실패하고 기존 Target 데이터는 유지됩니다.

## 확인할 로그

장애 발생 시 다음 순서로 확인합니다.

1. Airflow DAG Run 상태
2. Airflow Task 로그
3. Cloud Run Job 실행 로그
4. `CONFIG_URI`, `TABLE_ID`, `RUN_NAME`, `RUN_DATE`
5. 조회 날짜 범위
6. Staging 적재 건수
7. BigQuery Target 결과
