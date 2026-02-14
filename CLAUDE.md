# Spark Pivot Node 구조 분석

## 개요

Spark Pivot 노드는 **Spark GroupBy 노드를 확장**하여 피벗(Pivot) 기능을 추가한 노드다.
독립적인 NodeModel/NodeDialog를 갖지 않고, GroupBy의 `SparkGroupByNodeModel`과 `SparkGroupByNodeDialog`를 `pivotNodeMode=true`로 재사용한다.

- **패키지**: `org.knime.bigdata.spark.node.preproc.pivot`
- **최소 Spark 버전**: 2.0
- **노드 타입**: Manipulator

---

## 클래스 구조

### 1. Pivot 패키지 (`preproc/pivot/`)

| 파일 | 역할 |
|------|------|
| `SparkPivotNodeFactory.java` | 노드 팩토리. `DefaultSparkNodeFactory<SparkGroupByNodeModel>` 상속. `createNodeModel()`에서 `new SparkGroupByNodeModel(true)`, `createNodeDialogPane()`에서 `new SparkGroupByNodeDialog(true)` 반환 |
| `SparkPivotNodeFactory.xml` | 노드 UI 설명 (탭 구성, 옵션 설명) |

### 2. GroupBy 패키지 (`preproc/groupby/`) - Pivot이 재사용하는 핵심 클래스

| 파일 | 역할 |
|------|------|
| `SparkGroupByNodeModel.java` | GroupBy + Pivot 공용 NodeModel. `m_pivotNodeMode` 플래그로 분기 |
| `SparkGroupByNodeDialog.java` | GroupBy + Pivot 공용 Dialog. Pivot 모드일 때 Pivot 탭 추가 |
| `SparkGroupByJobInput.java` | Spark Job 입력 VO. GroupBy 함수 + 집계 함수 + Pivot 설정 포함 |
| `SparkGroupByJobOutput.java` | Spark Job 출력 VO. 결과 스펙 + `pivotValuesDropped` 플래그 |
| `AggregationFunctionSettings.java` | Manual/Pattern/Type 기반 집계 함수 설정 관리 |

### 3. Dialog 패키지 (`preproc/groupby/dialog/`)

| 파일 | 역할 |
|------|------|
| `PivotSettings.java` | Pivot 설정 모델 (컬럼, 모드, 값 목록, 제한, missing 처리) |
| `PivotPanel.java` | Pivot 탭 UI 패널 |
| `PivotValuesPanel.java` | Pivot 값 입력 UI 패널 |
| `AbstractAggregationFunctionRow.java` | 집계 함수 행 추상 클래스 |
| `column/` | Manual Aggregation 관련 패널/로우 |
| `pattern/` | Pattern Based Aggregation 관련 패널/로우 |
| `type/` | Type Based Aggregation 관련 패널/로우 |

---

## 포트 구성

| 포트 | 방향 | 타입 | 설명 |
|------|------|------|------|
| 0 | 입력 | `SparkDataPortObject` | 피벗 대상 Spark DataFrame/RDD |
| 1 | 입력 | `BufferedDataTable` (Optional) | 피벗 값 목록 테이블 |
| 0 | 출력 | `SparkDataPortObject` | 피벗된 결과 DataFrame/RDD |

---

## Pivot 모드 (PivotSettings)

3가지 피벗 값 결정 모드:

| 모드 | 상수 | 설명 |
|------|------|------|
| **Use all values** | `MODE_ALL_VALUES` ("all") | Spark가 자동으로 distinct 값 탐색. DataFrame 물리화 필요 |
| **Use values from data table** | `MODE_INPUT_TABLE` ("inputTable") | 옵션 입력 포트(포트 1)의 테이블에서 값 로드 |
| **Manually specify values** | `MODE_MANUAL_VALUES` ("manual") | 사용자가 직접 값 지정 |

### Pivot 설정 키

| Config Key | 타입 | 기본값 | 설명 |
|------------|------|--------|------|
| `pivot.column` | String | "" | 피벗 컬럼명 |
| `pivot.mode` | String | "all" | 피벗 모드 |
| `pivot.valuesLimit` | Integer | 500 (1~10000) | 최대 피벗 값 수 |
| `pivot.values` | String[] | [] | 수동 지정 피벗 값 |
| `pivot.ignoreMissingValues` | Boolean | true | Missing 값 무시 여부 |
| `pivot.inputValuesTableColumn` | String | "" | 입력 테이블의 피벗 값 컬럼명 |
| `pivot.validateManualValues` | Boolean | false | DataFrame 내 값과 검증 여부 |

---

## 출력 컬럼 네이밍

피벗 결과 컬럼명 형식: `{피벗값}+{집계명}`

집계명은 `ColumnNamePolicy`에 따라 결정:
- **Keep original name(s)**: 원래 컬럼명 유지
- **Aggregation method (column name)**: `method(colName)` 형식
- **Column name (aggregation method)**: `colName(method)` 형식

---

## 실행 흐름

```
SparkPivotNodeFactory
  └─ creates SparkGroupByNodeModel(pivotNodeMode=true)

configureInternal()
  ├─ Spark 버전 체크 (>= 2.0)
  ├─ 집계 함수 목록 구성 (Manual + Pattern + Type)
  ├─ Pivot 컬럼 유효성 검증
  └─ 모드별 출력 스펙 결정
       ├─ auto → null (Spark 결과에서 스펙 획득)
       ├─ manual → createPivotOutputSpec() 로 사전 계산
       └─ inputTable → null (실행 시 결정)

executeInternal()
  ├─ SparkGroupByJobInput 구성
  ├─ PivotSettings.addJobConfig() 로 피벗 설정 주입
  ├─ Spark Job 실행
  └─ 모드별 출력 스펙 결정 후 SparkDataPortObject 반환
```

---

## JobInput 피벗 관련 필드

| 필드 | 설명 |
|------|------|
| `pivotColumn` | 피벗 대상 컬럼명 |
| `pivotComputeValues` | true면 Spark에서 자동 계산 |
| `pivotComputeValuesLimit` | 자동 계산 시 최대 값 수 |
| `pivotValues` | 명시적 피벗 값 배열 |
| `pivotValidateValues` | 명시 값과 실제 DataFrame 값 비교 검증 |
| `pivotIgnoreMissingValues` | Missing 값 무시 여부 |

---

## 핵심 설계 패턴

1. **Flag 기반 모드 분기**: `SparkGroupByNodeModel`과 `SparkGroupByNodeDialog`가 `boolean pivotNodeMode`로 GroupBy/Pivot 동작 분기
2. **입력 포트 차이**: GroupBy는 `[SparkDataPortObject]` 1개, Pivot는 `[SparkDataPortObject, BufferedDataTable(Optional)]` 2개
3. **집계 함수 3단계**: Manual → Pattern → Type 순서로 적용, 앞 단계에서 선택된 컬럼은 뒷 단계에서 제외
4. **출력 스펙 지연 결정**: auto/inputTable 모드에서는 configure 시 null 반환, execute 후 Spark 결과에서 스펙 획득
