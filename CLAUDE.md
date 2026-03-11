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

---
---

# Spark Unpivot Node (신규 개발)

## 개요

Spark DataFrame을 wide format → long format으로 변환하는 Unpivot(Melt) 노드.
Spark 3.4+ 의 `Dataset.unpivot()` API를 사용한다.
기존 Pivot 노드와 달리 **독립적인 3개 플러그인**으로 구성.

- **노드명**: Spark Unpivot(Hyim)
- **최소 Spark 버전**: 3.4
- **노드 타입**: Manipulator

---

## 플러그인 구조

| 플러그인 | 역할 |
|----------|------|
| `org.knime.bigdata.spark.dx.node` | 노드 UI (Factory, Model, Dialog, Settings, JobInput/Output) |
| `org.knime.bigdata.spark3_4.dx` | Spark 3.4용 Job 구현 |
| `org.knime.bigdata.spark3_5.dx` | Spark 3.5용 Job 구현 |

---

## 클래스 구조

### Node 레이어 (`org.knime.bigdata.spark.dx.node.preproc.unpivot`)

| 파일 | 역할 |
|------|------|
| `SparkUnpivotNodeFactory.java` | `DefaultSparkNodeFactory<SparkUnpivotNodeModel>` 상속. 카테고리: "row" |
| `SparkUnpivotNodeFactory.xml` | 노드 설명 (4탭: Retained Columns, Value Columns, Options, Validation) |
| `SparkUnpivotNodeModel.java` | 노드 모델. configure에서 유효성 검증, execute에서 Spark Job 실행 |
| `SparkUnpivotNodeDialog.java` | `DataAwareNodeDialogPane` 상속. 4탭 UI, 타입 표시, 행 수 추정, 변수 매핑, 정렬, Validation Check |
| `SparkUnpivotSettings.java` | 설정 모델 (retainedColumns, valueColumns, variableColName, valueColName, skipMissingValues, castToString, sortOption, variableValueMap) |
| `SparkUnpivotJobInput.java` | Job 입력 VO. `JobInput` 상속. validateOnly/sortOption/varMap 지원 |
| `SparkUnpivotJobOutput.java` | Job 출력 VO. `JobOutput` 상속. previewData/inputRowCount 포함 |

### Spark Job 레이어 (spark3_4 / spark3_5 동일 구조)

| 파일 | 역할 |
|------|------|
| `UnpivotJob.java` | `SparkJob` 구현. `Dataset.unpivot()` 호출. castToString, 변수 매핑, 정렬, validateOnly 지원 |
| `UnpivotJobRunFactory.java` | Job 실행 팩토리 |
| `UnpivotJobRunFactoryProvider.java` | SPI 프로바이더 |

---

## 포트 구성

| 포트 | 방향 | 타입 | 설명 |
|------|------|------|------|
| 0 | 입력 | `SparkDataPortObject` | unpivot 대상 Spark DataFrame |
| 0 | 출력 | `SparkDataPortObject` | long format 결과 DataFrame |

---

## 설정 항목

| Config Key | 타입 | 기본값 | 설명 |
|------------|------|--------|------|
| `retainedColumns` | FilterString | [] | 유지할 ID 컬럼 목록 |
| `valueColumns` | FilterString | [] | unpivot 대상 값 컬럼 목록 |
| `variableColName` | String | "variable" | 출력 variable 컬럼명 |
| `valueColName` | String | "value" | 출력 value 컬럼명 |
| `skipMissingValues` | Boolean | true | null 값 행 제외 여부 |
| `castToString` | Boolean | false | 모든 value 컬럼을 String으로 캐스팅 |
| `sortOption` | String | "none" | 출력 정렬 옵션 (none / retained / variable) |
| `variableValueMap` | Map (keys+values 배열) | {} | variable 컬럼 값 커스텀 매핑 |

---

## 유효성 검증 (Dialog OK + NodeModel configure 양쪽)

| 검증 | 에러 메시지 |
|------|------------|
| retained 컬럼 미선택 | "No retained columns selected." |
| value 컬럼 미선택 | "No value columns selected." |
| retained/value 중복 선택 | "The following columns are selected as both retained and value columns: ..." |
| variable 컬럼명 빈값 | "Variable column name must not be empty." |
| value 컬럼명 빈값 | "Value column name must not be empty." |
| variable = value 컬럼명 동일 | "Variable column name and Value column name must be different." |
| 출력 컬럼명이 retained 컬럼명과 충돌 | "Variable/Value column name '...' conflicts with a retained column name." |
| 타입 혼합 (숫자+문자열) + cast OFF | "Value columns have incompatible types: ... Enable 'Cast all value columns to String' option." |

---

## 실행 흐름

```
SparkUnpivotNodeFactory
  └─ creates SparkUnpivotNodeModel

configureInternal()
  ├─ retained/value 컬럼 존재 여부 검증
  ├─ 중복/빈값/충돌 검증
  ├─ 타입 호환성 검증 (castToString OFF 시)
  └─ 출력 스펙 생성: [retained 컬럼들] + [variable: String] + [value: String]

executeInternal()
  ├─ SparkUnpivotJobInput 구성 (sortOption, varMap 포함)
  ├─ SparkContextUtil.getJobRunFactory() 로 Job 실행
  └─ Spark Job 결과 스펙으로 SparkDataPortObject 반환

UnpivotJob.runJob() (Spark 측)
  ├─ castToString=true → value 컬럼들 cast(StringType)
  ├─ Dataset.unpivot(idCols, valCols, variableColName, valueColName)
  ├─ varMap 적용 → when/otherwise로 variable 컬럼 값 치환
  ├─ skipMissing=true → filter(valueCol.isNotNull())
  ├─ sortOption 적용 (retained/variable/none)
  ├─ validateOnly=true → inputFrame.count() + result.showString(5) 반환
  └─ 결과 DataFrame + IntermediateSpec 반환
```

---

## 다이얼로그 기능

### 탭 구성
- **Tab 1 - Retained Columns**: `DialogComponentColumnFilter`로 유지 컬럼 선택 (타입 표시)
- **Tab 2 - Value Columns**: `DialogComponentColumnFilter`로 값 컬럼 선택 (타입 표시)
- **Tab 3 - Options**: 출력 설정, 정렬, 행 수 추정, 변수 값 매핑
- **Tab 4 - Validation**: Check 버튼으로 샘플 데이터 미리보기

### 컬럼 타입 표시
- Retained/Value 컬럼 필터에 `columnName (Type)` 형식으로 데이터 타입 표시
- `installTypeRenderers()`로 JList에 커스텀 ListCellRenderer 적용

### 출력 행 수 추정
- 다이얼로그 열 때 백그라운드(SwingWorker)로 입력 행 수 자동 조회
- `입력 행 수 × value 컬럼 수 = 예상 출력 행 수` 표시
- Skip missing values ON 시 "(max - actual may be less due to Skip missing values)" 안내

### 출력 정렬 옵션
- No sorting (기본) / Sort by retained columns / Sort by variable column

### 변수 값 매핑 (Variable Value Mapping)
- JTable로 각 value 컬럼의 variable 값을 커스텀 이름으로 변경 가능
- 기본값: 컬럼명 자체 (변경하지 않으면 저장하지 않음)

### Validation Check
- `DataAwareNodeDialogPane` 상속하여 PortObject 접근
- Check 버튼 → SwingWorker로 validate-only Spark Job 실행
- 성공 시 초록색 + 샘플 데이터 5행 표시 / 실패 시 빨간색 에러
- 성공 시 입력 행 수도 갱신되어 행 수 추정에 반영

### syncFilterModels
- `DialogComponentColumnFilter`는 saveSettingsTo() 호출 전까지 SettingsModel을 UI와 동기화하지 않음
- `syncFilterModels()`: 임시 NodeSettings에 saveSettingsTo() 호출하여 강제 동기화
- validateSettings(), runValidation(), updateRowEstimate(), updateVarMapTable() 등에서 사용

---

## 테스트 완료 항목

- [x] 기본 unpivot 실행 (동일 타입 value 컬럼)
- [x] retained 미선택 → 다이얼로그 에러
- [x] value 미선택 → 다이얼로그 에러
- [x] retained/value 중복 → 다이얼로그 에러
- [x] 혼합 타입 + cast OFF → 다이얼로그 에러 (Spark 실행 전 차단)
- [x] 혼합 타입 + cast ON → 정상 실행
- [x] variable/value 컬럼명 동일 → 다이얼로그 에러
- [x] 출력 컬럼명 vs retained 컬럼명 충돌 → 다이얼로그 에러
- [x] 빈 DataFrame (0행) → 정상 (0행 결과)
- [x] 전부 null + skipMissing ON → 0행 결과
- [x] 전부 null + skipMissing OFF → null 포함 행 출력
- [x] value 컬럼 1개만 선택 → 정상
- [x] 설정 저장/재로드 → 설정값 유지
- [x] upstream 컬럼 변경 → 적절한 에러 메시지
- [x] 노드 Reset → 재실행 동일 결과
- [x] Integer + Double (cast OFF) → Spark 자동 변환 정상
- [x] Long Integer + Double (cast OFF) → Spark 자동 변환 정상
- [x] Sort by retained / Sort by variable → 정상 정렬
- [x] Variable Value Mapping → 커스텀 이름 적용 정상
- [x] Validation Check → 샘플 데이터 미리보기 정상
- [x] 행 수 추정 → 다이얼로그 열 때 자동 조회 정상
- [x] Skip missing values + 행 수 추정 안내 정상
- [x] 처음 사용 시 → syncFilterModels로 정상 동작

---
---

# Spark Multi Query Node (신규 개발)

## 개요

선택한 여러 컬럼에 동일한 SQL 표현식 템플릿을 적용하여 변환하는 노드.
`$columnS` 플레이스홀더가 각 대상 컬럼명으로 치환되어 Spark SQL로 실행된다.

- **노드명**: Spark Multi Query(Hyim)
- **최소 Spark 버전**: 3.4
- **노드 타입**: Manipulator

---

## 플러그인 구조

| 플러그인 | 역할 |
|----------|------|
| `org.knime.bigdata.spark.dx.node` | 노드 UI (Factory, Model, Dialog, Settings, JobInput/Output) |
| `org.knime.bigdata.spark3_4.dx` | Spark 3.4용 Job 구현 |
| `org.knime.bigdata.spark3_5.dx` | Spark 3.5용 Job 구현 |

---

## 클래스 구조

### Node 레이어 (`org.knime.bigdata.spark.dx.node.sql.multiquery`)

| 파일 | 역할 |
|------|------|
| `SparkMultiQueryNodeFactory.java` | 노드 팩토리. 카테고리: "sql" |
| `SparkMultiQueryNodeFactory.xml` | 노드 설명 (2탭: Column Selection, Expression & Options) |
| `SparkMultiQueryNodeModel.java` | 노드 모델. configure에서 유효성 검증 (keepOriginal+패턴 충돌, 별칭 충돌 포함), execute에서 Job 실행 |
| `SparkMultiQueryNodeDialog.java` | `DataAwareNodeDialogPane` 상속. 2탭 UI, 템플릿 드롭다운, Keep Original, Output Pattern, SQL Preview, 선택 컬럼 요약, 개선된 Check (전체+개별 컬럼 테스트 + 샘플 데이터) |
| `SparkMultiQuerySettings.java` | 설정 모델 (targetColumns, sqlExpression, keepOriginalColumns, outputColumnPattern) |
| `SparkMultiQueryJobInput.java` | Job 입력 VO. validateOnly/keepOriginal/outputPattern 지원 |
| `SparkMultiQueryJobOutput.java` | Job 출력 VO. previewData 포함 |

### Spark Job 레이어 (spark3_4 / spark3_5 동일 구조)

| 파일 | 역할 |
|------|------|
| `MultiQueryJob.java` | SparkJob 구현. temp view 등록 → SELECT 생성 (keepOriginal/outputPattern 반영) → 실행. validateOnly시 LIMIT 5 + showString |
| `MultiQueryJobRunFactory.java` | Job 실행 팩토리 |
| `MultiQueryJobRunFactoryProvider.java` | SPI 프로바이더 |

---

## 포트 구성

| 포트 | 방향 | 타입 | 설명 |
|------|------|------|------|
| 0 | 입력 | `SparkDataPortObject` | 변환 대상 Spark DataFrame |
| 0 | 출력 | `SparkDataPortObject` | SQL 표현식 적용된 결과 DataFrame |

---

## 설정 항목

| Config Key | 타입 | 기본값 | 설명 |
|------------|------|--------|------|
| `targetColumns` | FilterString | [] | SQL 표현식을 적용할 대상 컬럼 목록 |
| `sqlExpression` | String | "string($columnS)" | $columnS 플레이스홀더 포함 SQL 표현식 |
| `keepOriginalColumns` | Boolean | false | true 시 원본 컬럼 유지하고 변환 컬럼을 새로 추가 |
| `outputColumnPattern` | String | "$columnS" | 출력 컬럼 이름 패턴. $columnS가 컬럼명으로 치환됨 |

---

## 실행 흐름

```
SparkMultiQueryNodeFactory
  └─ creates SparkMultiQueryNodeModel

configureInternal()
  ├─ 대상 컬럼 존재 여부 검증
  ├─ SQL 표현식 빈값/플레이스홀더 검증
  ├─ Output Pattern 빈값/플레이스홀더 검증
  ├─ keepOriginal=true + pattern="$columnS" → 중복 컬럼명 에러
  ├─ keepOriginal=true → 별칭이 기존 비대상 컬럼명과 충돌 검사
  └─ 출력 스펙 null (SQL 결과 타입은 실행 시 결정)

executeInternal()
  ├─ SparkMultiQueryJobInput 구성 (keepOriginal, outputPattern 포함)
  ├─ SparkContextUtil.getJobRunFactory() 로 Job 실행
  └─ Spark Job 결과 스펙으로 SparkDataPortObject 반환

MultiQueryJob.runJob() (Spark 측)
  ├─ 입력 DataFrame을 temp view 등록
  ├─ validateOnly=true → 전체 대상 컬럼으로 테스트 쿼리 (LIMIT 5) 실행 + showString
  ├─ SELECT 절 구성:
  │   ├─ 대상 컬럼 (keepOriginal=false): expr(`col`) AS `alias`
  │   ├─ 대상 컬럼 (keepOriginal=true): `col`, expr(`col`) AS `alias`
  │   └─ 비대상 컬럼: `col` (그대로 유지)
  ├─ spark.sql(query) 실행
  ├─ temp view 정리 (finally)
  └─ 결과 DataFrame + IntermediateSpec 반환
```

---

## 다이얼로그 기능

### 탭 구성
- **Tab 1 - Column Selection**: `DialogComponentColumnFilter`로 대상 컬럼 선택 (타입 표시)
- **Tab 2 - Expression & Options**: 선택 컬럼 요약, 템플릿 드롭다운, SQL 표현식, 옵션, SQL 프리뷰, Check

### 선택 컬럼 요약 (Target Columns)
- Expression & Options 탭 상단에 현재 선택된 컬럼 요약 표시
- `"3 column(s): age (Integer), name (String), salary (Double)"` 형식

### 컬럼 타입 표시
- Column Selection 필터에 `columnName (Type)` 형식으로 데이터 타입 표시

### 표현식 템플릿 드롭다운
11개 프리셋 제공 (양방향 동기화 - 드롭다운↔텍스트영역):
- Cast to String: `string($columnS)`
- Cast to Integer: `CAST($columnS AS INT)`
- Cast to Double: `CAST($columnS AS DOUBLE)`
- Uppercase: `UPPER($columnS)`
- Lowercase: `LOWER($columnS)`
- Trim: `TRIM($columnS)`
- Replace NULL with 0: `COALESCE($columnS, 0)`
- Replace NULL with empty: `COALESCE($columnS, '')`
- Parse Date (yyyyMMdd): `TO_DATE(string($columnS), 'yyyyMMdd')`
- Regex Replace (non-digits): `REGEXP_REPLACE($columnS, '[^0-9]', '')`
- Round to 2 decimals: `ROUND($columnS, 2)`

### Keep Original Columns
- 체크 시 원본 대상 컬럼 유지 + 변환된 컬럼을 새로 추가
- Output Pattern이 `$columnS`와 같으면 중복 에러

### Output Column Pattern
- `$columnS` 포함 필수 (예: `$columnS_str`, `$columnS_new`)
- keepOriginal=false 시 기본값 `$columnS`는 원래 컬럼 이름 유지

### SQL Preview
- 실시간 프리뷰 (DocumentListener 기반)
- 현재 설정 기반으로 생성될 SELECT 절을 미리 표시

### Validation Check
- `DataAwareNodeDialogPane` 상속하여 PortObject 접근
- Check 버튼 클릭 시:
  1. 로컬 검증 (컬럼 선택, 표현식 비어있지 않음, $columnS 포함)
  2. SwingWorker로 백그라운드 Spark Job 실행 (validateOnly=true)
  3. **전체 컬럼 테스트**: 모든 대상 컬럼을 한 번에 테스트
  4. **실패 시 개별 컬럼 테스트**: 어떤 컬럼이 실패했는지 식별
  5. 성공 → 초록색 + 샘플 데이터 5행 표시 / 실패 → 빨간색 (실패 컬럼 목록 + 에러)
- upstream 노드 미실행 시 "Execute the upstream node first" 에러

### syncFilterModel
- `DialogComponentColumnFilter`는 saveSettingsTo() 호출 전까지 SettingsModel을 UI와 동기화하지 않음
- `syncFilterModel()`: 임시 NodeSettings에 saveSettingsTo() 호출하여 강제 동기화
- updateSelectedColumnsInfo(), updatePreview(), runValidation(), saveSettingsTo()에서 사용

---

## 테스트 완료 항목

- [x] 기본 string 변환: `string($columnS)` → 정상
- [x] CAST 변환, COALESCE, UPPER, TRIM 등 → 정상
- [x] 단일/다수/전체 컬럼 선택 → 정상
- [x] 비대상 컬럼 보존 → 정상
- [x] 대상 컬럼 미선택 OK → 에러
- [x] SQL 표현식 빈값/플레이스홀더 미포함 OK → 에러
- [x] Output Pattern 빈값/플레이스홀더 미포함 OK → 에러
- [x] keepOriginal + 기본 패턴 → 중복 에러
- [x] Check 성공 → 초록색 + 샘플 데이터 표시
- [x] Check 실패 → 빨간색 + 실패 컬럼 식별
- [x] 템플릿 드롭다운 양방향 동기화 → 정상
- [x] keepOriginal ON + 패턴 → 원본 유지 + 새 컬럼 추가
- [x] SQL Preview 실시간 업데이트 → 정상
- [x] 설정 저장/재로드 → 설정값 유지
- [x] 빈 DataFrame (0행) → 정상
- [x] 특수문자 컬럼명 → 백틱 이스케이프 정상
- [x] 처음 사용 시 → syncFilterModel로 정상 동작
- [x] 선택 컬럼 요약 표시 → 정상

---

## 공통 기술 패턴

### DialogComponentColumnFilter 모델 동기화 문제
- KNIME의 `DialogComponentColumnFilter`는 `saveSettingsTo()` 호출 시에만 내부 `SettingsModelFilterString`을 UI 패널과 동기화함
- 다이얼로그를 처음 열거나 탭을 전환한 뒤 `getIncludeList()`를 호출하면 빈 리스트 반환될 수 있음
- **해결**: `syncFilterModels()` 헬퍼 메서드로 임시 NodeSettings에 saveSettingsTo() 호출하여 강제 동기화
- Unpivot / Multi Query 양쪽 노드에 적용

### JobOutput Number 직렬화 문제
- KNIME `JobOutput.set(key, value)`에서 `long`/`Number` 타입 직접 저장 불가
- `"Instance of Number not supported. Use dedicated methods"` 에러 발생
- **해결**: `String.valueOf(long)`으로 변환하여 저장, 읽을 때 `Long.parseLong()` 사용

### DialogComponentColumnFilter 최초 오픈 시 기본 배치 (Available 우선)
- 노드를 처음 열 때 (OK 클릭 전) 모든 컬럼이 Available(exclude) 쪽에 보이도록 하려면 `freshSettings` 방식 사용
- `CFG_CONFIGURED` 플래그(Settings 클래스에서 OK 클릭 시에만 저장)와 `m_everSavedWithOk`(Dialog 인스턴스 필드, 현 세션에서 OK 클릭 시 `true`)를 AND 조건으로 신선도 판단
- **조건**: `if (m_everSavedWithOk || settings.containsKey(CFG_CONFIGURED))` → 기존 설정 로드; 그 외 → freshSettings 생성
- freshSettings: 현재 spec의 모든 컬럼을 exclude 리스트에 넣은 `NodeSettings("defaults")`를 직접 만들어 `loadSettingsFrom()` 호출
- `SettingsModelFilterString` 두 번째 파라미터(`inclModeDefault=false`)는 EnforceExclusion; 알 수 없는 컬럼은 Available로 이동
- Unpivot: retainedColumns, valueColumns 양쪽 모두 freshSettings 적용
- Multi Query: targetColumns에 freshSettings 적용

### DialogComponentColumnFilter 컬럼 목록 비가시 문제 (Swing 레이아웃 타이밍)
- `DataAwareNodeDialogPane.loadSettingsFrom()`은 다이얼로그 표시 전에 호출됨
- 이 시점에서 내부 JScrollPane이 크기 0이기 때문에 동기적 `revalidate()` + `repaint()`는 효과 없음
- **증상**: Available 패널에 데이터는 존재하지만 화면에 보이지 않음
- **해결**: `SwingUtilities.invokeLater`로 EDT 큐 뒤로 작업 지연 + `resetSplitPaneDividers()` + `w.validate()` on window ancestor
- `JSplitPane.resetToPreferredSizes()`: 분할창 divider가 0에 고정된 경우 비율 초기화
- `resetSplitPaneDividers(container)`: 컴포넌트 트리를 재귀 탐색하여 모든 JSplitPane에 `resetToPreferredSizes()` 적용
- `SwingUtilities.getWindowAncestor(panel)`로 상위 Window를 찾아 `window.validate()` + `window.repaint()` 호출

### setShowInvalidIncludeColumns(true) 부작용
- `DialogComponentColumnFilter.setShowInvalidIncludeColumns(true)` 설정 시, include 목록에 있는 컬럼이 현재 spec에 없을 경우 빨간 테두리로 표시됨
- OK 없이 닫고 재열기 시 이전 include 목록이 현 spec과 불일치하여 Target에 빨간 테두리 + Available 비가시 현상 발생
- **해결**: 생성자에서 `setShowInvalidIncludeColumns(true)` 호출 제거 (기본값 false 유지)

---
---

# Spark Expression Node (신규 개발)

## 개요

여러 Spark SQL 표현식을 적용하여 컬럼을 변환하거나 추가하는 Expression 노드.
KNIME의 Expression 노드를 Spark 환경에서 동작하도록 구현한 버전.
각 표현식 행은 SQL 표현식, 출력 모드(APPEND/REPLACE), 출력 컬럼명으로 구성.

- **노드명**: Spark Expression (Hyim)
- **최소 Spark 버전**: 3.4
- **노드 타입**: Manipulator

---

## 플러그인 구조

| 플러그인 | 역할 |
|----------|------|
| `org.knime.bigdata.spark.dx.node` | 노드 UI (Factory, Model, Dialog, Settings, JobInput/Output) |
| `org.knime.bigdata.spark3_4.dx` | Spark 3.4용 Job 구현 |
| `org.knime.bigdata.spark3_5.dx` | Spark 3.5용 Job 구현 |

---

## 클래스 구조

### Node 레이어 (`org.knime.bigdata.spark.dx.node.sql.expression`)

| 파일 | 역할 |
|------|------|
| `SparkExpressionNodeFactory.java` | 노드 팩토리. `DefaultSparkNodeFactory` 상속. 카테고리: "sql" |
| `SparkExpressionNodeFactory.xml` | 노드 설명 (1탭: Expression - split-pane 레이아웃) |
| `SparkExpressionNodeModel.java` | 노드 모델. configure에서 유효성 검증, execute에서 Job 실행 |
| `SparkExpressionNodeDialog.java` | `DataAwareNodeDialogPane` 상속. KNIME Expression 노드 스타일 split-pane 레이아웃, Evaluate 버튼 |
| `SparkExpressionSettings.java` | 설정 모델 (expressions[], outputModes[], columnNames[]) |
| `SparkExpressionJobInput.java` | Job 입력 VO. validateOnly 지원 |
| `SparkExpressionJobOutput.java` | Job 출력 VO. previewData 포함 |

### Spark Job 레이어 (spark3_4 / spark3_5 동일 구조)

| 파일 | 역할 |
|------|------|
| `ExpressionJob.java` | SparkJob 구현. `withColumn(name, expr(sql))` 순차 적용. validateOnly시 LIMIT 5 + showString |
| `ExpressionJobRunFactory.java` | Job 실행 팩토리 |
| `ExpressionJobRunFactoryProvider.java` | SPI 프로바이더 |

---

## 포트 구성

| 포트 | 방향 | 타입 | 설명 |
|------|------|------|------|
| 0 | 입력 | `SparkDataPortObject` | 변환 대상 Spark DataFrame |
| 0 | 출력 | `SparkDataPortObject` | 표현식 적용된 결과 DataFrame |

---

## 설정 항목

| Config Key | 타입 | 기본값 | 설명 |
|------------|------|--------|------|
| `expressions` | String[] | [""] | Spark SQL 표현식 목록 |
| `outputModes` | String[] | ["APPEND"] | 출력 모드 목록 (APPEND / REPLACE) |
| `columnNames` | String[] | ["new_column"] | 출력 컬럼명 목록 |
| `nodeConfigured` | Boolean | false | OK 클릭 여부 |

---

## 유효성 검증 (Dialog OK + NodeModel configure 양쪽)

| 검증 | 에러 메시지 |
|------|------------|
| 표현식 비어있음 | "Expression N is empty. Enter a Spark SQL expression." |
| 컬럼명 비어있음 | "Output column name for Expression N is empty." |
| 중복 출력 컬럼명 | "Duplicate output column name 'X' in Expression N." |
| REPLACE: 컬럼 미존재 | "Expression N: cannot replace column 'X' because it does not exist." |
| APPEND: 컬럼 이미 존재 | "Expression N: output column 'X' already exists in the input table." |
| 노드 미설정 | "Node has not been configured." |

---

## 실행 흐름

```
SparkExpressionNodeFactory
  └─ creates SparkExpressionNodeModel

configureInternal()
  ├─ 노드 설정 여부 확인 (CFG_CONFIGURED)
  ├─ 표현식 비어있음/컬럼명 비어있음 검증
  ├─ 중복 출력 컬럼명 검증 (APPEND + REPLACE 모두)
  ├─ REPLACE: 대상 컬럼 존재 여부 검증 (체인 추적)
  ├─ APPEND: 기존 컬럼명 충돌 검증
  └─ 출력 스펙 null (SQL 결과 타입은 실행 시 결정)

executeInternal()
  ├─ SparkExpressionJobInput 구성 (expressions, modes, names)
  ├─ SparkContextUtil.getJobRunFactory() 로 Job 실행
  └─ Spark Job 결과 스펙으로 SparkDataPortObject 반환

ExpressionJob.runJob() (Spark 측)
  ├─ validateOnly=true:
  │   ├─ 표현식 순차 적용 withColumn(name, expr(sql))
  │   ├─ 실패 시 "Expression N error: ..." 반환
  │   └─ 성공 시 showString(5) 프리뷰 반환
  ├─ 정상 실행:
  │   ├─ 표현식 순차 적용 withColumn(name, expr(sql))
  │   ├─ APPEND: 새 컬럼 추가 / REPLACE: 기존 컬럼 덮어쓰기
  │   └─ 결과 DataFrame + IntermediateSpec 반환
  └─ 표현식 체인: 이전 표현식 결과를 다음 표현식에서 참조 가능
```

---

## 다이얼로그 기능

### 탭 구성 (KNIME Expression 노드 스타일 split-pane)
- **단일 탭 - Expression**: 좌측(Input Columns + Function Catalog) | 중앙(Expression 탭) | 하단(Output Preview)

### 레이아웃
```
┌────────────┬──────────────────────────────────────────┐
│            │  [Expr 1] [Expr 2] [+] [-]   (tabs)     │
│  Input     │  ┌──────────────────────────────────────┐ │
│  Columns   │  │   Expression Editor (large area)      │ │
│            │  │   (monospace font)                    │ │
│  ────────  │  ├──────────────────────────────────────┤ │
│            │  │ Output: [APPEND ▼]  Column: [____]  │ │
│  Function  │  └──────────────────────────────────────┘ │
│  Catalog   │  ─── Output Preview ─────────────────── │
│            │  [Evaluate] Preview first 5 rows         │
│            │  ┌──────────────────────────────────────┐ │
│            │  │ preview output                        │ │
│            │  └──────────────────────────────────────┘ │
└────────────┴──────────────────────────────────────────┘
```

### 좌측 패널
- **Input Columns** (상단): 입력 테이블 컬럼 목록 (이름 + 타입). 더블클릭으로 에디터에 삽입
- **Function Catalog** (하단): Spark SQL 함수 카탈로그 (String, Math, Date/Time, Null, Type Cast, Conditional). 더블클릭으로 템플릿 삽입
- 컬럼명에 공백/특수문자 포함 시 자동 백틱 래핑

### 중앙 패널 (Expression Tabs)
- JTabbedPane: 각 탭이 하나의 표현식
- `+` 버튼: 새 표현식 탭 추가 / `-` 버튼: 현재 탭 삭제 (최소 1개 유지)
- 각 탭: 큰 JTextArea 에디터 (모노스페이스) + Output Mode 콤보 (APPEND/REPLACE) + Column Name 필드
- 표현식 순차 적용: 이전 표현식 결과를 다음 표현식에서 참조 가능

### Evaluate (Output Preview)
- Evaluate 버튼 → SwingWorker로 validate-only Spark Job 실행
- 성공 시 초록색 + 샘플 데이터 5행 표시
- 실패 시 빨간색 + 어떤 표현식이 실패했는지 표시 ("Expression N error: ...")
- upstream 노드 미실행 시 안내 메시지
- 유효성 검증 실패 시 해당 탭으로 자동 전환

### 표현식 예시
- `UPPER(name)` — 대문자 변환
- `col1 + col2` — 산술 연산
- `CAST(age AS STRING)` — 타입 캐스팅
- `COALESCE(col1, 'default')` — null 처리
- `REGEXP_REPLACE(text, 'pattern', 'replacement')` — 정규식
- `CONCAT(first, ' ', last)` — 문자열 결합
- `CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END` — 조건식
- `TO_DATE(date_str, 'yyyy-MM-dd')` — 날짜 파싱
