<!-- Generated from DataFusion #[user_doc] metadata. Run cargo run -p vql-kernel --example generate_sql_reference --locked. -->

# VQL built-in function reference

This reference covers the built-in functions registered by the current source checkout. See the [SQL reference](sql-reference.md) for types, Tables, Models, Functions, streaming queries, and Jobs, and [sample setup](installation.md#sample-data-and-models) for data and artifacts. Catalog Models and user-defined Functions are described by their declarations.

- [Built-in AI functions](#built-in-ai-functions)
  - [VQL_CLASSIFY](#vql_classify)
  - [VQL_DETECT](#vql_detect)
  - [VQL_EXTRACT](#vql_extract)
- [Spatial functions](#spatial-functions)
  - [BOX_CENTER](#box_center)
  - [POLYGON](#polygon)
  - [ST_CONTAINS](#st_contains)
  - [ST_POLYGON](#st_polygon)
- [Streaming functions](#streaming-functions)
  - [TUMBLE](#tumble)

## Built-in AI functions

### VQL_CLASSIFY

Judge the whole input against a category list and return `ARRAY<STRUCT<label STRING, score FLOAT>>`. IMAGE execution uses the installed YOLO26n ImageNet classifier; the STRING overload is typed but returns `FEATURE_NOT_AVAILABLE`. No Catalog Model is required.

All arguments except `input` must be planning-time constants. Required arguments are positional; optional arguments use `name => value`. SQL NULL input returns NULL without resolving a backend. Row failures return NULL by default or fail with `SET vql.on_error = 'fail'`. Scores are in `[0,1]` and comparable only within one call.

```text
VQL_CLASSIFY(input IMAGE | STRING, categories ARRAY<STRING> [, output_mode => 'single' | 'multi', min_score => FLOAT]) -> ARRAY<STRUCT<label STRING, score FLOAT>>
```

Arguments:

- `input`: An IMAGE value. STRING execution is unavailable.
- `categories`: A non-empty list of distinct category names.
- `output_mode`: Optional; 'single' (default) returns the highest-scoring requested category. 'multi' returns categories at or above min_score.
- `min_score`: Optional score threshold in [0,1]; defaults to 0.25.

After [sample setup](installation.md#sample-data-and-models) and creating [sample_images](sql-reference.md#create-table):

```sql
SELECT uri,
       VQL_CLASSIFY(image, ['football_helmet']) AS categories
FROM sample_images;
```

Related functions: [VQL_DETECT](#vql_detect).

### VQL_DETECT

Discover instances using the installed YOLO26n detector and return `ARRAY<STRUCT<label STRING, score FLOAT, locator LOCATOR>>`. Results are sorted by descending score; no detections is an empty array. `locator.box` uses pixel coordinates; a Catalog OBJECT_DETECTION Model's `box` instead uses normalized coordinates. No Catalog Model is required.

All arguments except `input` must be planning-time constants. Optional arguments use `name => value`. SQL NULL input returns NULL without resolving a backend. Row failures return NULL by default or fail with `SET vql.on_error = 'fail'`. Scores are in `[0,1]` and comparable only within one call.

```text
VQL_DETECT(input IMAGE [, classes => ARRAY<STRING>, min_score => FLOAT]) -> ARRAY<STRUCT<label STRING, score FLOAT, locator LOCATOR>>
```

Arguments:

- `input`: An IMAGE value.
- `classes`: Optional class filter; omitted selects all classes. Unknown class names produce no matches.
- `min_score`: Optional score threshold in [0,1]; defaults to 0.25.

After [sample setup](installation.md#sample-data-and-models) and creating [sample_images](sql-reference.md#create-table):

```sql
SELECT uri,
       VQL_DETECT(image, classes => ['person'], min_score => 0.5) AS detections
FROM sample_images;
```

Related functions: [VQL_CLASSIFY](#vql_classify), [VQL_EXTRACT](#vql_extract).

### VQL_EXTRACT

Declare named fields for extraction and return `STRUCT<requested fields>`. Each scalar answer has `value STRING`, `score FLOAT`, and `locator LOCATOR`; `list = true` returns an array of answer structs. Answer components may be NULL. Both IMAGE and STRING execution currently return `FEATURE_NOT_AVAILABLE` and have no scheduled execution release. No Catalog Model is required.

`fields` must be a planning-time constant and is positional. SQL NULL input returns NULL without resolving a backend. Row failures return NULL by default or fail with `SET vql.on_error = 'fail'`. Scores are in `[0,1]` and comparable only within one call. For instance detection use `VQL_DETECT`; the retired detection-shaped `VQL_EXTRACT` call returns a rewrite hint.

```text
VQL_EXTRACT(input IMAGE | STRING, fields MAP<STRING, STRUCT<question STRING, list BOOLEAN>>) -> STRUCT<requested fields>
```

Arguments:

- `input`: An IMAGE or STRING value; execution for both overloads is unavailable.
- `fields`: A non-empty map of distinct field names to question/list descriptors. A string descriptor is shorthand for a scalar question.

With [sample_images](sql-reference.md#create-table), this demonstrates request syntax only; execution returns FEATURE_NOT_AVAILABLE:

```sql
SELECT VQL_EXTRACT(image, MAP {
  'title': 'What is the title?',
  'items': STRUCT('List the items' AS question, TRUE AS list)
}) AS extracted
FROM sample_images;
```

Related functions: [VQL_DETECT](#vql_detect).

## Spatial functions

### BOX_CENTER

Return the center of a normalized BOX2D as POINT2D. NULL input returns NULL.

```text
BOX_CENTER(box BOX2D) -> POINT2D
```

Alternative syntax:

```text
box.center
```

Arguments:

- `box`: A normalized BOX2D value.

With `sample_images` and the resolved `yolo` Model from the [SQL reference](sql-reference.md#create-model):

```sql
SELECT f.uri, BOX_CENTER(det.box) AS center
FROM sample_images AS f,
     UNNEST(yolo(f.image, classes => ['person'])) AS u(det);
```

Related functions: [ST_CONTAINS](#st_contains).

### POLYGON

Parse a closed polygon from normalized x y pairs and return POLYGON. Coordinates must be finite and within [0,1]. A polygon needs at least three edges and repeats its first point at the end. NULL input returns NULL.

```text
POLYGON(text STRING) -> POLYGON
```

Alternative syntax:

```text
ST_POLYGON(text STRING)
```

Arguments:

- `text`: Comma-separated x y coordinates, optionally wrapped in POLYGON((...)).

```sql
SELECT POLYGON('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))') AS area;
```

Related functions: [ST_CONTAINS](#st_contains).

### ST_CONTAINS

Return BOOLEAN indicating whether a POLYGON contains a POINT2D. Boundary points are included. NULL arguments return NULL.

```text
ST_CONTAINS(polygon POLYGON, point POINT2D) -> BOOLEAN
```

Arguments:

- `polygon`: A closed polygon in normalized coordinates.
- `point`: A point in the same normalized coordinate space.

With `sample_images` and the resolved `yolo` Model from the [SQL reference](sql-reference.md#create-model):

```sql
SELECT f.uri, det.label
FROM sample_images AS f,
     UNNEST(yolo(f.image, classes => ['person'])) AS u(det)
WHERE ST_CONTAINS(
  POLYGON('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'),
  BOX_CENTER(det.box)
);
```

Related functions: [POLYGON](#polygon), [BOX_CENTER](#box_center).

### ST_POLYGON

Parse a closed polygon from normalized x y pairs and return POLYGON. Coordinates must be finite and within [0,1]. A polygon needs at least three edges and repeats its first point at the end. NULL input returns NULL.

```text
POLYGON(text STRING) -> POLYGON
```

Alternative syntax:

```text
ST_POLYGON(text STRING)
```

Arguments:

- `text`: Comma-separated x y coordinates, optionally wrapped in POLYGON((...)).

```sql
SELECT POLYGON('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))') AS area;
```

Related functions: [ST_CONTAINS](#st_contains).

## Streaming functions

### TUMBLE

Return the start of the event-time bucket containing ts as TIMESTAMP. Timestamps use millisecond precision, with their timezone preserved. NULL arguments return NULL. See [streaming queries](sql-reference.md#tumble-and-streaming-queries) for watermark closure and continuous aggregate restrictions.

```text
TUMBLE(ts TIMESTAMP, width INTERVAL) -> TIMESTAMP
```

Arguments:

- `ts`: The event-time timestamp.
- `width`: A positive bucket width containing no calendar months.

```sql
SELECT TUMBLE(
  CAST('2026-08-23 10:00:07.800' AS TIMESTAMP),
  INTERVAL '5' SECOND
) AS window_start;
```
