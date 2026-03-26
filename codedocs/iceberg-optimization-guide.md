# Iceberg Table Optimization Guide

## Query: Star Schema Revenue by Category

```sql
SELECT
    p.category,
    SUM(f.total_amount) AS total_revenue
FROM
    fact_sales f
-- JOINing: Connecting the large fact table to small dimensions
JOIN
    dim_product p ON f.product_key = p.product_key
JOIN
    dim_date d ON f.date_key = d.date_key
-- FILTERING: Applying criteria to the small dimension tables
WHERE
    p.category = 'Electronics'
    AND d.year = 2025
-- AGGREGATING: Summarizing the metrics from the fact table
GROUP BY
    p.category;
```

---

## Why Data Layout Matters

Query performance depends on data skipping (pruning), which is the ability to avoid reading irrelevant files or row groups based on metadata.
Pruning effectiveness depends on data layout.
Data layout levers:
- Partitioning provides strong physical grouping across files, enabling efficient partition pruning when filters match partition keys.
- Sorting improves data locality within partitions, tightening column value ranges and enhancing row-group-level pruning.
- Compaction consolidates small files and enforces consistent sort order, making pruning more effective (and reducing the small file cost that partitioning can sometimes introduce).
- Z-ordering extend sorting to multi-dimensional
- Column statistics in Iceberg manifest files and Parquet row groups drive pruning by recording min/max values per column. The statistics reflect the physical layout.
- Bloom filters add another layer of pruning, especially for unsorted columns and exact match predicates.

### Metadata Layers → Pruning Granularity

Each metadata layer corresponds to a different granularity of pruning:

```
metadata.json
    └── manifest list  (snap-*.avro)       ← PARTITION PRUNING
            └── manifest file (.avro)      ← FILE-LEVEL PRUNING (column stats)
                    └── data file (.parquet)
                            └── row group  ← ROW-GROUP PRUNING (Parquet stats + bloom filters)
```

**Manifest list** — stores per-manifest **partition value summaries**. The engine skips entire manifest files whose partition ranges don't match the query. This is where **partitioning** pays off: `year=2025` eliminates manifests covering other years before opening a single file.

**Manifest files** — stores per-data-file **column min/max statistics** (and null counts). The engine skips individual data files whose min/max range doesn't overlap the filter predicate. This is where **sorting and compaction** pay off: sorted data has tight, non-overlapping ranges per file, so the engine can eliminate more files. **Z-ordering** extends this to multiple columns simultaneously.

**Parquet row groups** (inside data files) — store **row-group-level min/max stats** and **bloom filters**. Even after a file passes manifest-level checks, the engine can skip row groups within it. **Bloom filters** live here and are especially effective for exact-match predicates on unsorted columns.

| Layer | Pruning type | Driven by |
|---|---|---|
| Manifest list | Partition pruning (manifest-level) | Partitioning |
| Manifest file | File-level pruning (data file skipping) | Sorting, compaction, column stats |
| Parquet row group | Row-group pruning | Sorting, Z-ordering, bloom filters |

> **Key insight:** Iceberg metadata prunes at the file level; Parquet metadata prunes within files. Sorting improves both because it tightens value ranges at every layer.

---

## Iceberg Optimizations for `fact_sales`

### 1. Partitioning

Partition by year (derived from `date_key`) since your filter is `d.year = 2025`. This eliminates full table scans.

```sql
ALTER TABLE fact_sales
ADD PARTITION FIELD years(date_key);
```

If `category` is high-cardinality enough, co-partition:

```sql
ADD PARTITION FIELD product_category;  -- denormalize if needed
```

### 2. Sort Order (Z-ordering / Local Sort)

Sort by the join keys to group related rows and improve data skipping:

```sql
ALTER TABLE fact_sales
WRITE ORDERED BY date_key, product_key;
```

Or use Z-order for multi-dimensional skipping (Spark):

```sql
CALL system.rewrite_data_files(
  table => 'fact_sales',
  strategy => 'sort',
  sort_order => 'zorder(product_key, date_key)'
);
```

### 3. Bloom Filters on Join Keys

Enable bloom filters so scan nodes can skip files that don't contain matching `product_key` or `date_key` values:

```sql
ALTER TABLE fact_sales
SET TBLPROPERTIES (
  'write.parquet.bloom-filter-enabled.column.product_key' = 'true',
  'write.parquet.bloom-filter-enabled.column.date_key' = 'true'
);
```

### 4. File Compaction

Merge small files to reduce scan overhead:

```sql
CALL system.rewrite_data_files('fact_sales');
```

### 5. Statistics / Column Stats

Ensure Iceberg column-level stats (min/max) are up to date so the engine can skip files where `product_key` or `date_key` ranges don't overlap the filter:

```sql
CALL system.rewrite_manifests('fact_sales');
```

### Summary: Priority Order

| Priority | Optimization | Impact |
|---|---|---|
| 1 | Partition by `year(date_key)` | Eliminates year-level full scans |
| 2 | Sort by `product_key, date_key` | Enables file-level skipping for joins |
| 3 | Bloom filter on join keys | Speeds up row-group filtering |
| 4 | Compaction | Reduces file count overhead |

The biggest win is **partitioning by year** since your `WHERE d.year = 2025` filter maps directly to a partition predicate, allowing Iceberg to prune entire partitions at planning time.

---

## Star Schema vs Snowflake Schema

### Star Schema

Fact table joins directly to **flat, denormalized** dimension tables. Dimensions have all attributes in one wide table.

```
              dim_date
                 |
fact_sales ── dim_product
```

Query pattern: `fact JOIN dim_product JOIN dim_date`

**Characteristics:**
- Few JOINs (2–3 tables)
- Wide, denormalized dimension rows
- Filters live directly on the joined dimension → easy predicate pushdown
- Simpler query plans; most engines optimize well

### Snowflake Schema

Dimensions are **normalized** into sub-dimensions (e.g., `dim_product → dim_category → dim_subcategory`). Each dimension is split into a hierarchy of tables.

```
              dim_year
                 |
              dim_month
                 |
              dim_date
                 |
fact_sales ── dim_product ── dim_category ── dim_subcategory
```

Query pattern: `fact JOIN dim_product JOIN dim_category JOIN dim_date JOIN dim_month JOIN dim_year`

**Characteristics:**
- Many JOINs (5+ tables)
- Narrower, normalized tables (less storage redundancy)
- Filters may live 2–3 hops away from the fact table
- Harder for engines to push predicates down through multi-hop joins

### Equivalent Snowflake Schema Query

```sql
SELECT
    c.category_name,
    SUM(f.total_amount) AS total_revenue
FROM
    fact_sales f
JOIN dim_product p     ON f.product_key = p.product_key
JOIN dim_category c    ON p.category_key = c.category_key   -- extra hop
JOIN dim_date d        ON f.date_key = d.date_key
JOIN dim_month m       ON d.month_key = m.month_key          -- extra hop
JOIN dim_year y        ON m.year_key = y.year_key            -- extra hop
WHERE
    c.category_name = 'Electronics'
    AND y.year = 2025
GROUP BY
    c.category_name;
```

---

## How Iceberg Optimization Changes for Snowflake Schema

| Concern | Star Schema | Snowflake Schema |
|---|---|---|
| Partition keys | Derive directly from fact join keys (e.g., `year(date_key)`) | Predicate may live in a sub-dimension — requires denormalized partition column or pre-join |
| Bloom filters | Few join keys on fact table | More join keys; also apply bloom filters to sub-dimension FK columns |
| Sort order | `product_key, date_key` | Add sub-dimension FKs (e.g., `category_key, product_key, date_key`) |
| Join count | Low (2–3 tables) | High (5+ tables); consider pre-joining sub-dimensions into a view or materialized table |
| Predicate pushdown | Filter on dimension → pushed to fact via join pruning | Filter lives in sub-dimension; engine must traverse more hops before pruning |
| Denormalization option | Not needed (dims already flat) | Consider flattening frequently-filtered sub-dimensions into fact or a wide dim |

### Snowflake-Specific Recommendations

**Option A: Denormalize filter columns into `fact_sales`**

Add `category` and `year` as derived columns directly on the fact table. This restores star-schema-level partition pruning:

```sql
ALTER TABLE fact_sales ADD COLUMN category STRING;
ALTER TABLE fact_sales ADD COLUMN sale_year INT;

ALTER TABLE fact_sales ADD PARTITION FIELD sale_year;

ALTER TABLE fact_sales SET TBLPROPERTIES (
  'write.parquet.bloom-filter-enabled.column.category' = 'true'
);
```

**Option B: Pre-join sub-dimensions into a wide view**

Create a wide `dim_product_full` that flattens the snowflake chain, then treat it like a star schema:

```sql
CREATE VIEW dim_product_full AS
SELECT p.*, c.category_name, c.category_key
FROM dim_product p
JOIN dim_category c ON p.category_key = c.category_key;
```

**Option C: Bloom filters on all FK hop columns**

If you can't denormalize, add bloom filters on every join key in the chain:

```sql
ALTER TABLE fact_sales SET TBLPROPERTIES (
  'write.parquet.bloom-filter-enabled.column.product_key' = 'true',
  'write.parquet.bloom-filter-enabled.column.date_key' = 'true'
);

ALTER TABLE dim_product SET TBLPROPERTIES (
  'write.parquet.bloom-filter-enabled.column.category_key' = 'true'
);
```

### Key Takeaway

> In a snowflake schema, **the further a filter predicate is from the fact table, the harder it is for Iceberg (and the query engine) to prune files early**. The recommended mitigation is to selectively denormalize high-frequency filter columns back into the fact table as partition or sort keys.
