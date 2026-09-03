# Data Quality Check: Step-by-Step User Instructions

## 1. Infer an initial schema from a representative file
1. Open `manual_schema_processing.ipynb`.
2. In Cell 1, set `input_path` to a representative source file (`.csv` or `.parquet`).
3. Run Cell 1 to generate `data/schema.json`.

## 2. Manually adjust schema data types
1. Open `data/schema.json`.
2. Manually correct data types before running DQ.
3. Ensure all categorical/business-key columns are non-numeric (typically `string`).
4. This includes ID-like fields and code/reference fields, even if inferred as `integer`/`long`.

## 3. Confirm runtime config points to the schema
1. Open `dq_setup.json`.
2. Confirm `run.schema_file` points to `data/schema.json`.

## 4. Run the DQ check
Run:

```bash
docker compose --profile dq run --rm dq-runner
```

## 5. Validate execution in logs
1. Open the latest file under `logs/`.
2. Confirm the run logs include:
   - schemas match
   - inferred/provided PK columns used
   - distinct mismatch columns
   - total mismatch counts for `df1.exceptAll(df2)` and `df2.exceptAll(df1)`

## 6. Inspect mismatch output
1. Open `data/mismatches.json`.
2. Validate each record includes:
   - the key column value(s)
   - a `mismatches` array of `{column_name, df1_value, df2_value}` entries
   - `__missing_in_df1__`/`__missing_in_df2__` as `column_name` when a key exists on only one side

## 7. If PK is not found
1. Revisit `data/schema.json` and convert key/categorical columns to non-numeric types.
2. Re-run the DQ command.
3. Note: row-number fallback is disabled; valid key-based alignment is required.
