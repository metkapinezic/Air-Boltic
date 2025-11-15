Step 1 Upload the data in DWH
  - Set up dbt
  - Set up Snowflake
  - Transform json to csv (multiple otptions, decided to use the local pyuthon script and save it a s csv)
  - Upload data to ab_raw (create database, create schemas, create stg(temporary storage), upload csv to table)
      - When loading the data to tables, what was considered:
          - Running sql via snowflake ui (you can define contrains)
          - Using snowflake upload ui (no constrains, faster/simpler for one time upload
