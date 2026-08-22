# Day 1 Databricks Workflow

## Beginner UI setup

1. Open Jobs and Pipelines.
2. Create a job named abb-retail-day1.
3. Add a notebook task named bronze_ingestion.
4. Select notebooks/day1/02_bronze_ingestion.
5. Add the notebook parameter catalog with your workspace catalog name.
6. Add a second notebook task named validate_bronze.
7. Select notebooks/day1/03_bronze_validation.
8. Set validate_bronze to depend on bronze_ingestion.
9. Pass the same catalog parameter.
10. Run the job and inspect both task outputs.

Expected order:

    bronze_ingestion: SUCCESS
            |
    validate_bronze: SUCCESS

## Production note

Interactive execution proves notebook logic. A workflow proves the tasks run in the required order with explicit parameters. Production should use an approved job identity and compute policy.

## Optional Asset Bundle

The repository includes databricks.yml and resources/day1_job.yml. Change the catalog variable and workspace profile before deploying:

    databricks bundle validate -t dev
    databricks bundle deploy -t dev

The source simulator and bootstrap remain intentional one-time lab steps and are not part of the recurring ingestion job.

