This repository was created using sample test data to demonstrate my skills in designing and implementing a data model aimed at understanding the key drivers of growth for Air Boltic.

PART 1

The conceptual data model and lineage overview can be found here:
https://github.com/metkapinezic/Air-Boltic/blob/main/model_lineage_columns_.pdf

All SQL models, tests, and documentation were developed using dbt Core, and can be found here:
https://github.com/metkapinezic/Air-Boltic/tree/main/AirBoltic_dbt/models

For compute and warehousing, I used Snowflake, as it is the platform I am most familiar with.

My approach focuses on delivering a data model that is reliable, scalable, maintainable, and easy to use. dbt supports these goals exceptionally well and is an essential tool for data analysts and engineers working with complex modelling and transformations.

Key Deliverables and Principles
- Clear layer separation (raw → intermediate → dim/fact → mart)
- Transparent lineage from source tables through to final marts
- Deduplication and data validation in intermediate layers
- SCD Type 2 implemented in dimensions to preserve historical accuracy
- Data freshness tracking using the dbt_updated_at column
- Automated dbt tests (not_null, unique, relationships, accepted_values) defined in YAML for every model
- Surrogate keys added to enable consistent and scalable joins
- Aggregate marts, such as agg_daily_trip_metrics, designed for performant dashboards and flexible weekly/monthly rollups
- Documentation-first approach using dbt (dbt docs generate); in practice, no model should exist without a YAML file
- Consistent naming conventions (dim_*, fct_*, src_*, agg_*) to improve maintainability and user experience; this should be standardized and ideally enforced with automated checks

If given more time, I would improve:
- Adding more tests in macros, observability, and data freshness alerts
- Explicitly defining the use of incremental models in dbt for large tables

  

PART 2

In an ideal scenario, the CI/CD process would be fully automated, continuously tested, highly observable, version-controlled, and supported by isolated compute environments.

With unlimited resources and full support from DevOps, I would introduce four environments:

1. dev:
   - Sandbox for local development
   - Uses sampled data and isolated computation

2. int (QA / Integration)
   - Executes full datasets
   - Runs data quality, unit tests, and regression tests
   - Automatically refreshed on merge to main

3. stg (Staging)
   - Mirrors production (same compute, same data, same permissions)
   - Supports analyst UAT using local dbt repos
   - Data refresh triggered by orchestration

4. prod
    - Highly controlled, restricted access
    - Tagged releases promoted from staging only after all tests pass
    - Deployments via GitHub Actions, GitOps, or similar tooling

Ideal CI/CD Workflow:
- All data teams use git and local dbt repositories
- Every change happens on a feature branch with a mandatory PR review
- All deployments tag a Jira ticket and versioned release
- Unit tests must run before deployment to stg and prod
- End-to-end DAG execution using an orchestration tool (Airflow, Dagster, Prefect)
- Strong data observability standards (freshness, anomalies, schema drift)
- Automated promotion to production only if all tests pass

Real-World Observations:
In practice, data teams often face constraints:
- Limited budget for computing (or lack of cost transparency)
- Limited staffing, especially senior engineers
- Few environments (often only dev and int)
- No observability tools
- Manual or minimal QA
- Partial or slow cloud adoption
- Legacy systems still present
- Teams working in silos
- Data mesh concepts not yet adopted or understood

Low Effort / Short-Term Recommendations:
- Adopt git workflows with feature branches and PR reviews
- Implement basic dbt tests (not_null, unique, relationships) for every model
- Enable automated CI on PRs using dbt compile
- Add unit tests for macros
- Introduce a dedicated staging or integration schema to reduce production risk
- Configure simple SLAs and alerting (failed runs, freshness issues)

Example from my experience:
At a previous company, modelling was done manually without dbt, and transformations occurred outside the data warehouse. Only dev and prod environments existed. Introducing a proper int environment required collaboration with DevOps and new deployment tooling (GitOps / GitHub Actions).
The result was significantly improved clarity, reduced risk, and empowered BI analysts to own their schema development—leading to faster insights.

High Effort / Long-Term Strategic Investments:
- Fully managed staging/QA environment with complete-volume test runs
- Implement data observability at the same level as software engineering
- Introduce a semantic layer to unify metric definitions and enable governed self-service
- Adopt a data mesh approach with clear domain ownership and federated governance
