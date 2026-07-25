# spotify_dab

A Databricks lakehouse project that ingests Spotify-style dimension data, refines it through a medallion architecture, and deploys pipelines and jobs with Declarative Automation Bundles.

This repository is designed to demonstrate practical data engineering skills on Databricks, including streaming ingestion, Delta Lake processing, Unity Catalog-managed data assets, and deployment automation for development and production environments.

## Project overview

This project follows a layered architecture:

* Bronze layer stores raw source files in cloud storage.
* Silver layer uses streaming ingestion and transformation logic to standardize, cleanse, and deduplicate data.
* Gold layer publishes curated datasets through a Lakeflow Spark Declarative Pipeline for downstream analytics use cases.

From the current implementation, the project processes dimension-style entities such as:

* users
* artists
* tracks

## What this project demonstrates

* Databricks-based data engineering on Azure
* Declarative Automation Bundles for repeatable deployments
* Unity Catalog-aware pipeline publishing
* Auto Loader-style streaming ingestion from cloud storage
* Delta table writing with checkpointing and schema evolution
* Modular Python transformation reuse
* Separation of development and production deployment targets

## Architecture summary

| Layer | Purpose | Current implementation |
| --- | --- | --- |
| Bronze | Raw landing zone | Source parquet files stored in cloud object storage |
| Silver | Cleaned and standardized tables | Notebook-driven streaming ingestion for dimensions such as `DimUser`, `DimArtist`, and `DimTrack` |
| Gold | Curated serving layer | Lakeflow Spark Declarative Pipeline reading from silver tables and publishing to Unity Catalog |

## Repository structure

* `src/silver/`
  * Notebook-based transformation flow for silver-layer dimension tables
  * Uses streaming reads, schema tracking, deduplication, and Delta writes
* `src/gold/dlt/transformations/`
  * Gold-layer pipeline definitions for curated tables
* `utils/transformations.py`
  * Shared reusable helper logic for dataframe cleanup
* `resources/spotify_dab_etl.pipeline.yml`
  * Bundle definition for the main pipeline resource
* `resources/sample_job.job.yml`
  * Bundle definition for a sample orchestrated job
* `databricks.yml`
  * Bundle configuration with separate `dev` and `prod` targets

## Technical highlights

### Silver ingestion

The silver workflow reads parquet data from cloud storage using streaming patterns, applies basic business transformations, removes rescued columns, and deduplicates records before writing Delta tables.

Examples visible in the current project include:

* user name standardization
* artist deduplication
* track duration flag derivation
* track name cleanup

### Gold publishing

The gold layer is configured as a serverless Lakeflow Spark Declarative Pipeline in Unity Catalog. It reads from silver tables and is intended to provide curated, analytics-ready datasets.

### Deployment approach

This repository uses Declarative Automation Bundles so the same project can be deployed consistently across environments. The bundle currently defines:

* a `dev` target for development deployments
* a `prod` target for production deployments
* a pipeline resource
* a sample job resource

## Why this project stands out

This project showcases end-to-end data engineering skills rather than isolated scripts. It demonstrates:

* pipeline thinking across bronze, silver, and gold layers
* cloud data lake integration on Azure
* real Databricks deployment configuration
* operational concepts such as checkpointing, schema evolution, and environment separation
* maintainable project structure with reusable utilities and infrastructure-as-code style configuration

## How to deploy

Authenticate to your workspace first:

```bash
databricks configure
```

Deploy to development:

```bash
databricks bundle deploy --target dev
```

Deploy to production:

```bash
databricks bundle deploy --target prod
```

Run a configured bundle resource:

```bash
databricks bundle run
```

