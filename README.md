# Spotify - Azure End-to-End Data Engineering Project

A comprehensive end-to-end data engineering solution demonstrating data ingestion, transformation, and orchestration of Spotify analytics data on Microsoft Azure using Azure Data Factory, Databricks, [...]

## 📋 Project Overview

This project showcases a production-grade data pipeline that:
- **Ingests** Spotify data using Azure Data Factory
- **Stores** raw data in Azure Data Lake Storage (ADLS)
- **Transforms** data through a medallion architecture (Bronze → Silver → Gold) using Databricks
- **Orchestrates** automated ETL workflows with Azure Data Factory and Databricks Jobs
- **Serves** curated datasets through Delta Lake and Unity Catalog

## 🏗️ Architecture

### High-Level Data Flow

```
Spotify API
    ↓
Azure Data Factory (Copy Activity)
    ↓
Azure Data Lake Storage (ADLS) - Bronze Layer
    ↓
Databricks ETL Pipeline
    ├─ Bronze Layer (Raw Data)
    ├─ Silver Layer (Cleaned & Standardized)
    └─ Gold Layer (Curated for Analytics)
    ↓
Power BI / Analytics Tools
```

### Medallion Architecture

| Layer | Purpose | Technology | Format |
|-------|---------|-----------|--------|
| **Bronze** | Raw landing zone | ADLS Gen2 | Parquet/JSON |
| **Silver** | Cleaned, deduplicated, standardized | Delta Tables + Streaming | Delta Lake |
| **Gold** | Curated, aggregated, business-ready | DLT Pipelines | Delta Lake + UC |

---

## 🚀 Prerequisites

### Azure Resources Required
- Azure Subscription
- Azure Resource Group
- Azure Storage Account (ADLS Gen2)
- Azure Data Factory
- Azure Databricks Workspace
- Service Principal with appropriate permissions

### Local Setup
- Python 3.9+
- Azure CLI
- Databricks CLI
- Git

---

## 📦 Azure Setup Instructions

### 1. Create Azure Resource Group

```bash
az group create \
  --name spotify-rg \
  --location eastus
```

### 2. Create Azure Storage Account (ADLS Gen2)

```bash
az storage account create \
  --name spotifydata<unique-id> \
  --resource-group spotify-rg \
  --location eastus \
  --sku Standard_LRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true
```

### 3. Create Storage Containers

```bash
# Bronze layer (raw data)
az storage fs create \
  --name bronze \
  --account-name spotifydata<unique-id>

# Silver layer (cleaned data)
az storage fs create \
  --name silver \
  --account-name spotifydata<unique-id>

# Gold layer (curated data)
az storage fs create \
  --name gold \
  --account-name spotifydata<unique-id>
```

### 4. Create Azure Databricks Workspace

```bash
az databricks workspace create \
  --resource-group spotify-rg \
  --name spotify-dbws \
  --location eastus \
  --sku standard
```

### 5. Create Azure Data Factory

```bash
az datafactory create \
  --resource-group spotify-rg \
  --name spotify-adf \
  --location eastus
```

### 6. Create Service Principal for Authentication

```bash
az ad sp create-for-rbac \
  --name spotify-sp \
  --role Contributor \
  --scopes /subscriptions/{subscription-id}/resourceGroups/spotify-rg
```

Save the output credentials for later use.

---

## 🔗 Azure Data Factory Configuration

### 1. Create Linked Services

#### A. Azure Data Lake Storage Linked Service

```json
{
  "name": "AzureDataLakeStorage",
  "type": "AzureDataLakeStorageGen2",
  "typeProperties": {
    "url": "https://spotifydata<unique-id>.dfs.core.windows.net",
    "servicePrincipalId": "{service-principal-id}",
    "servicePrincipalKey": "{service-principal-secret}",
    "tenant": "{tenant-id}"
  }
}
```

#### B. Databricks Linked Service

```json
{
  "name": "AzureDatabricks",
  "type": "AzureDatabricks",
  "typeProperties": {
    "domain": "https://{workspace-region}.azuredatabricks.net",
    "accessToken": "{databricks-token}",
    "newClusterJobClusterConfig": {
      "sparkVersion": "13.3.x-scala2.12",
      "nodeTypes": ["Standard_D4s_v3"],
      "minNumberOfWorkers": 2,
      "maxNumberOfWorkers": 4,
      "sparkConf": {}
    }
  }
}
```

### 2. Create Datasets

#### A. Spotify API Source Dataset (HTTP)

```json
{
  "name": "SpotifyAPIDataset",
  "type": "RestResource",
  "linkedServiceName": "SpotifyHttpLinkedService",
  "typeProperties": {
    "relativeUrl": "/v1/me/top/tracks"
  }
}
```

#### B. Bronze Layer Sink Dataset

```json
{
  "name": "BronzeSinkDataset",
  "type": "DelimitedText",
  "linkedServiceName": "AzureDataLakeStorage",
  "typeProperties": {
    "location": {
      "type": "AzureBlobFSLocation",
      "fileSystem": "bronze",
      "folderPath": "spotify/tracks/@{pipeline().RunId}"
    },
    "columnDelimiter": ",",
    "escapeChar": "\\",
    "quoteChar": "\""
  }
}
```

### 3. Create Pipeline

#### Copy Activity Pipeline

```json
{
  "name": "SpotifyDataIngestionPipeline",
  "activities": [
    {
      "name": "CopyFromSpotifyAPI",
      "type": "Copy",
      "inputs": [
        {
          "referenceName": "SpotifyAPIDataset",
          "type": "DatasetReference"
        }
      ],
      "outputs": [
        {
          "referenceName": "BronzeSinkDataset",
          "type": "DatasetReference"
        }
      ]
    },
    {
      "name": "TriggerDatabricksPipeline",
      "type": "DatabricksNotebook",
      "dependsOn": [
        {
          "activity": "CopyFromSpotifyAPI",
          "dependencyConditions": ["Succeeded"]
        }
      ],
      "linkedServiceName": "AzureDatabricks",
      "typeProperties": {
        "notebookPath": "/Shared/spotify_etl/02_silver_transformation"
      }
    }
  ]
}
```

### 4. Create Trigger (Schedule)

```json
{
  "name": "DailySpotifyIngestionTrigger",
  "type": "ScheduleTrigger",
  "typeProperties": {
    "recurrence": {
      "frequency": "Day",
      "interval": 1,
      "startTime": "2024-01-01T02:00:00Z"
    }
  }
}
```

---

## 🔄 Databricks Setup & ETL Pipeline

### 1. Create Databricks Cluster

```python
cluster_config = {
    "cluster_name": "spotify-etl-cluster",
    "spark_version": "13.3.x-scala2.12",
    "node_type_id": "Standard_D4s_v3",
    "num_workers": 2,
    "spark_conf": {
        "spark.databricks.delta.schema.autoMerge.enabled": "true"
    }
}
```

### 2. Configure Storage Access with Databricks Connector

#### A. Assign Storage Contributor Role to Databricks Workspace

```bash
# Get the Databricks Workspace Service Principal Object ID
WORKSPACE_SP_ID=$(az databricks workspace show \
  --resource-group spotify-rg \
  --name spotify-dbws \
  --query identity.principalId -o tsv)

# Assign Storage Blob Data Contributor role
az role assignment create \
  --assignee-object-id $WORKSPACE_SP_ID \
  --role "Storage Blob Data Contributor" \
  --scope /subscriptions/{subscription-id}/resourceGroups/spotify-rg/providers/Microsoft.Storage/storageAccounts/spotifydata<unique-id>
```

#### B. Configure Databricks Connector in Notebooks

Use the Databricks connector to access ADLS Gen2 directly without mounting:

```python
# Configure Databricks connector for ADLS Gen2
storage_account = "spotifydata<unique-id>"
container = "bronze"

# Set Spark configuration for Databricks connector
spark.conf.set(
    f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebricksfsdaemon.AzureBricksDataDaemon"
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
)
```

### 3. Bronze Layer - Raw Data Ingestion

**Notebook: `01_bronze_ingestion`**

```python
# Configure storage account details
storage_account = "spotifydata<unique-id>"
container = "bronze"
path = "spotify/tracks"

# Build the ADLS Gen2 path
adls_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{path}/"

# Read raw Spotify data from ADLS using Databricks connector
df_raw = spark.read \
    .format("parquet") \
    .load(adls_path)

# Store in Bronze Delta Table
df_raw.write \
    .mode("append") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("bronze.spotify_tracks")

print(f"Loaded {df_raw.count()} records into Bronze layer")
```

### 4. Silver Layer - Data Transformation & Cleaning

**Notebook: `02_silver_transformation`**

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Read from Bronze
df_bronze = spark.table("bronze.spotify_tracks")

# Transformations
df_silver = df_bronze \
    .filter(col("track_id").isNotNull()) \
    .withColumn("track_name", trim(col("track_name"))) \
    .withColumn("artist_name", trim(col("artist_name"))) \
    .withColumn("popularity_score", col("popularity").cast(IntegerType())) \
    .withColumn("duration_minutes", col("duration_ms") / 60000) \
    .withColumn("is_explicit", col("explicit").cast(BooleanType())) \
    .withColumn("ingestion_date", current_timestamp()) \
    .dropDuplicates(["track_id", "artist_id"])

# Write to Silver
df_silver.write \
    .mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .saveAsTable("silver.dim_tracks")

print(f"Cleaned {df_silver.count()} unique tracks in Silver layer")
```

### 5. Gold Layer - Curated Analytics Tables

**Notebook: `03_gold_aggregation`**

```python
# Read from Silver
df_silver = spark.table("silver.dim_tracks")

# Top 50 Most Popular Tracks
top_tracks = df_silver \
    .filter(col("popularity_score") > 0) \
    .select("track_id", "track_name", "artist_name", "popularity_score", "duration_minutes") \
    .orderBy(desc("popularity_score")) \
    .limit(50)

top_tracks.write \
    .mode("overwrite") \
    .format("delta") \
    .option("path", "abfss://gold@spotifydata<unique-id>.dfs.core.windows.net/spotify/top_tracks") \
    .saveAsTable("gold.top_50_tracks")

# Artist Aggregations
artist_stats = df_silver \
    .groupBy("artist_name") \
    .agg(
        count("track_id").alias("track_count"),
        avg("popularity_score").alias("avg_popularity"),
        max("popularity_score").alias("max_popularity")
    ) \
    .orderBy(desc("track_count"))

artist_stats.write \
    .mode("overwrite") \
    .format("delta") \
    .option("path", "abfss://gold@spotifydata<unique-id>.dfs.core.windows.net/spotify/artist_stats") \
    .saveAsTable("gold.artist_statistics")

print("Gold layer tables created successfully")
```

### 6. Automated ETL Job Configuration

**File: `resources/spotify_etl.job.yml`**

```yaml
name: spotify-etl-job
tasks:
  - task_key: ingest_bronze
    notebook_task:
      notebook_path: /Repos/spotify_etl/notebooks/01_bronze_ingestion
    cluster_id: ${cluster_id}
    
  - task_key: transform_silver
    depends_on:
      - task_key: ingest_bronze
    notebook_task:
      notebook_path: /Repos/spotify_etl/notebooks/02_silver_transformation
    cluster_id: ${cluster_id}
    
  - task_key: aggregate_gold
    depends_on:
      - task_key: transform_silver
    notebook_task:
      notebook_path: /Repos/spotify_etl/notebooks/03_gold_aggregation
    cluster_id: ${cluster_id}

schedule:
  quartz_cron_expression: "0 2 * * ?" # Daily at 2 AM UTC
  timezone_id: "UTC"
  pause_status: UNPAUSED
```

---

## 📂 Repository Structure

```
.
├── README.md                              # This file
├── azure/
│   ├── adf_pipelines/                    # Azure Data Factory definitions
│   │   ├── SpotifyIngestionPipeline.json
│   │   ├── linked_services.json
│   │   └── datasets.json
│   └── infrastructure/
│       └── deploy.sh                     # Azure resource deployment script
├── databricks/
│   ├── notebooks/
│   │   ├── 01_bronze_ingestion.py        # Raw data ingestion
│   │   ├── 02_silver_transformation.py   # Data cleaning & transformation
│   │   └── 03_gold_aggregation.py        # Curated analytics tables
│   ├── resources/
│   │   ├── spotify_etl.job.yml          # Job configuration
│   │   └── cluster_config.yml           # Cluster configuration
│   └── utils/
│       ├── transformations.py            # Shared transformation utilities
│       └── validators.py                 # Data validation functions
├── spotify_dab/                           # Databricks Asset Bundles
│   ├── src/
│   │   ├── silver/                      # Silver layer notebooks
│   │   └── gold/
│   │       └── dlt/                     # Delta Live Tables definitions
│   ├── resources/
│   │   ├── spotify_dab_etl.pipeline.yml
│   │   └── sample_job.job.yml
│   ├── utils/
│   │   └── transformations.py
│   └── databricks.yml
├── config/
│   ├── dev.yaml                         # Development environment config
│   ├── prod.yaml                        # Production environment config
│   └── secrets.example.yaml             # Secrets template (do not commit actual secrets)
└── tests/
    ├── unit/
    │   └── test_transformations.py
    └── integration/
        └── test_etl_pipeline.py
```

---

## 🔧 Configuration & Deployment

### 1. Environment Configuration

**File: `config/dev.yaml`**

```yaml
azure:
  storage_account: spotifydata<unique-id>
  container_bronze: bronze
  container_silver: silver
  container_gold: gold

databricks:
  workspace_url: https://<region>.azuredatabricks.net
  cluster_id: <cluster-id>
  job_id: <job-id>

spark:
  num_partitions: 8
  shuffle_partitions: 200
```

### 2. Deploy Infrastructure (Azure)

```bash
cd azure/infrastructure
chmod +x deploy.sh
./deploy.sh --resource-group spotify-rg --location eastus
```

### 3. Deploy Databricks Jobs

```bash
databricks bundle validate -p spotify_dab
databricks bundle deploy -p spotify_dab --target dev
databricks bundle deploy -p spotify_dab --target prod
```

### 4. Trigger Pipeline in ADF

```bash
az datafactory pipeline create-run \
  --resource-group spotify-rg \
  --factory-name spotify-adf \
  --name SpotifyDataIngestionPipeline
```

---

## 📊 Data Quality & Monitoring

### Data Validation

**File: `databricks/utils/validators.py`**

```python
def validate_bronze_data(df):
    """Validate raw data meets minimum quality standards"""
    assert df.count() > 0, "Bronze table is empty"
    assert not df.filter(col("track_id").isNull()).count() > 0, "Null track_ids found"
    return True

def validate_silver_data(df):
    """Validate transformed data"""
    duplicates = df.groupBy("track_id").count().filter(col("count") > 1).count()
    assert duplicates == 0, f"Found {duplicates} duplicate records"
    return True
```

### Monitoring & Alerts

Monitor pipeline runs in:
- **Azure Data Factory**: Monitor tab in ADF UI
- **Databricks**: Jobs UI → Run history
- **Application Insights**: Configure for detailed logging

---

## 🧪 Testing

### Unit Tests

```bash
cd tests
python -m pytest unit/test_transformations.py -v
```

### Integration Tests

```bash
python -m pytest integration/test_etl_pipeline.py -v --spark-local
```

---

## 📈 Performance Optimization

### Partitioning Strategy

```python
# Partition Silver tables by ingestion date
df_silver.write \
    .partitionBy("ingestion_date") \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("silver.dim_tracks")
```

### Caching & Optimization

```python
# Cache frequently accessed tables
spark.sql("CACHE TABLE silver.dim_tracks")

# Use appropriate data formats
# Use Delta format for ACID compliance and time travel
# Use Parquet for external systems
```

---

## 🔐 Security Best Practices

1. **Storage Access Control**
   - Use Storage Blob Data Contributor role assignment for Databricks workspace
   - Leverages Azure Managed Identity for secure, credential-free access
   - No need to manage storage account keys in secrets

2. **Service Principal**: Use Azure AD service principals for authentication in ADF

3. **Network Security**: 
   - Deploy in VNet with firewall rules
   - Use private endpoints for storage

4. **Data Encryption**: Enable encryption at rest in ADLS and Databricks

5. **Access Control**
   - Use Databricks workspace access controls
   - Implement Unity Catalog for fine-grained permissions

---

## 📝 Useful Commands

```bash
# Authenticate to Azure
az login

# Authenticate to Databricks
databricks configure

# Deploy bundle
databricks bundle deploy -p spotify_dab --target prod

# View job runs
databricks jobs get-run --run-id <run-id>

# Cancel job run
databricks jobs cancel-run --run-id <run-id>

# Check ADLS contents
az storage fs file list --file-system bronze --account-name spotifydata<id>

# Assign Storage Blob Data Contributor role
az role assignment create \
  --assignee-object-id <workspace-sp-id> \
  --role "Storage Blob Data Contributor" \
  --scope /subscriptions/{subscription-id}/resourceGroups/spotify-rg/providers/Microsoft.Storage/storageAccounts/spotifydata<unique-id>
```

---

## 📚 Key Technologies

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Data Ingestion | Azure Data Factory | Orchestrate data pipelines |
| Storage | Azure Data Lake Storage Gen2 (ADLS) | Scalable cloud storage |
| Processing | Databricks | Distributed data processing |
| Format | Delta Lake | ACID transactions & time travel |
| Orchestration | Azure Data Factory + Databricks Jobs | Automation & scheduling |
| Monitoring | Application Insights | Logging & alerting |
| Storage Access | Databricks Connector + Managed Identity | Secure credential-free access |

---

## 🚀 Next Steps

1. **Enable Unity Catalog** for enhanced governance
2. **Implement Delta Live Tables (DLT)** for simplified pipeline definitions
3. **Add Data Quality Expectations** using DLT quality monitoring
4. **Setup Cost Optimization** with reserved instances and auto-scaling
5. **Implement Data Lineage** for compliance and troubleshooting

---

## 🤝 Contributing

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Commit changes: `git commit -m 'Add feature'`
3. Push to branch: `git push origin feature/your-feature`
4. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License - see LICENSE file for details.

---

## 📞 Support & Documentation

- [Azure Data Factory Documentation](https://docs.microsoft.com/en-us/azure/data-factory/)
- [Databricks Documentation](https://docs.databricks.com/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Azure CLI Reference](https://docs.microsoft.com/en-us/cli/azure/)

---

**Last Updated**: 2024-01-15
**Maintained by**: ndarji0110
