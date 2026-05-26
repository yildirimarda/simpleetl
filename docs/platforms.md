# Platform Deployment Guides

SimpleETL supports running ETL jobs on multiple platforms. This guide covers deployment for each supported platform.

## Local Development

### Running Jobs Locally

The default platform is `local`. Jobs run using pandas on your machine.

**Via Python:**

```python
from simpleetl.core.job import ETLJob

class MyJob(ETLJob):
    def run(self):
        # Your ETL logic
        pass

job = MyJob("config.yaml")
job.run_with_error_handling()
```

**Via CLI:**

```bash
python -m simpleetl --config config.yaml
```

**Via Platform Runner:**

```python
from simpleetl.platforms.local import LocalPlatformRunner

runner = LocalPlatformRunner()
runner.run_job(job)
```

### Local Configuration

```yaml
# configs/dev.yaml
environment: development
debug: true

logging:
  level: DEBUG
  format: text
  file: logs/etl_dev.log

database:
  default:
    driver: sqlite
    database: etl_dev.db

metrics:
  enabled: true
  port: 8000
```

---

## AWS Glue

### Overview

SimpleETL can run on AWS Glue. The `GluePlatformRunner` detects the Glue environment via the `AWS_EXECUTION_ENV` environment variable and delegates to local execution when running on Glue.

### Deployment Steps

1. **Package the application:**

   ```bash
   # Create a zip with dependencies
   uv pip install -t package/ .
   cd package && zip -r ../simpleetl.zip . && cd ..
   zip -g simpleetl.zip src/simpleetl -r
   zip -g simpleetl.zip configs/ -r
   ```

2. **Upload to S3:**

   ```bash
   aws s3 cp simpleetl.zip s3://my-bucket/simpleetl/simpleetl.zip
   ```

3. **Create a Glue Job:**

   - Go to the AWS Glue Console
   - Create a new Python Shell job
   -- Set the script location to `s3://my-bucket/simpleetl/simpleetl.zip`
   -- Set the job parameter `--config` to your config path

4. **Configure the job:**

   ```yaml
   name: glue_etl_job
   platform: glue
   input_format: csv
   output_format: parquet
   params:
     job_class: my_module.MyETLJob
     input_path: s3://my-bucket/input/data.csv
     output_path: s3://my-bucket/output/data.parquet
   ```

### Environment Detection

The Glue runner checks for the `AWS_EXECUTION_ENV` environment variable. When running on Glue, this variable starts with `AWS_Glue`.

```python
from simpleetl.platforms.detector import is_aws_glue

if is_aws_glue():
    print("Running on AWS Glue")
```

---

## Databricks

### Overview

SimpleETL can run on Databricks. The `DatabricksPlatformRunner` detects the Databricks environment via the `DATABRICKS_RUNTIME_VERSION` environment variable.

### Deployment Steps

1. **Install on Databricks Cluster:**

   Upload the package to DBFS or install via pip:

   ```python
   # In a Databricks notebook cell
   %pip install simpleetl
   ```

2. **Create a Notebook Job:**

   ```python
   # Databricks notebook
   from simpleetl.core.job import ETLJob
   from simpleetl.formats import FormatFactory

   class MyDatabricksJob(ETLJob):
       def run(self):
           reader = FormatFactory.get_reader(self.config.params["input_path"])
           data = reader.read(self.config.params["input_path"])

           # Transform
           data = data[data["age"] >= 18]

           writer = FormatFactory.get_writer(self.config.params["output_path"])
           writer.write(data, self.config.params["output_path"])

   job = MyDatabricksJob({
       "name": "databricks_job",
       "platform": "databricks",
       "input_format": "csv",
       "output_format": "parquet",
       "params": {
           "input_path": "/dbfs/mnt/data/input.csv",
           "output_path": "/dbfs/mnt/data/output.parquet"
       }
   })
   job.run_with_error_handling()
   ```

3. **Configure as a Job Task:**

   - Go to Databricks Jobs
   - Create a new Notebook task
   - Point to your ETL notebook
   - Set the cluster configuration

### Environment Detection

```python
from simpleetl.platforms.detector import is_databricks

if is_databricks():
    print("Running on Databricks")
```

---

## Azure Synapse

### Overview

SimpleETL can run on Azure Synapse Analytics. The `SynapsePlatformRunner` detects the Synapse environment via the `AZURE_SYNAPSE_SPARK_POOL_NAME` environment variable.

### Deployment Steps

1. **Install on Synapse Spark Pool:**

   ```python
   # In a Synapse notebook cell
   %pip install simpleetl
   ```

2. **Create a Notebook Job:**

   ```python
   # Synapse notebook
   from simpleetl.core.job import ETLJob
   from simpleetl.formats import FormatFactory

   class MySynapseJob(ETLJob):
       def run(self):
           reader = FormatFactory.get_reader(self.config.params["input_path"])
           data = reader.read(self.config.params["input_path"])

           # Transform
           data = data[data["revenue"] > 1000]

           writer = FormatFactory.get_writer(self.config.params["output_path"])
           writer.write(data, self.config.params["output_path"])

   job = MySynapseJob({
       "name": "synapse_job",
       "platform": "synapse",
       "input_format": "csv",
       "output_format": "parquet",
       "params": {
           "input_path": "abfss://container@storage.dfs.core.windows.net/input/data.csv",
           "output_path": "abfss://container@storage.dfs.core.windows.net/output/data.parquet"
       }
   })
   job.run_with_error_handling()
   ```

3. **Configure as a Pipeline Activity:**

   - Go to Azure Synapse Studio
   - Create a new Pipeline
   - Add a Notebook activity
   - Point to your ETL notebook

### Environment Detection

```python
from simpleetl.platforms.detector import is_azure_synapse

if is_azure_synapse():
    print("Running on Azure Synapse")
```

---

## Docker

### Overview

SimpleETL includes a `Dockerfile` and `docker-compose.yml` for containerized execution.

### Building the Image

```bash
docker build -t simpleetl:latest .
```

### Running a Job

```bash
docker run --rm \
  -v $(pwd)/configs:/app/configs \
  -v $(pwd)/examples:/app/examples \
  simpleetl:latest \
  uv run python -m simpleetl --config configs/my_job.yaml
```

### Docker Compose

The included `docker-compose.yml` sets up SimpleETL with PostgreSQL and MySQL for testing database operations.

```bash
# Start all services
docker compose up

# Run SimpleETL with dependencies
docker compose up simpleetl

# Run tests
docker compose run --rm simpleetl uv run pytest tests/ -v
```

**Services:**

| Service | Port | Description |
|---|---|---|
| simpleetl | 8000 | SimpleETL application with metrics endpoint |
| postgres | 5432 | PostgreSQL for database format testing |
| mysql | 3306 | MySQL for database format testing |

### Custom Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc g++ && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
RUN pip install uv && uv sync --frozen

COPY src/ ./src/
COPY configs/ ./configs/

RUN useradd -m -u 1000 etluser && chown -R etluser:etluser /app
USER etluser

EXPOSE 8000

CMD ["uv", "run", "python", "-m", "simpleetl"]
```

---

## Kubernetes

### Overview

SimpleETL includes Kubernetes manifests in the `k8s/` directory for deploying to a K8s cluster.

### Manifests

| File | Description |
|---|---|
| `k8s/namespace.yaml` | Creates the `simpleetl` namespace |
| `k8s/configmap.yaml` | Configuration for the ETL job |
| `k8s/service-account.yaml` | Service account for the deployment |
| `k8s/deployment.yaml` | Main deployment with health probes |
| `k8s/service.yaml` | ClusterIP service for metrics |
| `k8s/kustomization.yaml` | Kustomize configuration |

### Deployment Steps

1. **Build and push the Docker image:**

   ```bash
   docker build -t my-registry/simpleetl:latest .
   docker push my-registry/simpleetl:latest
   ```

2. **Update the deployment image:**

   Edit `k8s/deployment.yaml` to use your image:

   ```yaml
   image: my-registry/simpleetl:latest
   ```

3. **Apply the manifests:**

   ```bash
   kubectl apply -k k8s/
   ```

   Or apply individually:

   ```bash
   kubectl apply -f k8s/namespace.yaml
   kubectl apply -f k8s/configmap.yaml
   kubectl apply -f k8s/service-account.yaml
   kubectl apply -f k8s/deployment.yaml
   kubectl apply -f k8s/service.yaml
   ```

4. **Verify the deployment:**

   ```bash
   kubectl get pods -n simpleetl
   kubectl logs -n simpleetl -l app=simpleetl
   ```

### Health Probes

The deployment includes liveness and readiness probes:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8000
  initialDelaySeconds: 30
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /ready
    port: 8000
  initialDelaySeconds: 5
  periodSeconds: 5
```

### Resource Limits

```yaml
resources:
  requests:
    memory: "256Mi"
    cpu: "250m"
  limits:
    memory: "512Mi"
    cpu: "500m"
```

### Accessing Metrics

```bash
# Port-forward to access metrics
kubectl port-forward -n simpleetl svc/simpleetl-service 8000:8000

# View metrics
curl http://localhost:8000/metrics

# Health check
curl http://localhost:8000/health
curl http://localhost:8000/ready
```

### Scaling

```bash
# Scale the deployment
kubectl scale deployment simpleetl --replicas=3 -n simpleetl
```

---

## Platform Detection

SimpleETL automatically detects the current platform:

```python
from simpleetl.platforms.detector import get_current_platform, get_platform_info

platform = get_current_platform()
# Returns: 'local', 'glue', 'databricks', or 'synapse'

info = get_platform_info()
# Returns: {
#   'platform': 'local',
#   'is_glue': False,
#   'is_databricks': False,
#   'is_synapse': False,
#   'system': 'Darwin',
#   'python_version': '3.11.0',
#   'environment': {...}
# }
```

You can also override the platform via CLI:

```bash
python -m simpleetl --config config.yaml --platform glue
```
