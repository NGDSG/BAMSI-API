BAMSI-api
=========

Official BAMSI python API.

# Installation
git clone link

# Usage

```
from src.api import BAMSIApiClient
```


```python
KEY = 'test'
IP='localhost:8888/'
bamsi_client = BAMSIApiClient(KEY, IP)
```

### Get information on the worker pool
```python
active_workers = bamsi_client.active_workers()
```


## Submit a filtering job
```python
# Define a job
query = '{"regions" : "1:1-30000", "subpops" : "CHB,JPT,CHS", "format" : "b"}'
query_args = json.loads(query)

# Launch the job
job_tracking_id = bamsi_client.spawn(**query_args)
```

### Status
```python
job_status = bamsi_client.job_status(tracking=job_tracking_id)
```


