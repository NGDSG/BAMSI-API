from src.api import BAMSIApiClient
import json
import time

KEY = 'test'
IP='localhost:8888/'
bamsi_client = BAMSIApiClient(KEY, IP)

# Get information on the state of the worker pool
active_workers = json.loads(bamsi_client.active_workers())
print("Active workers: " + str(active_workers.keys()))

num_active_workers = len(active_workers)

if num_active_workers > 0:

	# Define a job
	query = '{"regions" : "1:1-30000", "subpops" : "CHB,JPT,CHS", "format" : "b"}'
	query_args = json.loads(query)

	# Launch the job
	job_tracking_id = bamsi_client.spawn(**query_args)

	# Check the status of the job
	job_status = bamsi_client.job_status(tracking=job_tracking_id)
	limit = 10
	n = 0

	# Wait until all tasks of the job are done, or the time limit is reached
	while job_status != "COMPLETED" and n < limit:
		time.sleep(60)
		job_status = bamsi_client.job_status(tracking=job_tracking_id)
		print("The status of job " + job_tracking_id + " is " + job_status)
		n += 1

	# Get information on the tasks that have finished
	results_stats = json.loads(bamsi_client.results_stats(tracking=job_tracking_id))

	# Get list of URLs from where the results of the finished tasks can be downloaded
	urls = results_stats["URLs"]



