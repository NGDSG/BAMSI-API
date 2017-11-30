try:
	import simplejson as json
except :
	import json


import requests

__all__ = [
	'BAMSIApiClient'
]

################################################################################

class HttpException(Exception):
	def __init__(self, code, reason, error='Unknown Error'):
		self.code = code
		self.reason = reason
		self.error_message = error
		super(HttpException, self).__init__()

	def __str__(self):
		return '\n Status: %s \n Reason: %s \n Error Message: %s\n' % (self.code, self.reason, self.error_message)

################################################################################


class HttpApiClient(object):
	"""
	Base implementation for an HTTP
	API Client. Used by the different
	API implementation objects to manage
	Http connection.
	"""

	def __init__(self, api_key, base_url):
		"""Initialize base http client."""
		self.api_key = api_key
		self.base_url = base_url

	def _http_request(self, service_type, **kwargs):
		"""
		Perform an HTTP Request using base_url and parameters
		given by kwargs.
		Results are expected to be given in JSON format
		and are parsed to python data structures.
		"""

		uri = '%s%s?api_key=%s' % \
			(self.base_url, service_type, self.api_key)
		response = requests.get(uri, params=kwargs)
		return response.headers, response


	def _is_http_response_ok(self, response):
		return response['status'] == '200' or response['status'] == 200

	def _get_params(self, tracking=None, regions = None, inds=None, subpops = None, format = None, seq=None,
			  minTlen = None, maxTlen = None, num_files = None, tags_incl = None, tags_excl = None, cigar = None,
			  f = None, F = None, q = None, fields = None, ids=None):

		params = {}
		if tracking:
			params['tracking'] = tracking
		if inds:
			params['inds'] = inds
		if regions:
			params['regions'] = regions
		if subpops:
			params['subpops'] = subpops
		if format:
			params['format'] = format
		if minTlen:
			params['minTlen'] = minTlen
		if maxTlen:
			params['maxTlen'] = maxTlen
		if seq:
			params['seq'] = seq
		if tags_incl:
			params['tags_incl'] = tags_incl
		if tags_excl:
			params['tags_excl'] = tags_excl
		if cigar:
			params['cigar'] = cigar
		if f:
			params['f'] = f
		if F:
			params['F'] = F
		if q:
			params['q'] = q
		if num_files:
			params['num_files'] = num_files
		if fields:
			params['fields'] = fields
		if ids:
			params['ids'] = ids

		return params


	def _create_query(self, category_type, params):
		header, response = self._http_request(category_type , **params)
		resp =  response.text
		if not (response.status_code == requests.codes.ok):
			raise response.raise_for_status()
		return resp


################################################################################

class BAMSIApiClient(HttpApiClient):

	def __init__(self, api_key, ip):
		self.ip = ip
		self.api_url  = 'http://' + ip
		base_url = self.api_url
		super(BAMSIApiClient, self).__init__(api_key, base_url)


	def spawn(self, regions = None, inds=None, subpops = None, format = None, seq=None,
			  minTlen = None, maxTlen = None, num_files = None, tags_incl = None, tags_excl = None, cigar = None,
			  f = None, F = None, q = None):
		"""
		Spawns a filtering job.

		Args:
		  regions         : Filter criteria
			type : [string]
		  subpops          : Sub population code (if need to be narrowed)
			type : [string]
		  format          : Whether to reformat
			type : [string]
		  nfs            : Use NFS or S3
			type : [string]

		Returns:
		  Tracking Id in a string form

		Raises:
		  HttpException with the error message from the server
		"""

		params = self._get_params(regions = regions, subpops = subpops, format = format, num_files=num_files,
								  inds = inds, seq=seq, minTlen=minTlen, maxTlen=maxTlen,
								  tags_incl=tags_incl, tags_excl=tags_excl, cigar=cigar, f=f, F=F, q=q)
		response = self._create_query('spawn', params)
		if(response.split()[0] == "Submitted"):
			return response.split('.')[1]
		else:
			return response

	def celery_status(self, tracking):
		"""
		Get the status of a job, as reported by celery.result:AsyncResult

		Args:
		  tracking id: Identifier of the filtering job

		Returns:
		  JSON string with Status and TrackingId

		.. note::
			This seems to always be PENDING, even when all tasks are completed. This is
			a celery issue.

		Raises:
		  HttpException with the error message from the server
		"""
		params = self._get_params(tracking = tracking)
		return self._create_query('status', params)


	def job_status(self, tracking):
		"""
		Get the status of a job.

		Args:
		  tracking id: Identifier of the filtering job

		Returns:
		  string: "COMPLETED" if all tasks in job have status "COMPLETED"
		  		  "ERROR" if any jobs have status "ERROR"
		  		  "PROGRESS" otherwise

		Raises:
		  HttpException with the error message from the server
		"""
		params = self._get_params(tracking = tracking, fields=['status'])
		job_data = self.job_stats(**params)
		meta_data = json.loads(job_data)[0]
		data = json.loads(job_data)[1]
		num_tasks = int(meta_data["total_entries"])
		completed_counter = 0
		for task in range(num_tasks):
			if(data[task]["status"] == "COMPLETED"):
				completed_counter +=1
			if(data[task]["status"] == "ERROR"):
				return "ERROR"

		if completed_counter == num_tasks:
			return "COMPLETED"
		else:
			return "PROGRESS"



	def job_stats(self, tracking, fields = None):
		"""
		Get the data of a specific job.

		Args:
		  tracking (str): id of the job
		  fields (str list): which fields to fetch from the database entry of the job. Optional.

		Returns:
		  JSON string containing the data for the given job.

		Raises:
		  HttpException with the error message from the server
		"""
		params = self._get_params(tracking = tracking, fields = fields)

		return self._create_query('jobs', params)

	def results_stats(self, tracking, ids = None):
		"""
		Get urls from which results can be downloaded.

		Args:
		  tracking (str): id of the job
	  	  ids (str list): list of individual ids specifying which files to download. Only files that
   		  have status COMPLETED will be selected.
		  if ids are not specified, all files whose tasks have status COMPLETED are selected.

		Returns:
		  JSON string containing the download URLs.

		Raises:
		  HttpException with the error message from the server
		"""

		if not ids:
			ids = []
			params = self._get_params(tracking = tracking, fields=['status', 'individual'])
			job_data = self.job_stats(**params)
			meta_data = json.loads(job_data)[0]
			data = json.loads(job_data)[1]
			num_tasks = int(meta_data["total_entries"])
			for task in range(num_tasks):
				if(data[task]["status"] == "COMPLETED"):
					ids.append(data[task]["individual"])


		ids_string = ",".join(map(str, ids))
		params = self._get_params(tracking = tracking, ids = ids_string)

		return self._create_query('download', params)




	def active_workers(self):
		"""
		Check the status of the worker pool.

		Returns:
		  JSON string containing the active workers.
		Raises:
		  HttpException with the error message from the server
		"""
		params =  self._get_params()
		return self._create_query('poke', params)

