import requests
from requests.auth import HTTPBasicAuth
import os
import gzip
import sys
import io
#sys.path.append("<dir/for/slack_notifier/and/or/bq_uploader/if/different>") #Directory for additional custom Classes, CHANGE TO LOCAL DIR where BigQuery/Slack Notifier is stored if seperate directory
import json
import urllib
from datetime import *
import time
from sqlalchemy import create_engine, MetaData, Table, Integer, DateTime, String, Column, select, BigInteger, Numeric
from sqlalchemy.dialects import postgresql
from bq_uploader import BigQuery_uploader #bq_uploader.py file
from slack_notifier import Error_Notifier #slack_notifier.py


class Unity_Analytics_API:

	def __init__(self):
		
		self.CONFIG = {}
		self.CONFIG = CONFIG

		if not self.CONFIG['user'] or not self.CONFIG['password'] or not self.CONFIG['postgres_server'] \
			or not self.CONFIG['database'] or not self.CONFIG['local_collection_path'] \
			or not self.CONFIG['unity_project_id']:
		
			print("missing parameter in config.json. see docs.")
			exit(1)

		# figure out home directory if necessary
		self.CONFIG['local_collection_path'] = os.path.expanduser(self.CONFIG['local_collection_path'])	
		
		self.metadata = MetaData()

		# Create schema for Job Info Storage on local system, this allows for recovery of previous Job IDs and descriptive data in case of issue
		self.job_id_table = Table('ua_completed_reports', self.metadata,
			Column('job_Id', Integer, primary_key=True),
			Column('job_Type', String),
			Column('ts', DateTime),
			Column('app_id', String),
			Column('previous_job_id', String),
			Column('date_range', String))

		self.engine = create_engine('postgresql+psycopg2://' + self.CONFIG['user'] + ':' + self.CONFIG['password'] + 
									'@' + self.CONFIG['postgres_server'] + '/' + self.CONFIG['database'])

		self.conn = self.engine.connect()
		self.metadata.create_all(self.engine)
		
		self.wait_time = 0
		self.uri = "https://analytics.cloud.unity3d.com/api/v2/projects/"  
		self.today = date.today()
		self.yesterday = date.today() - timedelta(days=1)
		
	def request_raw_analytics_dump(self, unity_project_id, start_date, end_date, dump_format, data_set,
								   continue_from=None): #Raw data request method

		uri = self.uri + unity_project_id + '/rawdataexports'

		postBodyJson = {'endDate': end_date, 'format': dump_format, 'dataset': data_set}

		if continue_from is not None: #If previous Job ID exists, will be entered here
			postBodyJson['continueFrom'] = continue_from
		else:
			postBodyJson['startDate'] = start_date #Otherwise chooses start date (from main() method)
		
		headers = {'content-type': 'application/json'}
		r = requests.post(uri, headers=headers, json=postBodyJson, auth=HTTPBasicAuth(unity_project_id, self.CONFIG['{}'.format(unity_project_id)]))

		if r.status_code == 200:
			return r.json()['id']
		else:
			print(r.text)
			return None
	       
							  
	def is_raw_analytics_dump_ready(self, unity_project_id, unity_api_key, job_id): #Checks for dump to be ready

		uri = 'https://analytics.cloud.unity3d.com/api/v2/projects/' + unity_project_id + '/rawdataexports/' + job_id
		r = requests.get(uri, auth=HTTPBasicAuth(unity_project_id, unity_api_key))

		if r.status_code == 200:
			return r.json()['status'] == 'completed'

		return False
	
	def find_previous_job_id(self, job_type, unity_project_id):# returns the last job stored in the database for a job type, if it exists
		s = select([self.job_id_table]).where(self.job_id_table.c.job_Type == job_type).where(self.job_id_table.c.app_id == unity_project_id).order_by(self.job_id_table.c.ts.desc())
		selectResult = self.conn.execute(s)
		result = selectResult.fetchone()
		selectResult.close()

		if result is None:
			return None

		print('found previous job ' + result['job_Id'] + ' for job type ' + job_type + " on app " + unity_project_id)

		return result['job_Id']
				
	# extracts and un-compresses all result files in a job
	def save_raw_analytics_dump(self,unity_project_id, unity_api_key, job_id, job_type,destination_directory):
		daily_directory = str(destination_directory + "/" + unity_project_id +"/" + job_type + "/" + str(self.yesterday) + "_" + job_id)
		if not os.path.exists(daily_directory):
			os.makedirs(daily_directory)

		uri = 'https://analytics.cloud.unity3d.com/api/v2/projects/' + unity_project_id + '/rawdataexports/' + job_id
		r = requests.get(uri, auth=HTTPBasicAuth(unity_project_id, unity_api_key))

		if r.status_code != 200:
			print('unable to retrieve result due to HTTP error: ' + r.status_code)
			print('URI: ' + uri)
			return

		responseJson = r.json()

		if responseJson['status'] != 'completed':
			print('job status not completed... can\'t dump results yet')
			return

		if 'fileList' not in responseJson['result']:
			print('no files for job: ' + job_id)
			return

		for fileToDownload in responseJson['result']['fileList']:
			fileUri = fileToDownload['url']
			fileName = os.path.splitext(fileToDownload['name'])[0]  # file name w/o extension
			fileName = fileName + ".json"
			fileRequest = requests.get(fileUri)

			if fileRequest.status_code == 200:
				compressed_file = io.BytesIO(fileRequest.content)
				decompressed_file = gzip.GzipFile(fileobj=compressed_file)
				
				with open(os.path.join(daily_directory, fileName), 'w+b') as outFile:
					file = outFile.write(decompressed_file.read())
					fname = str(daily_directory + "/" + fileName)
					#If have multiple apps/tables to upload to, list unity_project_id below and table name
					if unity_project_id == "<unity_project_id>": #CHANGE TO PERSONAL SETTING
						upload = BigQuery_uploader().main("<BigQuery Table Name>",job_type,fname,"NEWLINE_DELIMITED_JSON")
					elif unity_project_id == "<unity_project_id>":#CHANGE TO PERSONAL SETTING
						upload = BigQuery_uploader().main("<BigQuery Table Name>",job_type,fname,"NEWLINE_DELIMITED_JSON")
					
	def main(self, job_type, unity_project_id, local_dump_directory):

		print('collector: starting collection for job: ' + job_type)
		continuationJobId = self.find_previous_job_id(job_type, unity_project_id) #Looks for previous Job ID
		
		start_date =  self.today - timedelta(days=30) #start date with maximum historical data allowed by Unity

		jobId = self.request_raw_analytics_dump(unity_project_id, str(start_date), str(self.today),'json', job_type, continuationJobId) #Requests Raw Data Dump
		

		print('started jobId: ' + jobId)
		print("Waiting for data dump to be ready...this may take several minutes.")
		
		#Checks Unity every (5) seconds for Dump to be ready
		while not self.is_raw_analytics_dump_ready(unity_project_id,
											  self.CONFIG['{}'.format(unity_project_id)], jobId):
			time.sleep(5) #Choose frequency of data dump ready check
			self.wait_time += 5
			
		print("Total wait time: {} seconds".format(self.wait_time))
		
		self.wait_time = 0
		
		#Saves dump to local directory
		self.save_raw_analytics_dump(unity_project_id,  self.CONFIG['{}'.format(unity_project_id)], jobId, job_type,
								local_dump_directory)
		
		print('done! all results for job ' + job_type + ' saved to: ' + local_dump_directory)

		now = str(datetime.now())
		date_range = str(start_date) + "_to_" + now
		# keep track of the last jobId we ingested for continuation next time
		self.conn.execute(self.job_id_table.insert().values(ts=now, job_Id=jobId, job_Type=job_type, app_id=unity_project_id, previous_job_id=continuationJobId, date_range=date_range))
		
		print('*** COMPLETE ***')
		
							  
if __name__ == "__main__":
	try:
		report_types = ["appStart", "appRunning", "deviceInfo", "custom", "transaction"] #List of reports desired
		try:
			with open(sys.argv[1], "r") as f:
				CONFIG = json.load(f)
		except Exception as e:
			message = "Unity Analytics autoupload ERROR: " + str(e)
			error = Error_Notifier.main("<channel name>", message) #CHANGE TO PERSONAL SETTING
			print('failed to read or parse config file')
			exit(1)
		Job = Unity_Analytics_API()
		for UPID in CONFIG['unity_project_id']:
			for report in report_types:
				task = Job.main(report, UPID, CONFIG['local_collection_path'])
	except Exception as e:
		message = "Unity Analytics autoupload ERROR: " + str(e)
		error = Error_Notifier.main("<channel name>", message) #CHANGE TO PERSONAL SETTING
	