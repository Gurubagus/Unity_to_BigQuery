import requests
from requests.auth import HTTPBasicAuth
import os
import gzip
import sys
import io
sys.path.append("/home/zac/PyResources") #Directory for additional custom Classes
import json
import urllib
from datetime import *
import time
from sqlalchemy import create_engine, MetaData, Table, Integer, DateTime, String, Column, select, BigInteger, Numeric
from sqlalchemy.dialects import postgresql
from bq_uploader import BigQuery_uploader # Class from PyResources
from slack_notifier import Error_Notifier

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
		self.is_files = True
		
	def request_raw_analytics_dump(self, unity_project_id, start_date, end_date, dump_format, data_set,
								   continue_from=None):

		uri = self.uri + unity_project_id + '/rawdataexports'

		postBodyJson = {'endDate': end_date, 'format': dump_format, 'dataset': data_set}

		if continue_from is not None:
			postBodyJson['continueFrom'] = continue_from
		else:
			postBodyJson['startDate'] = start_date
		
		headers = {'content-type': 'application/json'}
		r = requests.post(uri, headers=headers, json=postBodyJson, auth=HTTPBasicAuth(unity_project_id, self.CONFIG['{}'.format(unity_project_id)]))

		if r.status_code == 200:
			return r.json()['id']
		else:
			print(r.text)
			return None
	       
							  
	def is_raw_analytics_dump_ready(self, unity_project_id, unity_api_key, job_id):

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

		#return None #for when you ust make custom request
				
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
			print('No files for job: ' + job_id)
			self.is_files = False
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
					#outFile.write(decompressed_file.read())
					file = outFile.write(decompressed_file.read())
					fname = str(daily_directory + "/" + fileName)
					if unity_project_id == "67a658d0-5a68-405a-b489-452ade4b929d":
						upload = BigQuery_uploader().main("Hammer_Jump",job_type,fname,"NEWLINE_DELIMITED_JSON")
					elif unity_project_id == "58f091bd-7a0f-48b9-bc9a-c32b9fd140a6":
						upload = BigQuery_uploader().main("Pole_Sprint",job_type,fname,"NEWLINE_DELIMITED_JSON")
					elif unity_project_id == "7456b5d0-e2f4-48cc-8b00-389d8ebd016f":
						upload = BigQuery_uploader().main("Tricky_Tower_3D",job_type,fname,"NEWLINE_DELIMITED_JSON")
					elif unity_project_id == "ce926feb-81ea-412e-8c26-2304176eb18a":
						upload = BigQuery_uploader().main("Bendy_Bug",job_type,fname,"NEWLINE_DELIMITED_JSON")
					
	def main(self, job_type, unity_project_id, local_dump_directory):

		print('collector: starting collection for job: ' + job_type)
		continuationJobId = self.find_previous_job_id(job_type, unity_project_id)
		
		start_date =  self.today - timedelta(days=30)
		#start_date = "2019-10-28" #if custom is required, also comment out find_previous_job_id except return None 

		jobId = self.request_raw_analytics_dump(unity_project_id, str(start_date), str(self.today),'json', job_type, continuationJobId)
		

		print('started jobId: ' + jobId)
		print("Waiting for data dump to be ready...this may take several minutes.")
		
		while not self.is_raw_analytics_dump_ready(unity_project_id,
											  self.CONFIG['{}'.format(unity_project_id)], jobId):
			time.sleep(5)
			self.wait_time += 5
		print("Total wait time: {} seconds".format(self.wait_time))
		self.wait_time = 0
		self.save_raw_analytics_dump(unity_project_id,  self.CONFIG['{}'.format(unity_project_id)], jobId, job_type,
								local_dump_directory)
		
		
		# keep track of the last jobId we ingested for continuation next time
		
		if self.is_files == True:
			
			self.conn.execute(self.job_id_table.insert().values(ts=self.today, job_Id=jobId, job_Type=job_type, app_id=unity_project_id, previous_job_id=continuationJobId))
			print('done! all results for job ' + job_type + ' saved to: ' + local_dump_directory)
		else:
			print("No files for job, this job will not be registered in Database.")
			self.is_files = True
		
		print('*** COMPLETE ***')
		
							  
if __name__ == "__main__":
	try:
		report_types = ["appStart", "appRunning", "deviceInfo", "custom", "transaction"]
		#report_types = ["custom"]
		try:
			with open("/home/zac/Auth/UA_config.json", "r") as f: #REPLACE AS sys.argv[1] when finished
				CONFIG = json.load(f)
		except Exception as e:
			message = "Unity Analytics autoupload ERROR: " + str(e)
			error = Error_Notifier.main("error_log", message)
			print('failed to read or parse config file')
			exit(1)
		Job = Unity_Analytics_API()
		for UPID in CONFIG['unity_project_id']:
			if UPID == "67a658d0-5a68-405a-b489-452ade4b929d":
				print("\nHammer Jump:\n")
			elif UPID == "58f091bd-7a0f-48b9-bc9a-c32b9fd140a6":
				print("\nPole Sprint:\n")
			elif UPID == "7456b5d0-e2f4-48cc-8b00-389d8ebd016f":
				print("\nTricky Tower 3D:\n")
			elif UPID == "ce926feb-81ea-412e-8c26-2304176eb18a":
				print("\nBendy Bug:\n")
			for report in report_types:
				task = Job.main(report, UPID, CONFIG['local_collection_path'])
	except Exception as e:
		message = "Unity Analytics autoupload ERROR: " + str(e)
		error = Error_Notifier.main("error_log", message)
	
