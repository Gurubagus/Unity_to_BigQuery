import csv, json
from google.cloud import bigquery

class BigQuery_uploader: 
	
	#This class will upload the selected file with its designated file format to 
	# to the designated dataset/table inputed through the main method
	
	def __init__(self):
		
		# Will initialize the BigQuery Client, script e.g. BQ = BigQuery_uploader()	
		self.bigquery_client = bigquery.Client()
		
	def main(self, dataset, table, file, file_format):
		
		# Usage example with script initialize example above: BQ.main("<Your dataset name>", "<your table name>", "<path to your file and file name>", "<file type e.g. CSV>")
		try:	
			dataset = self.bigquery_client.dataset(dataset) # Sets chosen dataset
			table = dataset.table(table) # Sets chosen table

			with open(file,'rb') as source_file: # Opens chosen file to be loaded to BigQuery
				job_config = bigquery.LoadJobConfig() # Configs Job 
				
				if file_format == 'CSV':
					job_config.skip_leading_rows = 1 #Skips CSV Headers
					
				job_config.source_format = file_format # Sets format
				job = self.bigquery_client.load_table_from_file(source_file, table, job_config=job_config) # Loads table to BigQuery table
				print("Starting job {}".format(job.job_id))
				job.result()  # Waits for table load to complete.
				print("Job finished.")
				destination_table = self.bigquery_client.get_table(table)
				#print("Loaded {} rows.".format(source_file.num_rows))
				
				return job.job_id
			
		except Exception as e:
			print(e) # Will print error if applicable