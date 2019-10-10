import os

class Error_Notifier:
	
	def main(channel, error):

		if channel == "<input channel name here>":
			channel = "<slack channel URL>"

		message = "{\"text\": \"" + error + "\"}"
		os.system("curl -X POST -H 'Content-type: application/json' --data '" + message + "' "+ channel +"")