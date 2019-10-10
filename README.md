# Unity_to_BigQuery
Automated ETL from Unity Analytics Raw Data Export to GCP BigQuery table

This contains:
- Master Unity_to_BigQuery.py file
- example config.json for Unity connection (must be replaced with new info)
- BigQuery uploader .py file
- Slack error notifier .py
- requirements.txt for pip3 install

Important note: Designed for Linux systems,you will also be required to update location of your GCP authentication file with command (personal solution, have bash file run both export command and .py script to avoid errors): 
export GOOGLE_APPLICATION_CREDENTIALS="<dir/for/auth/file>"

TO RUN example:
python3 Unity_to_BigQuery.py /path/to/config/file

This script will call Unity's Raw Data Export system and request each of the reports you wish and upload them to their corresponding BigQuery Table, will work best when naming conventions stay the same, e.g. Table name = Report Name
It will also store data into a local database so you can request data from the end of the last report created, making sure you never miss any data OR you can choose a start date.
On first run, it will request the maximum amount of historical data from the Unity system (30 days), upload it to your tables and store the data on a local directory of your choosing.
