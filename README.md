Download the Data:
Set the cricsheet_url variable to the URL where the data is hosted.
Run the script. It will download the data and save it to the specified data_folder. You can specify the URL and folder when running the script.
python script_name.py --url https://example.com/data.zip --folder data


Ingest Data into the Database:
Set the db_path variable to the desired path for the SQLite database.
Run the script again. It will ingest the data into the database.
python script_name.py --db_path cricket_data.db
