import json
import sqlite3
import os
import pandas as pd
import requests
from zipfile import ZipFile

def download_data(url, destination):
    response = requests.get(url)
    with open(destination, 'wb') as file:
        file.write(response.content)

def extract_zip(zip_path, extract_folder):
    with ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_folder)
def create_database_schema(conn):
    # Database schema DDL
    conn.execute('''
    CREATE TABLE IF NOT EXISTS matches (
        match_id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_name TEXT,
        match_type TEXT,
        outcome_result TEXT,
        toss_decision TEXT,
        toss_winner TEXT,
        venue TEXT,
        city TEXT,
        start_date TEXT,
        gender TEXT,
        season TEXT,
        balls_per_over INTEGER
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS innings (
            inning_id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INTEGER,
            team_name TEXT,
            FOREIGN KEY (match_id) REFERENCES matches (match_id)
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS deliveries (
            delivery_id INTEGER PRIMARY KEY AUTOINCREMENT,
            inning_id INTEGER,
            batter TEXT,
            bowler TEXT,
            non_striker TEXT,
            runs_batter INTEGER,
            runs_extras INTEGER,
            runs_total INTEGER,
            FOREIGN KEY (inning_id) REFERENCES innings (inning_id)
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS players (
            player_id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_name TEXT UNIQUE
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS match_players (
            match_id INTEGER,
            player_id INTEGER,
            team_name TEXT,
            PRIMARY KEY (match_id, player_id),
            FOREIGN KEY (match_id) REFERENCES matches (match_id),
            FOREIGN KEY (player_id) REFERENCES players (player_id)
        )
    ''')

def ingest_data(data_folder, db_path):
    conn = sqlite3.connect(db_path)
    zip_path = os.path.join(data_folder, "data.zip")
    extract_folder = os.path.join(data_folder, "extracted_data")
    extract_zip(zip_path, extract_folder)
    Create tables if they don't exist
    create_database_schema(conn)

    for file_name in os.listdir(extract_folder):
        file_path = os.path.join(extract_folder, file_name)

         # Check if the file is not empty
        if os.path.getsize(file_path) > 0:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                try:
                    data = json.load(file)
                    # Extract match information
                    match_info = data.get('info', {})
                    event_name = match_info.get('event', {}).get('name', '')
                    match_type = match_info.get('match_type', '')
                    # Convert match_info to DataFrame
                    match_df = pd.DataFrame([match_info])

                    # Get the columns present in the 'matches' table
                    matches_columns = [col for col in match_df.columns if col in ('event_name', 'match_type', 'outcome_result', 'toss_decision', 'toss_winner', 'venue', 'city', 'start_date', 'gender', 'season', 'balls_per_over')]

                    # Insert only the selected columns into 'matches' table
                    match_df[matches_columns].to_sql('matches', conn, if_exists='append', index=False)
                    
                    match_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]

                    # Extract innings information
                    innings_data = data.get('innings', [])
                    for inning in innings_data:
                        team_name = inning.get('team', '')
                        # Convert inning to DataFrame
                        inning_df = pd.DataFrame([{'match_id': match_id, 'team_name': team_name}])

                        # Get the columns present in the 'innings' table
                        innings_columns = [col for col in inning_df.columns if col in ('match_id', 'team_name')]

                        # Insert only the selected columns into 'innings' table
                        inning_df[innings_columns].to_sql(f'innings_{event_name}_{match_type}', conn, if_exists='append', index=False)
                        inning_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]

                        # Extract deliveries information
                        deliveries_data = inning.get('overs', [])
                        # Convert deliveries_data to DataFrame
                        deliveries_df = pd.DataFrame(deliveries_data)

                        # Get the columns present in the 'deliveries' table
                        deliveries_columns = [col for col in deliveries_df.columns if col in ('inning_id', 'batter', 'bowler', 'non_striker', 'runs_batter', 'runs_extras', 'runs_total')]

                        # Insert only the selected columns into 'deliveries' table
                        deliveries_df[deliveries_columns].assign(inning_id=inning_id).to_sql('deliveries', conn, if_exists='append', index=False)

                        # Extract player information and populate the 'players' and 'match_players' tables
                        players_data = set()
                        deliveries_data = inning.get('overs', [])
                        for delivery in deliveries_data:
                            batter = delivery.get('batter', '')
                            bowler = delivery.get('bowler', '')
                            non_striker = delivery.get('non_striker', '')
                            runs_batter = delivery.get('runs', {}).get('batter', 0)
                            runs_extras = delivery.get('runs', {}).get('extras', 0)
                            runs_total = delivery.get('runs', {}).get('total', 0)

                            delivery_data = {
                                'inning_id': inning_id,
                                'batter': batter,
                                'bowler': bowler,
                                'non_striker': non_striker,
                                'runs_batter': runs_batter,
                                'runs_extras': runs_extras,
                                'runs_total': runs_total
                            }

                        pd.DataFrame([delivery_data]).to_sql('deliveries', conn, if_exists='append', index=False)
                        for player_name in players_data:
                            # Add players to the 'players' table if they don't exist
                            conn.execute('INSERT OR IGNORE INTO players (player_name) VALUES (?)', (player_name,))

                            # Add players to the 'match_players' table
                            player_id = conn.execute('SELECT player_id FROM players WHERE player_name = ?', (player_name,)).fetchone()[0]
                            conn.execute('INSERT INTO match_players (match_id, player_id, team_name) VALUES (?, ?, ?)', (match_id, player_id, team_name))

                except Exception as e:
                    print(f"Error decoding JSON in file: {file_path}"+str(e))

    conn.close()


if __name__ == "__main__":
    cricsheet_url = "https://cricsheet.org/downloads/all_json.zip" #we can pass url here
    data_folder = "data"
    os.makedirs(data_folder, exist_ok=True)
    db_path = "crick.db"

    download_data(cricsheet_url, os.path.join(data_folder, "data.zip"))

    ingest_data(data_folder, db_path)
