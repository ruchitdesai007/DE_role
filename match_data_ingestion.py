import json
import sqlite3
import os
import pandas as pd
import requests

def download_data(url, destination):
    response = requests.get(url)
    with open(destination, 'wb') as file:
        file.write(response.content)


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
            season TEXT
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

    # Create tables if they don't exist
    create_database_schema(conn)

    for file_name in os.listdir(data_folder):
        file_path = os.path.join(data_folder, file_name)

        with open(file_path, 'r') as file:
            data = json.load(file)

            # Extract match information
            match_info = data.get('info', {})
            event_name = match_info.get('event', {}).get('name', '')
            match_type = match_info.get('match_type', '')

            pd.DataFrame([match_info]).to_sql('matches', conn, if_exists='append', index=False)
            match_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]

            # Extract innings information
            innings_data = data.get('innings', [])
            for inning in innings_data:
                team_name = inning.get('team', '')
                pd.DataFrame([{'match_id': match_id, 'team_name': team_name}]).to_sql(f'innings_{event_name}_{match_type}', conn, if_exists='append', index=False)
                inning_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]

                # Extract deliveries information
                deliveries_data = inning.get('overs', [])
                pd.DataFrame(deliveries_data).assign(inning_id=inning_id).to_sql('deliveries', conn, if_exists='append', index=False)

                # Extract player information and populate the 'players' and 'match_players' tables
                players_data = set()
                for delivery in deliveries_data:
                    players_data.add(delivery['batter'])
                    players_data.add(delivery['bowler'])
                    players_data.add(delivery['non_striker'])

                for player_name in players_data:
                    # Add players to the 'players' table if they don't exist
                    conn.execute('INSERT OR IGNORE INTO players (player_name) VALUES (?)', (player_name,))

                    # Add players to the 'match_players' table
                    player_id = conn.execute('SELECT player_id FROM players WHERE player_name = ?', (player_name,)).fetchone()[0]
                    conn.execute('INSERT INTO match_players (match_id, player_id, team_name) VALUES (?, ?, ?)', (match_id, player_id, team_name))

    conn.close()

if __name__ == "__main__":
    cricsheet_url = "" #we can pass url here
    data_folder = "data"
    os.makedirs(data_folder, exist_ok=True)
    db_path = "cricket_data.db"

    download_data(cricsheet_url, os.path.join(data_folder, "data.zip"))

    ingest_data(data_folder, db_path)
