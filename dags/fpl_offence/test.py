import json
import pandas as pd
import logging
import requests

fixtures_output_file = "fixtures.json"
url_fixtures = "https://fantasy.premierleague.com/api/fixtures/"

# with open(output_file, 'r') as file:
#     data = json.load(file)
#     r_team = data[0]['teams']
#     data = pd.json_normalize(r_team)
#     team = data[['id', 'name', 'short_name']]
#     team.to_csv('dags/fpl_offence/data/epl_team.csv', index=False)


def download_fixtures():
    """
    epl season fixtures function
    """
    try:
        logging.info("Fetching fixtures data from Fantasy Premier League API.")
        response = requests.get(url_fixtures, stream=True)
        with open(fixtures_output_file, "w") as f:
            data_list = []  # List to store JSON objects

            for line in response.iter_lines():
                if line:
                    decoded_line = json.loads(line.decode('utf-8'))
                    data_list.append(decoded_line)
        # Write the collected JSON data to the file
            json.dump(data_list, f, indent=4)
        logging.info("downloading fixtures data successful")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def fetch_fixtures():
    """
    epl season fixtures function
    """
    try:
        logging.info("Fetching fixtures data fixtures.json.")
        with open(fixtures_output_file, 'r') as file:
            data = json.load(file)
            dat = data[0]
            r_norm = pd.json_normalize(dat)
            # converting kickoff_time to datetime
            convert_time = pd.to_datetime(r_norm['kickoff_time'])
            r_norm['match_date_time'] = convert_time
            max_year = convert_time.max().year
            min_year = convert_time.min().year
            season = str(min_year) + '/' + str(max_year)
            r_norm['season'] = season
            r_norm['event'] = r_norm['event'].fillna(0).astype(int)
            r_norm['team_a_score'] = r_norm['team_a_score'].fillna(0).astype(int)
            r_norm['team_h_score'] = r_norm['team_h_score'].fillna(0).astype(int)
            res = r_norm[['code', 'event', 'finished', 'id', 'match_date_time',
                          'team_a', 'team_a_score', 'team_h', 'team_h_score',
                          'season']]
            # filtering for manchester united team id==14
            filter_manu = res[(res['team_a'] == 14) | (res['team_h'] == 14)]
            filter_manu.to_csv('fixtures.csv', index=False)
        logging.info("Fetching fixtures data successful")
        return filter_manu
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


download_fixtures()
fetch_fixtures()
