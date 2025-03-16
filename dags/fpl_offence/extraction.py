import json
import logging

import pandas as pd
import requests

logging.basicConfig(format="%(asctime)s %(message)s")

url_bootstrap = "https://fantasy.premierleague.com/api/bootstrap-static/"
url_fixtures = "https://fantasy.premierleague.com/api/fixtures/"
bootstrap_output_file = "dags/fpl_offence/stream/bootstrap.json"
fixtures_output_file = "dags/fpl_offence/stream/fixtures.json"


def fetch_data():
    """
    function for epl team data
    """
    try:
        logging.info("Fetching data from Fantasy Premier League API...")
        response = requests.get(url_bootstrap, stream=True)
        with open(bootstrap_output_file, "w") as f:
            data_list = []  # List to store JSON objects

            for line in response.iter_lines():
                if line:
                    decoded_line = json.loads(line.decode('utf-8'))
                    data_list.append(decoded_line)

        # Write the collected JSON data to the file
            json.dump(data_list, f, indent=4)

        logging.info(f"Streamed data saved to {bootstrap_output_file}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def fetch_teams():
    """
    Parse team names from the response of fetch_data().
    Returns a DataFrame with team information.
    """
    try:
        logging.info("Fetching team names")
        with open(bootstrap_output_file, 'r') as file:
            data = json.load(file)
            r_team = data[0]['teams']
            data = pd.json_normalize(r_team)
            team = data[['id', 'name', 'short_name']]
            team.to_csv('dags/fpl_offence/data/epl_team.csv', index=False)
            logging.info("Fetching team names completed")
        return team
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def fetch_players():
    """
    Fetch player data for a team with id=14 by parsing the fetch_data().
    Returns a DataFrame containing player information for the specified team.
    """
    try:
        logging.info("fetching players data....")
        with open(bootstrap_output_file, 'r') as file:
            data = json.load(file)
            r_players = data[0]['elements']
            data = pd.json_normalize(r_players)
            player = data[['id', 'first_name', 'second_name', 'team',
                           'element_type']].copy()
            player.rename(columns={'element_type': 'position_id',
                                   'team': 'team_id'}, inplace=True)
            # filtering for manchester united players team id==14
            filter_player = player[player['team_id'] == 14]
            filter_player.to_csv('dags/fpl_offence/data/team_players.csv',
                                 index=False)
            logging.info("fetching players data.... completed")
        return filter_player
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def fetch_position():
    """
    Function for player position parsing using fetch_data.
    """
    try:
        logging.info("fetching players position....")
        with open(bootstrap_output_file, 'r') as file:
            data = json.load(file)
            position = data[0]['element_types']
            data = pd.json_normalize(position)
            position_data = data[['id', 'singular_name']].copy()
            position_data.rename(columns={'singular_name': 'position'},
                                 inplace=True)
            position_data.to_csv('dags/fpl_offence/data/position.csv',
                                 index=False)
            logging.info("fetching players position....completed")
            return position_data
    except Exception as e:
        logging.error(f"An error occurred in fetch_position: {e}")


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
        logging.info(f"Streamed data saved to {fixtures_output_file}")
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
            columns_to_fill = ['event', 'team_a_score', 'team_h_score']
            for col in columns_to_fill:
                r_norm[col] = r_norm[col].fillna(0).astype(int)
            res = r_norm[['code', 'event', 'finished', 'id', 'match_date_time',
                          'team_a', 'team_a_score', 'team_h', 'team_h_score',
                          'season']]
            # filtering for manchester united team id==14
            filter_manu = res[(res['team_a'] == 14) | (res['team_h'] == 14)]
            filter_manu.to_csv('dags/fpl_offence/data/fixtures.csv',
                               index=False)
        logging.info("Fetching fixtures data successful")
        return filter_manu
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


def fetch_game_week():
    """
    Fetch game weeks as a list from the fetched fixtures.
    """
    try:
        logging.info("Fetching game week as a list.....")
        df = pd.read_csv('dags/fpl_offence/data/fixtures.csv')
        game_week = df['event']
        all_game_week = game_week.unique().tolist()
        logging.info("Fetching game week as a list...successful")
        return all_game_week
    except Exception as e:
        logging.error(f"An unexpected error occurred in fetch_game_week: {e}")


def fetch_player_stats():
    """
    Fetch player stats for each game_week and save to a CSV file.
    """
    logging.info("Fetching players stats for each game week.....")
    df = pd.read_csv('dags/fpl_offence/data/fixtures.csv')
    event_week = fetch_game_week()
    extracted_data = []

    for i in event_week:
        url = f"https://fantasy.premierleague.com/api/event/{i}/live"
        is_finished = df[df['finished'] == 1]

        if not is_finished.empty:
            try:
                response = requests.get(url)
                # Raise an HTTPError for bad responses
                response.raise_for_status()
                res = response.json()
                for element in res.get("elements", []):
                    id_ = element.get("id")
                    stats = element.get("stats", {})
                    minutes = stats.get("minutes", 0)
                    yellow_cards = stats.get("yellow_cards", 0)
                    red_cards = stats.get("red_cards", 0)
                    extracted_data.append({
                        "id": id_,
                        "minutes": minutes,
                        "yellow_cards": yellow_cards,
                        "red_cards": red_cards,
                        "gameweek": int(i)
                    })
            except Exception as e:
                logging(f"Error fetching data for gameweek {i}: {e}")
    extracted_data = pd.DataFrame(extracted_data)
    extracted_data.to_csv('dags/fpl_offence/data/players_stats.csv',
                          index=False)
    logging.info("Fetching players stats for each game week...successful")
    return extracted_data


def manu_player():
    """
    Fetch Manchester United players' stats for each game week.
    """
    try:
        logging.info("selecting Manchester United players' stats...")
        players = pd.read_csv('dags/fpl_offence/data/players_stats.csv')
        filter_player = pd.read_csv('dags/fpl_offence/data/team_players.csv')
        spec_player = filter_player['id'].astype(int)
        manu_players = spec_player.unique().tolist()
        # filtering for only Manchester United players
        all_stats = players[players['id'].isin(manu_players)]
        all_stats.to_csv('dags/fpl_offence/data/manunited_players_stats.csv',
                         index=False)
        logging.info("selecting Manchester United players' stats...successful")
        return all_stats
    except Exception as e:
        logging.error(f"An unexpected error occurred in manu_player: {e}")
