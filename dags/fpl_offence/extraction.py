import logging

import pandas as pd
import requests

logging.basicConfig(format="%(asctime)s %(message)s")


def fetch_data():
    """
    function for epl team data
    """
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    try:
        logging.info("Fetching data from Fantasy Premier League API...")
        response = requests.get(url)
        # Raise an HTTPError for bad responses
        response.raise_for_status()
        res = response.json()
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

    return res


def fetch_teams():
    """
    Parse team names from the response of fetch_data().
    Returns a DataFrame with team information.
    """
    try:
        logging.info("Fetching team names")
        data = fetch_data()
        r_team = data['teams']
        data = pd.json_normalize(r_team)
        team = data[['id', 'name', 'short_name']]
        team.to_csv('epl_team.csv', index=False)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

    return team


# fetch_teams()


def fetch_players():
    """
    Fetch player data for a team with id=14 by parsing the fetch_data().
    Returns a DataFrame containing player information for the specified team.
    """
    try:
        logging.info("fetching players data....")
        data = fetch_data()
        r_players = data['elements']
        data = pd.json_normalize(r_players)
        player = data[['id', 'first_name', 'second_name', 'team',
                       'element_type']].copy()
        player.rename(columns={'element_type': 'position_id',
                               'team': 'team_id'}, inplace=True)
        filter_player = player[player['team_id'] == 14]
        filter_player.to_csv('manu_players.csv', index=False)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

    return filter_player


# fetch_players()


def fetch_position():
    """
    Function for player position parsing using fetch_data.
    """
    try:
        data = fetch_data()
        logging.info("fetching players data....")
        position = data['element_types']
        data = pd.json_normalize(position)
        position_data = data[['id', 'singular_name']].copy()
        position_data.rename(columns={'singular_name': 'position'},
                             inplace=True)
        position_data.to_csv('players_position.csv', index=False)
    except Exception as e:
        logging.error(f"An error occurred in fetch_position: {e}")

    return position_data


# fetch_position()


def fetch_fixtures():
    """
    epl season fixtures function
    """
    url = 'https://fantasy.premierleague.com/api/fixtures/'
    try:
        logging.info("Fetching fixtures data from Fantasy Premier League API.")
        response = requests.get(url)
        res = response.json()
        r = pd.json_normalize(res)
        convert_time = pd.to_datetime(r['kickoff_time'])
        r['match_date_time'] = convert_time
        max_year = convert_time.max().year
        min_year = convert_time.min().year
        season = str(min_year) + '/' + str(max_year)
        r['season'] = season
        r['event'] = r['event'].fillna(0).astype(int)
        r['team_a_score'] = r['team_a_score'].fillna(0).astype(int)
        r['team_h_score'] = r['team_h_score'].fillna(0).astype(int)
        res = r[['code', 'event', 'finished', 'id', 'match_date_time',
                 'team_a', 'team_a_score', 'team_h', 'team_h_score', 'season']]
        filter_manu = res[(res['team_a'] == 14) | (res['team_h'] == 14)]
        logging.info("Fetching fixtures data successful")
        # filter_manu.to_csv('fixtures_manu.csv', index=False)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    return filter_manu


# fetch_fixtures()


def fetch_game_week():
    """
    Fetch game weeks as a list from the fetched fixtures.
    """
    try:
        game_fixtures = fetch_fixtures()

        game_week = game_fixtures['event']
        # .astype(int)

        all_game_week = game_week.unique().tolist()
    except Exception as e:
        logging.error(f"An unexpected error occurred in fetch_game_week: {e}")

    return all_game_week


def fetch_player_stats():
    """
    Fetch player stats for each game_week and save to a CSV file.
    """
    game_played = fetch_fixtures()
    event_week = fetch_game_week()
    extracted_data = []

    for i in event_week:
        url = f"https://fantasy.premierleague.com/api/event/{i}/live"
        is_finished = game_played[game_played['finished'] == 1]

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
            except requests.RequestException as e:
                print(f"Error fetching data for gameweek {i}: {e}")
            except ValueError as e:
                print(f"Error parsing JSON for gameweek {i}: {e}")
    extracted_data = pd.DataFrame(extracted_data)
    extracted_data.to_csv('full_epl_player_stats.csv', index=False)
    return extracted_data


fetch_player_stats()


def manu_player():
    """
    Fetch Manchester United players' stats for each game week.
    """
    try:
        players = fetch_player_stats()
        filter_player = fetch_players()
        spec_player = filter_player['id'].astype(int)

        manu_players = spec_player.unique().tolist()

        all_stats = players[players['id'].isin(manu_players)]
        all_stats.to_csv('manu_player_stat.csv', index=False)
    except Exception as e:
        logging.error(f"An unexpected error occurred in manu_player: {e}")

    return all_stats


# manu_player()
