import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Database connection
def get_player_alerts():
    """
    Retrieve all player alerts from the "alert" table in PostgreSQL.

    Returns:
        list: A list of records from the "alert" table.
    """
    hook = PostgresHook(postgres_conn_id='postgres_id')
    query = """
        select *
        from alert
    """
    results = hook.get_records(query)
    return results


def send_email(player_name, alert_type, gameweek, match_time, reason):
    """
    Send an email alert regarding a player.

    Args:
        player_name (str): The name of the player.
        alert_type (str): The type of alert (warning or suspension).
        gameweek (int): The gameweek associated with the alert.
        match_time (str): The scheduled match time.
        reason (str): The reason for the alert.

    Sends an email using SMTP with Gmail server settings.
    """
    sender_email = Variable.get('sender_email')
    receiver_email = Variable.get('receiver_email')
    password = Variable.get('password')
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = f"Player Alert: {player_name} - {alert_type}"

    body = f"""
    Dear Team Manager,

    Player: {player_name}
    Alert: {alert_type}
    Gameweek: {gameweek}
    Match Time: {match_time}
    Reason: {reason}

    Please take appropriate action.

    Regards,
    Your Team
    """
    message.attach(MIMEText(body, "plain"))
    try:
        server = smtplib.SMTP("smtp.gmail.com", 587)
        server.starttls()
        server.login(sender_email, password)
        text = message.as_string()
        server.sendmail(sender_email, receiver_email, text)
        print(f"Email sent to {receiver_email} for {player_name}!")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        server.quit()


def process_and_notify():
    """
    Process player alerts and send notifications via email.

    Retrieves player alerts from the database, extracts relevant details,
    and sends an email for each alert.
    """
    players = get_player_alerts()

    for player in players:
        player_name = f"{player[1]} {player[2]}"
        alert_type = player[5]
        gameweek = player[3]
        match_time = player[4]
        reason = player[6]

        send_email(player_name, alert_type, gameweek, match_time, reason)
