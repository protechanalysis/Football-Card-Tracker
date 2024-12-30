import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from airflow.providers.postgres.hooks.postgres import PostgresHook


# Database connection
def get_player_alerts():
    # Initialize PostgresHook
    hook = PostgresHook(postgres_conn_id='postgres_id')

    # Define your query
    query = """
        select *
        from alert
    """

    # Execute the query and fetch results
    results = hook.get_records(query)

    return results


# Function to send email
def send_email(player_name, alert_type, gameweek, match_time, reason):
    # Email server setup (example with Gmail)
    sender_email = "adewunmioluwaseyi98@gmail.com"
    receiver_email = "olaniyiadewunmi30@gmail.com"
    password = "rujiijdqujtohklr"

    # Create message
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = f"Player Alert: {player_name} - {alert_type}"

    # Email body
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

    # Send email
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


# Main function to process the players and send alerts
def process_and_notify():
    players = get_player_alerts()

    for player in players:
        player_name = f"{player[1]} {player[2]}"  # First and Last Name
        alert_type = player[5]  # 'warning' or 'miss_next_match'
        gameweek = player[3]  # Gameweek
        match_time = player[4]  # Match date/time
        reason = player[6]

        # Send the email
        send_email(player_name, alert_type, gameweek, match_time, reason)


# Run the notification system
# process_and_notify()
