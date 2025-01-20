## Card Booking Tracker
### Project Overview
#### Offensive Card Booking Tracker for Football Club

### Project Summary
This project aims to track and manage the accumulation of yellow and red cards by players and officials during a competition. Leveraging the FPL API, the system collects data and stores it in a PostgreSQL database. Airflow orchestrates the data pipeline, Docker ensures containerized deployment, and Metabase provides visualization.

#### Project Objectives
- Extract card data from the FPL API and load it into a PostgreSQL database.
- Automate the pipeline with Airflow for periodic updates.
- Use Metabase for visualizing card metrics.
- Deploy the solution in a containerized environment with Docker.

### Data Architecture

![workflow](/asset/epl_architecture.png)

### Data Workflow
#### Data Sources
- API: FPL API endpoint providing data for players, fixtures, players match stats, teams.
#### ETL Process
- Extraction:
    Use Python’s requests library to fetch data from the FPL API.
- Transformation:
    Clean and normalize the data to align with the PostgreSQL schema.
- Loading:
    Insert transformed data into PostgreSQL.
#### Airflow Orchestration Dag
![dag](/asset/dag_flow_tracker.png)

#### Notification Features
- Warning Alert:
    Triggered when a player accumulates 4 yellow cards.
- Suspension Alert:
    Triggered when a player accumulates 5 yellow cards or when player receives a red card in the most recent match.
- Reset Notification:
    Triggered after the player has served the suspension by missing a match.
![notify1](/asset/offence_notification.png)
![notify2](/asset/offence_notification2.png)

#### Visualization
![viz](/asset/tracker_viz.png)
![viz2](/asset/tracker_viz2.png)
