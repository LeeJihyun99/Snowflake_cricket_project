# Cricket Matches Data Pipeline and Dashboard
## Overview
This project implements a data pipeline to process semi-structured JSON files containing cricket match data. The pipeline is designed using Snowflake, with automated data ingestion, transformation, and visualization via a customized dashboard. This project was initially developed by following a [video tutorial](https://www.youtube.com/watch?v=qDmqE89DSQQ&t=205s). I then customized and extended the project, particularly in the dashboard and automation sections.


## Getting Started
#### Snowflake Setup (Free Trial)
If you're new to Snowflake, you can start with a free trial account. Here's a quick setup guide:

#### Create a Snowflake Account:
Go to Snowflake [Sign Up](https://signup.snowflake.com/) and register for a free trial.
After signing up, you'll receive a Username and a Dedicated Login URL.
```
Username: <yourusername>
Login URL: https://<youraccountname>.snowflakecomputing.com
```

### Install SnowSQL (Command-Line Interface):
Download and install SnowSQL from the [Snowflake Downloads](https://www.snowflake.com/en/developers/downloads/snowsql/).
Connect to Snowflake using the following command:
```
snowsql -a <youraccountname> -u <yourusername>
```
Enter your password when prompted.

### Verify Connection:
If the connection is successful, you will see a Snowflake prompt where you can run SQL commands.
To exit, type:
``` Ctrl+D ```
## Technologies Used
#### Snowflake: For data storage, transformation, and flattening nested JSON data.
#### JSON: Semi-structured data format containing cricket match details.
#### Dashboarding Tool: Snowflake's native interface.
#### Automation: Snowflake tasks and streams for handling new data insertions in real-time.
#### [JSON Viewer](https://jsonviewer.tools/editor): to visualize JSON file structure.
#### [DBeaver](https://sosbornlaw.com/?gad_source=1&gclid=CjwKCAiA9bq6BhAKEiwAH6bqoOHbmOl3YezBtBSD7unzmfsE4lcTAyAdUaYvFp3eo0p7BBC1HAC5yRoCPqAQAvD_BwE): To generate diagrams for fact, dimensional tables.

## Architecture
Below is the overall architecture of the project:
![image]
#### The pipeline consists of the following layers:
- Landing Layer: Initial data ingestion and staging.
- Raw Layer: Loading raw data into Snowflake tables.
- Clean Layer: Flattening and curating data.
- Consumption Layer: Creating fact and dimension tables for analytics

## Landing Layer
This section explains how to set up and manage the Landing Layer in Snowflake, including database creation, file format, staging, and loading JSON files into Snowflake tables.
1. Set Role, Warehouse, and Create Database and Schemas
In the Snowflake Web UI:
- Set the Role and Warehouse to execute queries with proper permissions:
```sql
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
```
- Create a Database and Schema:
```sql
CREATE DATABASE IF NOT EXISTS CRICKET;
CREATE SCHEMA IF NOT EXISTS CRICKET.LAND;
```

2. Create File Format
A File Format in Snowflake defines how to interpret the structure of a file during data import/export.

- JSON File Format for this project:
```sql
CREATE OR REPLACE FILE FORMAT cricket.land.my_json_format
    TYPE = JSON
    NULL_IF = ('\\n', 'null', '')  -- Values to interpret as NULL
    STRIP_OUTER_ARRAY = TRUE       -- (Optional) Remove outer array brackets (Not needed for this project since files don't have outer brackets.)
    COMMENT = 'JSON File Format with outer strip array flag true';
```

3. Create Internal Stage
A Stage is a temporary storage location for files before loading them into Snowflake tables.

- Internal Stage Creation:
```sql
CREATE OR REPLACE STAGE cricket.land.my_stg;
```

#### Types of Stages:
- Internal Stage:

Managed by Snowflake’s internal storage. No external cloud storage account is required.

Example: JSON file 1384401.json uploaded to the my_stg stage is stored within Snowflake.

- External Stage:
  
Data is stored outside Snowflake (e.g., Amazon S3, Azure Blob Storage) and read directly from the external storage.

#### Why Stages Are Essential:
- Improves Performance: Enables parallel data loading.
- Flexibility: Reuse, inspect, and validate files before loading.
- Supports External Data: Access data from external storage systems.
- Decoupled Architecture: Separate compute and storage operations for efficient data handling.

#### You can also load JSON files using CLI.

Connect to SnowSQL:
```
snowsql -a <youraccountname> -u <your username>
```

Set Database, Schema, Warehouse, and Role:
```
USE DATABASE CRICKET;
USE SCHEMA LAND;
USE WAREHOUSE COMPUTE_WH;
USE ROLE ACCOUNTADMIN;
```

Upload JSON Files to the Stage:
```
PUT <your local path to the folder with your json files>/*.json @my_stg/cricket/json/;
```

## Raw Layer
The Raw Layer is where the original JSON data is ingested and stored as-is. This layer does not perform any transformations; it simply stores the raw data, often in nested formats (like arrays, objects, etc.). The Raw Layer is crucial because it serves as the foundation for all subsequent transformations, ensuring that the original data is preserved and can be revisited or corrected as necessary.

Before you proceed with creating raw tables and ingesting data, create a schema named raw in your Snowflake database.
```sql
CREATE SCHEMA IF NOT EXISTS raw;
```
### Create Raw Table
Create the Raw Table that will store the JSON data from the files you upload. This table includes columns to store the various fields (e.g., meta, info, innings) and metadata like the file name, row number, and hash key for auditing.

```sql
CREATE OR REPLACE TRANSIENT TABLE raw.match_raw_tbl (
    meta OBJECT NOT NULL,
    info VARIANT NOT NULL,
    innings ARRAY NOT NULL,
    stg_file_name TEXT NOT NULL,
    stg_file_row_number INT NOT NULL,
    stg_file_hashkey TEXT NOT NULL,
    stg_modified_ts TIMESTAMP NOT NULL
);
```
### Load Data into Raw Table
You can now load the raw JSON files into the raw.match_raw_tbl table. The COPY INTO command will load the data while preserving the structure.
```sql
COPY INTO raw.match_raw_tbl
FROM (
    SELECT
        t.$1:meta::OBJECT AS meta,
        t.$1:info::VARIANT AS info,
        t.$1:innings::ARRAY AS innings,
        metadata$filename AS stg_file_name,
        metadata$file_row_number AS stg_file_row_number,
        metadata$file_content_key AS stg_file_hashkey,
        metadata$file_last_modified AS stg_modified_ts
    FROM @cricket.land.my_stg/cricket/json (FILE_FORMAT => 'cricket.land.my_json_format') t
)
ON_ERROR = CONTINUE;
```
The ON_ERROR = CONTINUE option ensures that the load process continues even if some rows fail, allowing valid rows to be loaded while problematic rows are skipped.
Inspect failed rows in SnowSight for debugging and re-upload fixed files.

## Clean Layer
The Clean Layer is where nested data is transformed into a more structured format. This process involves flattening the data (e.g., arrays and objects) to make it easier for analysis.

### Create Player Info Table
Flatten the nested data in the info:players field to extract player information for each match.
```sql
CREATE OR REPLACE TRANSIENT TABLE cricket.clean.player_clean_tbl AS (
    SELECT 
        raw.info:match_type_number::INT AS match_type_number,
        p.key::TEXT AS country,
        team.value::TEXT AS player_name,
        raw.stg_file_name,
        raw.stg_file_row_number,
        raw.stg_file_hashkey,
        raw.stg_modified_ts
    FROM raw.match_raw_tbl raw,
    LATERAL FLATTEN(INPUT => raw.info:players) p,
    LATERAL FLATTEN(INPUT => p.value) team
);
```

#### Why Use LATERAL?
Flatten works on a single row, while LATERAL is used to apply the flattening to each row in the dataset.

### Enforce NOT NULL Constraints
Ensure critical columns do not contain null values for fields that are essential to the analysis.
```sql
ALTER TABLE cricket.clean.player_clean_tbl
MODIFY COLUMN match_type_number SET NOT NULL;
ALTER TABLE cricket.clean.player_clean_tbl
MODIFY COLUMN country SET NOT NULL;
ALTER TABLE cricket.clean.player_clean_tbl
MODIFY COLUMN player_name SET NOT NULL;
```
### Create Match Info Table
```sql
create or replace transient table cricket.clean.match_detail_clean as(
select 
info:match_type_number::int as match_type_number,
info:event.name::text as event_name,
case
when info:match_number::text is not null then info:match_number
when info:match_stage::text is not null then info:match_stage
else 'NA' end as match_stage,
info:dates[0]::date as event_date,
date_part('year', info:dates[0]::date) as event_year,
date_part('month', info:dates[0]::date) as event_month,
date_part('day', info:dates[0]::date) as event_day,
info:match_type::text as match_type,
info:season::text as season,
info:team_type::text as team_type,
info:overs::text as overs,
info:city::text as city,
info:venue::text as venue,
info:gender::text as gender,
info:teams[0]::text as first_team,
info:teams[1]::text as second_team,
case
when info:outcome.winner is not null then 'Result Declared'
when info:outcome.result = 'tie' then 'Tie'
when info:outcome.reult = 'no result' then 'No Result'
else info:outcome.result end as match_result,
case 
when info:outcome.winner is not null then info:outcome.winner else 'NA' end as winner,
info:toss.winner::text as toss_winner,
initcap(info:toss.decision::text) as toss_decision,
-- 
stg_file_name,
stg_file_row_number,
stg_file_hashkey,
stg_modified_ts
from cricket.raw.match_raw_tbl
);
```
### Create Delivery Info Table
Flatten the complex nested JSON data to extract detailed delivery-level information for the match.
```sql
CREATE OR REPLACE TRANSIENT TABLE cricket.clean.delivery_clean_tbl AS
SELECT 
    tbl.info:match_type_number::INT AS match_type_number,
    i.value:team::TEXT AS team_name,
    o.value:over::INT + 1 AS over,
    d.value:bowler::TEXT AS bowler,
    d.value:batter::TEXT AS batter,
    d.value:non_striker::TEXT AS non_striker,
    d.value:runs.batter::TEXT AS runs,
    d.value:runs.extras::TEXT AS extras,
    d.value:runs.total::TEXT AS total,
    e.key::TEXT AS extra_type,
    e.value::INT AS extra_runs,
    w.value:player_out::TEXT AS player_out,
    w.value:kind::TEXT AS player_out_kind,
    w.value:fielders::VARIANT AS player_out_fielders,
    f.value:name::TEXT AS fielder_name,
    f.value:substitute::TEXT AS substitute,
    tbl.stg_file_name,
    tbl.stg_file_row_number,
    tbl.stg_file_hashkey,
    tbl.stg_modified_ts
FROM raw.match_raw_tbl tbl,
LATERAL FLATTEN(INPUT => tbl.innings) i,
LATERAL FLATTEN(INPUT => i.value:overs) o,
LATERAL FLATTEN(INPUT => o.value:deliveries) d,
LATERAL FLATTEN(INPUT => d.value:extras, OUTER => TRUE) e,
LATERAL FLATTEN(INPUT => d.value:wickets, OUTER => TRUE) w,
LATERAL FLATTEN(INPUT => w.value:fielders, OUTER => TRUE) f;
```

## Consumption Layer
The Consumption Layer is where the cleaned data are structured into fact and dimension tables. These tables are crucial for analytical purposes, enabling efficient querying and reporting.

Fact and dimension tables are designed and architected based on the data and structure provided in the video tutorial. These tables are then used for further analysis and reporting.
You can refer to the diagram to better understand the relationships between the tables.

[Insert the schema diagram here (DBeaver-generated diagram)]

#### Fact and Dimension Tables Overview
Using the DBeaver tool, I uploaded the fact and dimension tables, which automatically generated a diagram to illustrate the relationships between them. Here's a brief overview of the tables created:

1. Fact and Dimension Tables Creation
### Dimension date table
```sql
create or replace table date_dim (
    date_id int primary key autoincrement,
    full_dt date,
    day int,
    month int,
    year int,
    quarter int,
    dayofweek int,
    dayofmonth int,
    dayofyear int,
    dayofweekname varchar(3),
    isweekend boolean
);
```
### Dimension Referee, Team, Player tables (Referee table is not used in this project)
```sql
create or replace table referee_dim(
    referee_id int primary key autoincrement,
    referee_name text not null,
    referee_type text not null
);

create or replace table team_dim(
    team_id int primary key autoincrement,
    team_name text not null
);

create or replace table player_dim (
    player_id int primary key autoincrement,
    team_id int not null,
    player_name text not null
);

```
#### add foreign keys
alter table cricket.consumption.player_dim
add constraint fk_team_player_id
foreign key (team_id)
references cricket.consumption.team_dim (team_id);

### Dimension Venue table
```sql
create or replace table venue_dim (
    venue_id int primary key autoincrement,
    venue_name text not null,
    city text not null,
    state text,
    country text,
    continent text,
    end_Names text,
    capacity number,
    pitch text,
    flood_light boolean,
    established_at date,
    playing_area text,
    other_sports text,
    curator text,
    lattitute number(10,6),
    longitude number(10,6)
);
```
### Dimension match type table
```sql
create or replace table match_type_dim (
    match_type_id int primary key autoincrement,
    match_type text not null
);
```

### Fact match table
```sql
create or replace table match_fact (
    match_id int primary key autoincrement,
    date_id int not null,
    referee_id int not null,
    team_a_id int not null,
    team_b_id int not null,
    match_type_id int not null,
    venue_id int not null,
    total_overs int,
    balls_per_over int,

    overs_played_by_team_a int,
    bowls_played_by_team_a int,
    extra_bowls_played_by_team_a int,
    extra_runs_scored_by_team_a int,
    fours_by_team_a int,
    sixes_by_team_a int,
    total_score_by_team_a int,
    wicket_lost_by_team_a int,

    overs_played_by_team_b int,
    bowls_played_by_team_b int,
    extra_bowls_played_by_team_b int,
    extra_runs_scored_by_team_b int,
    fours_by_team_b int,
    sixes_by_team_b int,
    total_score_by_team_b int,
    wicket_lost_by_team_b int,

    toss_winner_team_id int not null,
    toss_decision text not null,
    match_result text not null,
    winner_team_id int not null,

    constraint fk_date foreign key (date_id) references date_dim (date_id),
    constraint fk_referee foreign key (referee_id) references referee_dim (referee_id),
    constraint fk_team1 foreign key (team_a_id) references team_dim (team_id),
    constraint fk_team2 foreign key (team_b_id) references team_dim (team_id),
    constraint fk_match_type foreign key (match_type_id) references match_type_dim (match_type_id),
    constraint fk_toss_winner_team foreign key (toss_winner_team_id) references team_dim (team_id),
    constraint fk_winner_team foreign key (winner_team_id) references team_dim (team_id),
    constraint fk_venue foreign key (venue_id) references venue_dim (venue_id)
);
```
### Fact Delivery table
```sql
CREATE or replace TABLE delivery_fact (
    match_id INT ,
    team_id INT,
    bowler_id INT,
    batter_id INT,
    non_striker_id INT,
    over INT,
    runs INT,
    extra_runs INT,
    extra_type VARCHAR(255),
    player_out VARCHAR(255),
    player_out_kind VARCHAR(255),

    CONSTRAINT fk_del_match_id FOREIGN KEY (match_id) REFERENCES match_fact (match_id),
    CONSTRAINT fk_del_team FOREIGN KEY (team_id) REFERENCES team_dim (team_id),
    CONSTRAINT fk_bowler FOREIGN KEY (bowler_id) REFERENCES player_dim (player_id),
    CONSTRAINT fk_batter FOREIGN KEY (batter_id) REFERENCES player_dim (player_id),
    CONSTRAINT fk_stricker FOREIGN KEY (non_striker_id) REFERENCES player_dim (player_id)
);
```
3. Insert Data into Fact and Dimension Tables
```sql
insert into cricket.consumption.team_dim (team_name)
select distinct team
from (
select first_team as team
from cricket.clean.match_detail_clean
union all
select second_team as team
from cricket.clean.match_detail_clean
)order by team;

insert into cricket.consumption.player_dim (team_id, player_name)
select distinct t.team_id, p.player_name
from cricket.clean.player_clean_tbl p
join cricket.consumption.team_dim t
on t.team_name = p.country;

insert into cricket.consumption.venue_dim (venue_name, city)
select venue,
case when city is null then 'NA' else city end as city
from cricket.clean.match_detail_clean 
group by venue, city;

insert into match_type_dim (match_type)
select match_type from cricket.clean.match_detail_clean group by match_type;

insert into cricket.consumption.date_dim (date_id, full_dt, day, month, year, quarter, dayofweek, dayofmonth, dayofyear, dayofweekname,isweekend)
SELECT
    ROW_NUMBER() OVER (ORDER BY event_date) AS DateID,
    event_date AS FullDate,
    EXTRACT(DAY FROM event_date)::int AS Day,
    EXTRACT(MONTH FROM event_date) AS Month,
    EXTRACT(YEAR FROM event_date)::int AS Year,
    CASE WHEN EXTRACT(QUARTER FROM event_date) IN (1, 2, 3, 4) THEN EXTRACT(QUARTER FROM event_date) END AS Quarter,
    DAYOFWEEKISO(event_date) AS DayOfWeek,
    EXTRACT(DAY FROM event_date) AS DayOfMonth,
    DAYOFYEAR(event_date) AS DayOfYear,
    DAYNAME(event_date) AS DayOfWeekName,
    CASE When DAYNAME(event_date) IN ('Sat', 'Sun') THEN TRUE ELSE FALSE END AS IsWeekend
FROM cricket.clean.match_detail_clean group by event_date;


insert into cricket.consumption.match_fact 
select 
    m.match_type_number as match_id,
    dd.date_id as date_id,
    0 as referee_id,
    ftd.team_id as first_team_id,
    std.team_id as second_team_id,
    mtd.match_type_id as match_type_id,
    vd.venue_id as venue_id,
    50 as total_overs,
    6 as balls_per_overs,
    max(case when d.team_name = m.first_team then  d.over else 0 end ) as OVERS_PLAYED_BY_TEAM_A,
    sum(case when d.team_name = m.first_team then  1 else 0 end ) as balls_PLAYED_BY_TEAM_A,
    sum(case when d.team_name = m.first_team then  d.extras else 0 end ) as extra_balls_PLAYED_BY_TEAM_A,
    sum(case when d.team_name = m.first_team then  d.extra_runs else 0 end ) as extra_runs_scored_BY_TEAM_A,
    0 fours_by_team_a,
    0 sixes_by_team_a,
    (sum(case when d.team_name = m.first_team then  d.runs else 0 end ) + sum(case when d.team_name = m.first_team then  d.extra_runs else 0 end ) ) as total_runs_scored_BY_TEAM_A,
    sum(case when d.team_name = m.first_team and player_out is not null then  1 else 0 end ) as wicket_lost_by_team_a,    
    
    max(case when d.team_name = m.second_team then  d.over else 0 end ) as OVERS_PLAYED_BY_TEAM_B,
    sum(case when d.team_name = m.second_team then  1 else 0 end ) as balls_PLAYED_BY_TEAM_B,
    sum(case when d.team_name = m.second_team then  d.extras else 0 end ) as extra_balls_PLAYED_BY_TEAM_B,
    sum(case when d.team_name = m.second_team then  d.extra_runs else 0 end ) as extra_runs_scored_BY_TEAM_B,
    0 fours_by_team_b,
    0 sixes_by_team_b,
    (sum(case when d.team_name = m.second_team then  d.runs else 0 end ) + sum(case when d.team_name = m.second_team then  d.extra_runs else 0 end ) ) as total_runs_scored_BY_TEAM_B,
    sum(case when d.team_name = m.second_team and player_out is not null then  1 else 0 end ) as wicket_lost_by_team_b,
    tw.team_id as toss_winner_team_id,
    m.toss_decision as toss_decision,
    m.match_result as match_result,
    mw.team_id as winner_team_id
     
from 
    cricket.clean.match_detail_clean m
    join date_dim dd on m.event_date = dd.full_dt
    join team_dim ftd on m.first_team = ftd.team_name 
    join team_dim std on m.second_team = std.team_name 
    join match_type_dim mtd on m.match_type = mtd.match_type
    join venue_dim vd on m.venue = vd.venue_name and m.city = vd.city
    join cricket.clean.delivery_clean_tbl d  on d.match_type_number = m.match_type_number 
    join team_dim tw on m.toss_winner = tw.team_name 
    join team_dim mw on m.winner= mw.team_name 
    --where m.match_type_number = 4686
    group by
        m.match_type_number,
        date_id,
        referee_id,
        first_team_id,
        second_team_id,
        match_type_id,
        venue_id,
        total_overs,
        toss_winner_team_id,
        toss_decision,
        match_result,
        winner_team_id
        ;

insert into delivery_fact
select 
    d.match_type_number as match_id,
    td.team_id,
    bpd.player_id as bower_id, 
    spd.player_id batter_id, 
    nspd.player_id as non_stricker_id,
    d.over,
    d.runs,
    case when d.extra_runs is null then 0 else d.extra_runs end as extra_runs,
    case when d.extra_type is null then 'None' else d.extra_type end as extra_type,
    case when d.player_out is null then 'None' else d.player_out end as player_out,
    case when d.player_out_kind is null then 'None' else d.player_out_kind end as player_out_kind
from 
    cricket.clean.delivery_clean_tbl d
    join team_dim td on d.team_name = td.team_name
    join player_dim bpd on d.bowler = bpd.player_name
    join player_dim spd on d.batter = spd.player_name
    join player_dim nspd on d.non_striker = nspd.player_name;
```

## Dashboard with Snowsight
### Features of the Dashboard

- **Dynamic Filtering**: Allows you to filter the data based on specific criteria (in this case, a team selection).
- **Match Information**: Displays match details including match date, opposing team, and winner team.
- **Visualization**: After executing the query, Snowsight shows the results in both tabular format and graphical charts, offering various chart types.

### SQL Query for Match Information Dashboard

Below is the SQL query used to display match information for a selected team, including the match ID, the opponent team, the winner, and the match date. 

The query also integrates a filter to allow users to select a specific team.

```sql
WITH tmp_table AS (
    SELECT 
        m.match_id AS match_id,
        d.full_dt AS match_date,
        CASE 
            WHEN t.team_id = m.team_a_id THEN m.team_b_id
            WHEN t.team_id = m.team_b_id THEN m.team_a_id 
        END AS matched_with,
        m.winner_team_id AS winner_id
    FROM 
        match_fact m
    JOIN 
        team_dim t 
        ON m.team_a_id = t.team_id OR m.team_b_id = t.team_id
    JOIN 
        date_dim d 
        ON m.date_id = d.date_id
    WHERE 
       t.team_name = :filter_by_team
)
SELECT 
    tmp.match_id, 
    tmp.match_date, 
    matched_team.team_name AS matched_with, 
    winner_team.team_name AS winner_team
FROM 
    tmp_table tmp
LEFT JOIN 
    team_dim matched_team 
    ON tmp.matched_with = matched_team.team_id
LEFT JOIN 
    team_dim winner_team 
    ON tmp.winner_id = winner_team.team_id;
```

The filter :filter_by_team is used to dynamically change the displayed data based on the selected team.

I also added the total number of matches by the filtered team and also the number of matches the team won.
The dashboard looks like this:
[image]

## Automation of data pipeline 
I automated the data pipeline by using **Streams** and **Tasks** in Snowflake. This approach allows for efficient, scheduled data processing with dependency management to ensure tasks are executed in the correct order. (You can see the dependencies and the order of tasks run in the Graph section)

Additionally, all tasks are initially in a **suspended** state, and can be manually resumed to start execution. You need to resume all tasks. 

### Overview of Streams and Tasks

- **Streams**: In Snowflake, streams track changes (like inserts, updates, and deletes) in a table. They allow you to query only the new or modified data since the last stream was processed.
- **Tasks**: Tasks are Snowflake's way of automating SQL query execution at scheduled intervals. You can chain tasks with dependencies, ensuring that tasks are executed in the right order.

### Pipeline Scheduling and Task Management
Creating Streams for Raw Tables
In this project, three different streams are created on the raw table (match_raw_tbl) to track new data inserted into specific areas (match, player, and delivery). These streams will only capture new insertions in the table and are set to track changes with the append_only=True option.

```sql
CREATE OR REPLACE STREAM cricket.raw.for_match_stream 
    ON TABLE cricket.raw.match_raw_tbl 
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM cricket.raw.for_player_stream 
    ON TABLE cricket.raw.match_raw_tbl 
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM cricket.raw.for_delivery_stream 
    ON TABLE cricket.raw.match_raw_tbl 
    APPEND_ONLY = TRUE;
```

#### What Does APPEND_ONLY = TRUE Mean?
The APPEND_ONLY=True option ensures that the stream only tracks newly inserted rows and does not capture updates or deletions to existing rows. This is particularly useful when your goal is to process incremental inserts rather than tracking changes like updates or deletions.

- Insertions: When new rows are added to the table, the stream captures them, and you can then process these new records in your data pipeline.
- No Updates/Deletions: If existing rows in the table are updated or deleted, these changes will not be tracked by the stream. Only new inserts will be captured.

This is commonly used in scenarios where you want to process only the newly added records without worrying about modifications to existing data.

#### 1. Load JSON Data into Raw Layer

This task loads cricket match data from a JSON file stored in the `@cricket.land.my_stg/cricket/json` stage into the `raw.match_raw_tbl` table.
And it is scheduled to be executed every 5 mintue. 

```sql
create or replace task cricket.raw.load_json_to_raw
    warehouse='COMPUTE_WH'
    schedule = '5 minute'
    as
    copy into cricket.raw.match_raw_tbl from
    (
        select
            t.$1:meta::object as meta,
            t.$1:info::variant as info,
            t.$1:innings::array as innings,
            metadata$filename,
            metadata$file_row_number,
            metadata$file_content_key,
            metadata$file_last_modified
        from @cricket.land.my_stg/cricket/json (file_format => 'cricket.land.my_json_format') t
    )
    on_error = continue;
```

#### 2. Load Data into Clean Match Layer
This task reads from the stream cricket.raw.for_match_stream and inserts cleaned match data into the clean.match_detail_clean table.

```sql
CREATE OR REPLACE TASK cricket.raw.load_to_clean_match
    WAREHOUSE = 'COMPUTE_WH'
    AFTER cricket.raw.load_json_to_raw
    WHEN SYSTEM$STREAM_HAS_DATA('cricket.raw.for_match_stream')
AS
INSERT INTO cricket.clean.match_detail_clean
SELECT 
    info:match_type_number::int AS match_type_number,
    info:event.name::text AS event_name,
    CASE
        WHEN info:match_number::text IS NOT NULL THEN info:match_number
        WHEN info:match_stage::text IS NOT NULL THEN info:match_stage
        ELSE 'NA' 
    END AS match_stage,
    info:dates[0]::date AS event_date,
    DATE_PART('year', info:dates[0]::date) AS event_year,
    DATE_PART('month', info:dates[0]::date) AS event_month,
    DATE_PART('day', info:dates[0]::date) AS event_day,
    info:match_type::text AS match_type,
    info:season::text AS season,
    info:team_type::text AS team_type,
    info:overs::text AS overs,
    info:city::text AS city,
    info:venue::text AS venue,
    info:gender::text AS gender,
    info:teams[0]::text AS first_team,
    info:teams[1]::text AS second_team,
    CASE
        WHEN info:outcome.winner IS NOT NULL THEN 'Result Declared'
        WHEN info:outcome.result = 'tie' THEN 'Tie'
        WHEN info:outcome.result = 'no result' THEN 'No Result'
        ELSE info:outcome.result 
    END AS match_result,
    CASE 
        WHEN info:outcome.winner IS NOT NULL THEN info:outcome.winner 
        ELSE 'NA' 
    END AS winner,
    info:toss.winner::text AS toss_winner,
    INITCAP(info:toss.decision::text) AS toss_decision,
    -- Additional fields
    stg_file_name,
    stg_file_row_number,
    stg_file_hashkey,
    stg_modified_ts
FROM cricket.raw.for_match_stream;
```

#### 3. Load Data into Clean Player Layer
This task reads from the cricket.raw.for_player_stream stream and inserts cleaned player data into the clean.player_clean_tbl table.

```sql
create or replace task cricket.raw.load_to_clean_player
    warehouse='COMPUTE_WH'
    after cricket.raw.load_to_clean_match
    when system$stream_has_data('cricket.raw.for_player_stream')
as
insert into cricket.clean.player_clean_tbl
select 
    rcm.info:match_type_number::int as match_type_number,
    p.key::text as country,
    team.value::text as player_name ,
    rcm.stg_file_name ,
    rcm.stg_file_row_number,
    rcm.stg_file_hashkey,
    rcm.stg_modified_ts
from cricket.raw.for_player_stream rcm,
lateral flatten (input => rcm.info:players) p,
lateral flatten (input => p.value) team;
```

#### 4. Load Data into Clean Delivery Layer
This task reads from the cricket.raw.for_delivery_stream stream and inserts cleaned delivery data into the clean.delivery_clean_tbl table.

```sql
create or replace task cricket.raw.load_to_clean_delivery
    warehouse='COMPUTE_WH'
    after cricket.raw.load_to_clean_player
    when SYSTEM$STREAM_HAS_DATA('cricket.raw.for_delivery_stream')
as
insert into cricket.clean.delivery_clean_tbl
select 
    m.info:match_type_number::int as match_type_number,
    i.value:team::text as team_name,
    o.value:over::int+1 as over,
    d.value:bowler::text as bowler,
    d.value:batter::text as batter,
    d.value:non_striker::text as non_striker,
    d.value:runs.batter::text as runs,
    d.value:runs.extras::text as extras,
    d.value:runs.total::text as total,
    e.key::text as extra_type,
    e.value::int as extra_runs,
    w.value:player_out::text as player_out,
    w.value:kind::text as player_out_kind,
    w.value:fielders::variant as player_out_fielders,
    f.value:name::text as fielder_name,
    f.value:substitute::text as substitute,
    m.stg_file_name ,
    m.stg_file_row_number,
    m.stg_file_hashkey,
    m.stg_modified_ts
from cricket.raw.for_delivery_stream m,
lateral flatten (input => m.innings) i,
lateral flatten (input => i.value:overs) o,
lateral flatten (input => o.value:deliveries) d,
lateral flatten (input => d.value:extras, outer=>True) e,
lateral flatten (input => d.value:wickets, outer => True) w,
lateral flatten (input => w.value:fielders,outer => True) f;
```

#### 5. Load Data into Team Dimension
This task inserts distinct teams into the team_dim dimension table.

```sql
create or replace task cricket.raw.load_to_team_dim
    warehouse='COMPUTE_WH'
    after cricket.raw.load_to_clean_delivery
as
insert into cricket.consumption.team_dim (team_name)
select distinct team
from (
    select first_team as team
    from cricket.clean.match_detail_clean
    union all
    select second_team as team
    from cricket.clean.match_detail_clean
)
minus
select team_name from cricket.consumption.team_dim;
```

#### 6. Load Data into Player Dimension
This task inserts distinct players and their associated teams into the player_dim dimension table.

```sql
create or replace task cricket.raw.load_to_player_dim
    warehouse='COMPUTE_WH'
    after cricket.raw.load_to_clean_delivery
as
insert into cricket.consumption.player_dim  (team_id, player_name)
(select distinct t.team_id, p.player_name
from cricket.clean.player_clean_tbl p
join cricket.consumption.team_dim t
on t.team_name = p.country
minus
select team_id, player_name from cricket.consumption.player_dim
);
```

#### 7. Load Data into Venue Dimension
This task inserts distinct venues and their associated cities into the venue_dim dimension table.

```sql
create or replace task cricket.raw.load_to_venue_dim
    warehouse='COMPUTE_WH'
    after cricket.raw.load_to_clean_delivery
as
insert into cricket.consumption.venue_dim (venue_name, city)
(select venue,
    case when city is null then 'NA' else city end as city
from cricket.clean.match_detail_clean 
group by venue, city
minus 
select venue_name, city from cricket.consumption.venue_dim
);
```

#### 8. Load Data into Date Dimension
This task inserts distinct dates and their attributes into the date_dim dimension table.

```sql
create or replace task cricket.raw.load_to_date_dim
    warehouse='COMPUTE_WH'
    after cricket.raw.load_to_clean_delivery
as
insert into cricket.consumption.date_dim (date_id, full_dt, day, month, year, quarter, dayofweek, dayofmonth, dayofyear, dayofweekname,isweekend)
SELECT
    ROW_NUMBER() OVER (ORDER BY event_date) AS DateID,
    event_date AS FullDate,
    EXTRACT(DAY FROM event_date)::int AS Day,
    EXTRACT(MONTH FROM event_date) AS Month,
    EXTRACT(YEAR FROM event_date)::int AS Year,
    CASE WHEN EXTRACT(QUARTER FROM event_date) IN (1, 2, 3, 4) THEN EXTRACT(QUARTER FROM event_date) END AS Quarter,
    DAYOFWEEKISO(event_date) AS DayOfWeek,
    EXTRACT(DAY FROM event_date) AS DayOfMonth,
    DAYOFYEAR(event_date) AS DayOfYear,
    DAYNAME(event_date) AS DayOfWeekName,
    CASE When DAYNAME(event_date) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END as isweekend
FROM cricket.clean.match_detail_clean;
```

#### 9. Load Data into Match Fact Table
This task inserts match-related data into the match_fact table in the consumption layer.

```sql
create or replace task cricket.raw.load_match_fact
    warehouse = 'COMPUTE_WH'
    after cricket.raw.load_to_team_dim, cricket.raw.load_to_player_dim, cricket.raw.load_to_venue_dim, cricket.raw.load_to_date_dim
as
insert into cricket.consumption.match_fact 
select a.* from (
    select 
        m.match_type_number as match_id,
        dd.date_id as date_id,
        0 as referee_id,
        ftd.team_id as first_team_id,
        std.team_id as second_team_id,
        -- other match-related data
    from 
        cricket.clean.match_detail_clean m
        join cricket.consumption.date_dim dd on m.event_date = dd.full_dt
        join cricket.consumption.team_dim ftd on m.first_team = ftd.team_name
        join cricket.consumption.team_dim std on m.second_team = std.team_name
    -- additional joins
) a
left join cricket.consumption.match_fact b on a.match_id = b.match_id
where b.match_id is null;
```

#### 10. Load Data into Delivery Fact Table

This task inserts delivery-related data into the `delivery_fact` table in the consumption layer.

```sql
create or replace task cricket.raw.load_delivery_fact
    warehouse= 'COMPUTE_WH'
    after cricket.raw.load_match_fact
as
insert into cricket.consumption.delivery_fact
select a.* from (
    select 
        d.match_type_number as match_id,
        td.team_id,
        bpd.player_id as bowler_id, 
        spd.player_id as batter_id, 
        nspd.player_id as non_striker_id,
        d.over,
        d.runs,
        case when d.extra_runs is null then 0 else d.extra_runs end as extra_runs,
        case when d.extra_type is null then 'None' else d.extra_type end as extra_type,
        case when d.player_out is null then 'None' else d.player_out end as player_out,
        case when d.player_out_kind is null then 'None' else d.player_out_kind end as player_out_kind
    from 
        cricket.clean.delivery_clean_tbl d
    join cricket.consumption.team_dim td on d.team_name = td.team_name
    join cricket.consumption.player_dim bpd on d.bowler = bpd.player_name
    join cricket.consumption.player_dim spd on d.batter = spd.player_name
    join cricket.consumption.player_dim nspd on d.non_striker = nspd.player_name
) a
left join cricket.consumption.delivery_fact b on a.match_id = b.match_id
where b.match_id is null;
```
#### 11. Resuming All Tasks

To ensure that the entire data pipeline is executed correctly, all tasks need to be resumed. 

**Important**: The child tasks must be resumed first, followed by the root task. If the root task is not resumed first, the child tasks will not be able to be modified or resumed.

Here is the sequence of commands to resume all tasks:

##### Resume the child tasks first:
```sql
ALTER TASK cricket.raw.load_to_clean_match RESUME;
ALTER TASK cricket.raw.load_to_clean_player RESUME;
ALTER TASK cricket.raw.load_to_clean_delivery RESUME;
ALTER TASK cricket.raw.load_to_team_dim RESUME;
ALTER TASK cricket.raw.load_to_player_dim RESUME;
ALTER TASK cricket.raw.load_to_venue_dim RESUME;
ALTER TASK cricket.raw.load_to_date_dim RESUME;
ALTER TASK cricket.raw.load_match_fact RESUME;
ALTER TASK cricket.raw.load_delivery_fact RESUME;
```
##### Finally, resume the root task:
```sql
ALTER TASK cricket.raw.load_json_to_raw RESUME;
```
#### 12. Testing the Automation with Modified JSON Data

To test the automation of the data pipeline, you can make a few changes to your JSON files. You can either:

1. **Modify Existing Data** in the current JSON files and upload them again into your stage.
2. **Create a New JSON File** with modified data and upload it into the stage.

### Note:
- The example JSON data provided below is **incomplete** and only includes the **modified sections** for testing purposes. The other parts of the JSON file remain unchanged and were not directly impacted by these changes.
- In **real-life usage**, ensure that the full JSON structure matches the expected format. The modified JSON in this case does not represent a fully correct or complete JSON file, as it only focuses on the parts relevant for triggering the streams.

For the modified JSON file to trigger the previously created streams, ensure that the changes made are significant enough to be detected as new data. Here is an example of the modifications I made, which successfully triggered the three streams:

### Example of Modified JSON Data:
```json
{
  "match_type_number": 4705,
  "match_type": "T20I",
  "dates": [
    "2023-11-17"
  ],
  "city": "Cuttack",
  "players": {
    "South Africa": [
      "Q de Kock",
      "T Bavuma",
      "HE van der Dussen",
      "DA Miller",
      "H Klaasen",
      "AK Markram",
      "M Jansen",
      "G Coetzee",
      "KA Maharaj",
      "K Rabada",
      "L Ngidi"
    ],
    "Canada": [
      "AA Varadharajan",
      "AN Patel",
      "GU Bajwa",
      "KA Tathgur",
      "RA Pathan",
      "DI Heyliger",
      "JU Siddiqui",
      "NA Dhaliwal",
      "PA Kumar",
      "RI Joshi",
      "GU Sidhu"
    ]
  },
  "teams": [
    "South Africa",
    "Canada"
  ],
  "toss": {
    "decision": "field",
    "winner": "Canada"
  },
  "venue": "This is a sample venue for the city Cuttack."
}
```

## Monitoring Task History
After the tasks have been triggered, monitor their progress in the Task History section under Monitoring. You can view the following statuses:

- Success: If the task has executed without any issues.
- Failure: If the task has failed, you can view the logs for detailed error messages.

Use SQL queries or your dashboard to verify if the data has been successfully inserted into the consumption layer.
In my example, I added a new country 'Canada' that lost on a match with South Africa. 
[image]

### Note: you need to edit your filter by executing the query again so that the new data are also resulted. 
