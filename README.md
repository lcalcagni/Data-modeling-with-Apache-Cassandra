# Data Modeling with Apache Cassandra

#### Project Overview
This project is part of Sparkify, a fictitious startup startup that aims to analyze user activity and song listening habits from their new music streaming app. The main goal is to create a NoSQL database using Apache Cassandra to efficiently store and query user activity data. The dataset is composed of event logs from users, containing information about the songs they listen to, session data, and user details.

**The task involves**:

- Data modeling with Cassandra to answer specific analytical queries.
- Implementing an ETL pipeline to load data from CSV files into the Cassandra database.
- Running analytical queries on the dataset to retrieve insights.


#### Project Structure
This project is implemented in a Jupyter Notebook and includes the following sections:

1. Data Preprocessing:

- Loading the event data files (event_data) from CSV files.
- Merging multiple CSV files into a single, streamlined CSV (event_datafile_new.csv) that will be used for further processing.

2. Data Modeling with Cassandra:

- Creating a keyspace in Cassandra for storing the Sparkify data.
- Designing tables to handle specific queries.
- Loading data into the Cassandra tables.
- Running queries to extract the necessary information.

3. ETL Pipeline:

- An ETL process is used to read the data from the event logs, process it, and insert it into the appropriate Cassandra tables.

4. Querying the Data:

- After the data is loaded into Cassandra, queries are written to answer the following:
    - Retrieve the artist, song title, and song length for a given `sessionId` and `itemInSession`.
    - Retrieve the artist, song title, and user name (first and last) for a specific `userId` and `sessionId`, sorted by the order of the songs listened to.
    - Retrieve the list of users (first and last names) who listened to a specific song.

#### Tables Created:
1. Table: `song_by_session_item`

    **Purpose**: Retrieve the artist, song title, and length based on `sessionId` and `itemInSession`.
    **Primary Key**: `(sessionId, itemInSession)`
    Queries data for a specific session and item in session.

2. Table: `song_by_user_session`

    **Purpose**: Retrieve artist, song title, and user details for a given `userId` and `sessionId`.
    **Primary Key**: `((userId, sessionId), itemInSession)`
    Retrieves data sorted by the order of songs within a session for a specific user.

3. Table: `users_by_song`

    **Purpose**: Retrieve all users who listened to a specific song.
    **Primary Key**: `(song, userId)`
    Queries based on song title to find the users who listened to it.

#### Files in the Project
- **event_data**: The folder containing the original CSV files partitioned by date.
- **event_datafile_new.csv**: A merged and cleaned dataset that will be loaded into Cassandra.
- **Jupyter Notebook**: The notebook file contains the entire ETL pipeline, from data loading to querying Cassandra tables.

#### Running the Project
1. Set up Cassandra: Ensure that you have Apache Cassandra installed and running locally or in the cloud.
2. Run the ETL Pipeline: Use the provided Jupyter Notebook to load the CSV data into the Cassandra tables.
3. Query the Data: Run the given queries to analyze the data and retrieve insights based on song listening habits.

#### Conclusion
This project demonstrates how to model data using Apache Cassandra for a real-world application like Sparkify's music streaming platform. By understanding the underlying queries and designing the tables accordingly, the data is stored efficiently, and queries are optimized for quick retrieval of relevant information.