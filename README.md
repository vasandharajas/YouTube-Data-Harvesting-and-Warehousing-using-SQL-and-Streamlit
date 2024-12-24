YouTube Data Harvesting and Warehousing using SQL and Streamlit

The project focuses on creating an interactive Streamlit application that allows users to retrieve, store, and analyze data from YouTube channels. The application uses the YouTube Data API to fetch detailed information about channels and their videos. The gathered data is stored in a SQL data warehouse (MySQL or PostgreSQL) for structured querying and analysis. The primary goal is to provide insights into various YouTube channels through a user-friendly interface.

Key Features and Functionalities
User Input for YouTube Channel ID:
Users can input a YouTube channel ID into the Streamlit application.
The app fetches relevant channel data using the YouTube API, including:
Channel name
Subscription count
Total video count
Playlist ID
Details of each video (like video ID, name, description, views, likes, dislikes, comments, etc.)

Data Collection from Multiple Channels:
Users can collect data from up to 10 different YouTube channels.
Data is harvested by interacting with the YouTube API and is temporarily stored in memory using Pandas DataFrames before being moved to a SQL database.

SQL Data Storage:
Collected data is stored in either MySQL or PostgreSQL databases. The schema is designed to efficiently store channel and video details.
Tables include:
channels (channel_id, name, subscription_count, etc.)
videos (video_id, video_name, view_count, like_count, etc.)
comments (comment_id, comment_text, video_id, comment_author, etc.)

Data Querying and Reporting:
Users can query the SQL database using predefined SQL queries to generate various insights about the data, such as:
Video and Channel Names: List of all video names along with their corresponding channels.
Most Videos: Channels with the most number of videos and their counts.
Top 10 Most Viewed Videos: List of the top 10 most viewed videos with their channels.
Comment Count by Video: Number of comments on each video.
Videos with Highest Likes: Videos with the highest number of likes and their corresponding channels.
Total Likes and Dislikes: Total likes and dislikes for each video.
Total Views by Channel: Total views for each channel.
Videos from 2022: Channels that published videos in 2022.
Average Video Duration by Channel: Average video length for each channel.
Videos with Most Comments: Videos that have the most comments.

Data Visualization in Streamlit:
Streamlit is used to display results in a tabular format.
The application allows users to interact with the data, including querying specific details and visualizing the results in a simple interface.
Approach and Implementation

1. Setting Up Streamlit App:
Streamlit is chosen for building the user interface, as it allows quick and interactive app development.
The app will have fields to input YouTube channel IDs, a button to collect data, and options to store the data in the selected SQL database.

2. Integrating with YouTube API:
Use Google’s YouTube Data API v3 for accessing channel and video data.
The google-api-python-client library will be used to send requests to the API.
For each channel, the app will fetch information like channel statistics, videos, and comments.
API requests will be handled using Python functions, which will be triggered by user input in the Streamlit UI.

3. Storing Data in SQL Database:
Data will be cleaned and structured using Pandas and then stored in a relational database (MySQL or PostgreSQL).
Python's SQLAlchemy will be used for interacting with the database and executing queries.

4. SQL Querying and Reporting:
Predefined SQL queries will be written to retrieve various insights from the database.
The user can use a search bar to input queries or select options from a menu to generate reports.

5. Display Data in Streamlit:
Data fetched from SQL queries will be displayed in tables using Streamlit’s st.write function.
The application will include various visualizations, such as bar charts or line graphs, to display insights like views or comments over time.

SQL Tables:

channels:
channel_id (Primary Key)
name
subscription_count
channel_views
playlist_id

videos:
video_id (Primary Key)
channel_id (Foreign Key referencing channels)
video_name
view_count
like_count
dislike_count
comment_count

comments:
comment_id (Primary Key)
video_id (Foreign Key referencing videos)
author
comment_text

Conclusion
This project involves developing a dynamic and interactive application that fetches data from YouTube using the API, stores it in a SQL database, and enables users to query and analyze it using SQL queries through Streamlit. The application will be helpful for analyzing the performance and engagement of YouTube channels and videos.
