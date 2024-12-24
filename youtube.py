from googleapiclient.discovery import build
import pymongo
from pymongo import MongoClient
import ssl
import psycopg2
from pymongo import MongoClient
import pandas as pd
import streamlit as st

#Function to fetch the data from MYSQL Database
def fetch_data(query):
    mydb = mysql.connector.connect(host="localhost", 
                                   user="postgres", 
                                   password="password", 
                                   database="youtube_data",
                                   port="5432")
    df = pd.read_sql(query, mydb)
    mydb.close()
    return df

#Function to execute the predefined queries
def execute_query(question):
    query_mapping = {
        "What are the names of all the videos and their corresponding channels?":
		         """SELECT Video_title,channel_name 
                 FROM videos 
                 JOIN channels ON channels.channel_id=videos.channel_id;""",
        "Which channels have the most number of videos, and how many videos do they have?": 
		         """SELECT channel_name, COUNT(video_id) AS video_count
				 FROM videos 
                 JOIN Channels ON channels.channel_id=videos.channel_id
                 GROUP BY channel_name
                 ORDER BY video_count DESC;""",
        "What are the top 10 most viewed videos and their respective channels?": 
		         """SELECT video_title,channel_name 
                 FROM videos 
                 JOIN channels ON channels.channel_id =videos.channel_id 
                 ORDER BY video_viewcount DESC 
                 LIMIT 10;""",
        "How many comments were made on each video, and what are their corresponding video names?": 
		         """SELECT video_title, COUNT(*) AS comment_counts
                 FROM videos 
                 JOIN comments on videos.video_id=comments.video_id
                 GROUP BY video_title;""",
        "Which videos have the highest number of likes, and what are their corresponding channel names?": 
		         """SELECT video_title,channel_name
                 FROM videos 
                 JOIN channels ON channels.channel_id=videos.channel_id
                 ORDER BY video_likecount DESC
                 LIMIT 1;""",
        "What is the total number of likes for each video, and what are their corresponding video names?":	          
                """SELECT videos.Video_title, SUM(videos.Video_likecount) AS total_likes
                  FROM videos
                  GROUP BY videos.Video_title;""",
        "What is the total number of views for each channel, and what are their corresponding channel names?": 
		          """SELECT channel_name, SUM(video_viewcount) AS Total_views
                  FROM videos
                  JOIN channels ON channels.channel_id=videos.channel_id
                  GROUP BY channel_name;""",
        "What are the names of all the channels that have published videos in the year 2022?": 
		          """SELECT DISTINCT channels.channel_name
                  FROM channels
                  JOIN videos ON channels.channel_id = videos.channel_id
                  WHERE YEAR(videos.Video_pubdate) = 2022;""",
        "What is the average duration of all videos in each channel, and what are their corresponding channel names?": 
		          """ SELECT channel_name,AVG(video_duration) AS Average_duration
                  FROM videos
                  JOIN channels ON videos.channel_id = channels.channel_id
                  GROUP BY channel_name;""",
        "Which videos have the highest number of comments, and what are their corresponding channel names?": 
		          """ SELECT video_title,channel_name
                  FROM videos
                  JOIN channels ON videos.channel_id = channels.channel_id
                  ORDER BY Video_commentcount DESC
                  LIMIT 1;""" 
    }

    query=query_mapping.get(question)
    if query:
        return fetch_data(query)
    else:
        return pd.DataFrame()

def Api_connect():
    Api_id="yourapikey"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=Api_id)
    return youtube
youtube=Api_connect()

def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id                                                  
    )
    response=request.execute()
    
    for i in response['items']:
        data=dict(channel_Name=i['snippet']['title'],
                  channel_Id=i['id'],
                  subscribers=i['statistics']['subscriberCount'],
                  views=i['statistics']['viewCount'],
                  Total_videos=i['statistics']['videoCount'],
                  channel_Description=i['snippet']['description'],
                  playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'],
                  joined=i['snippet']['publishedAt'],
                 )
        return data 

def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                     part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    next_page_token=None
    
    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')
    
        if next_page_token is None:
           break
    return video_ids

#Get Video Information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()
    
        for item in response["items"]:
            data=dict(channel_Name=item['snippet']['channelTitle'],
                      channel_Id=item['snippet']['channelId'],
                      video_Id=item['id'],
                      Title=item['snippet']['title'],
                      Tags=item['snippet'].get('tags'),
                      Thumbnail=item['snippet']['thumbnails']['default']['url'],
                      Description=item['snippet'].get('description'),
                      Published_Date=item['snippet']['publishedAt'],
                      Duration=item['contentDetails']['duration'],
                      views=item['statistics'].get('viewCount'),
                      Likes=item['statistics'].get('likeCount'),
                      comments=item['statistics'].get('commentCount'),
                      Favorite_Count=item['statistics']['favoriteCount'],
                      Definition=item['contentDetails']['definition'],
                      Caption_Status=item['contentDetails']['caption'],
                      ) 
            video_data.append(data)
    return video_data

#Get Comment Information
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()
            
            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                          video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                          comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                          comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                          comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
    
                Comment_data.append(data)
    
    except:
        pass
    return Comment_data

#Get_Playlist_Details
def get_playlist_details(channel_id):
    next_page_token=None
    All_data=[]
    while True:
        request=youtube.playlists().list(
                part='snippet,contentDetails',
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
        )    
        response=request.execute()
        
        for item in response['items']:
            data=dict(PlaylistId=item['id'],
                      Title=item['snippet']['title'],
                      Channel_Id=item['snippet']['channelId'],
                      Channel_Name=item['snippet']['channelTitle'],
                      PublishedAt=item['snippet']['publishedAt'],
                      Video_Count=item['contentDetails']['itemCount'])
            All_data.append(data)
    
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
                break
    return All_data

client=pymongo.MongoClient("mongodb+srv://vasanthmuthoot893:vasandhs@cluster0.e0jvv.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db=client["Youtube_data"]

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    print("Channel Details:", ch_details)  # Print channel details to verify the data

    pl_details = get_playlist_details(channel_id)
    print("Playlist Details:", pl_details)  # Print playlist details to verify the data

    vi_ids = get_videos_ids(channel_id)
    print("Video IDs:", vi_ids)  # Print video IDs to verify the data

    vi_details = get_video_info(vi_ids)
    print("Video Details:", vi_details)  # Print video details to verify the data

    com_details = get_comment_info(vi_ids)
    print("Comment Details:", com_details)  # Print comment details to verify the data

    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information": ch_details, "playlist_information": pl_details,
                      "video_information": vi_details, "comment_information": com_details})

    return "upload completed successfully"


# Tables Creation For Channels, Playlists, Comments, Vedios
# Connection to PostgreSQL
def channel_details(channel_name_s):
    mydb = psycopg2.connect(host="localhost", 
                            user="postgres", 
                            password="password", 
                            database="youtube_data", 
                            port="5432")
    
    
    # Fetch channel data
    single_channel_detail = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({"channel_information.channel_Name": channel_name_s},{"_id":0}):
        single_channel_detail.append(ch_data["channel_information"])
    
    df_single_channel_detail = pd.DataFrame(single_channel_detail)
    
    # Insert data into PostgreSQL
    try:
        # Create table if not exists
        with mydb.cursor() as cursor:
            create_query = '''CREATE TABLE IF NOT EXISTS channels (Channel_Name varchar(100), 
                                                                   Channel_Id varchar(80) primary key, 
                                                                   Subscribers bigint, 
                                                                   Views bigint, 
                                                                   Total_Videos int, 
                                                                   Channel_Description text, 
                                                                   Playlist_Id varchar(80))'''
            cursor.execute(create_query)
            mydb.commit()
    
        # Insert rows into the table
        with mydb.cursor() as cursor:
            insert_query = '''INSERT INTO channels (Channel_Name, 
                                                    Channel_Id, 
                                                    Subscribers, 
                                                    Views, 
                                                    Total_Videos, 
                                                    Channel_Description, 
                                                    Playlist_Id) 
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s) 
                                                    ON CONFLICT (Channel_Id) DO NOTHING
                                                    '''
            for index, row in df_single_channel_detail.iterrows():
                values = (row['channel_Name'],
                          row['channel_Id'],
                          row['subscribers'],
                          row['views'],
                          row['Total_videos'],
                          row['channel_Description'],
                          row['playlist_Id'])
                try:
                    cursor.execute(insert_query, values)
                except Exception as e:
                    print(f"Error inserting data for channel {row['channel_Name']}: {e}")
                    continue  # Skip this row and continue inserting other rows
            mydb.commit()
    
    except Exception as e:
        print(f"Error occurred: {e}")
    
    finally:
        mydb.close()

def playlist_table(channel_name_s):
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="password",
                            database="youtube_data",
                            port="5432")
    cursor = mydb.cursor()
    
    create_query = '''CREATE TABLE IF NOT EXISTS playlists(Playlist_Id varchar(100) primary key,
                                                           Title varchar(100),
                                                           Channel_Id varchar(100),
                                                           Channel_Name varchar(100),
                                                           PublishedAt timestamp,
                                                           Video_Count int)'''
    cursor.execute(create_query)
    mydb.commit()

    single_playlist_details = []
    db = client["Youtube_data"] 
    coll1 = db["channel_details"]
    for ch_data in coll1.find({"channel_information.channel_Name": channel_name_s},{"_id":0}): 
        single_playlist_details.append(pl_data["playlist_information"]) 
    df_single_playlist_details=pd.DataFrame(single_playlist_details[0])

    for index, row in df_single_playlist_details.iterrows():
        insert_query = '''INSERT INTO playlists (Playlist_Id, 
                                                 Title, 
                                                 Channel_Id, 
                                                 Channel_Name, 
                                                 PublishedAt, 
                                                 Video_Count)
                                                 
                                                 VALUES (%s, %s, %s, %s, %s, %s)
                                                 ON CONFLICT (Playlist_Id) DO NOTHING'''
        
        values = (row['PlaylistId'],
                  row['Title'],
                  row['Channel_Id'],
                  row['Channel_Name'],
                  row['PublishedAt'],
                  row['Video_Count']
                  )
        
       
        cursor.execute(insert_query, values)
        mydb.commit()

def videos_table(channel_name_s):
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="password",
                            database="youtube_data",
                            port="5432")
    
    cursor = mydb.cursor()
    
    create_query = '''CREATE TABLE IF NOT EXISTS videos(channel_Name varchar(100),
                                                        channel_Id varchar(100),
                                                        video_Id varchar(30) primary key,
                                                        Title varchar(150),
                                                        Tags text,
                                                        Thumbnail varchar(200),
                                                        Description text,
                                                        Published_Date timestamp,
                                                        Duration interval,
                                                        views bigint,
                                                        Likes bigint,
                                                        comments int,
                                                        Favorite_Count int,
                                                        Definition varchar(10),
                                                        Caption_Status varchar(50))'''
    
    
    cursor.execute(create_query)
    mydb.commit()
    
    single_videos_details = []
    db = client["Youtube_data"] 
    coll1 = db["channel_details"]
    for ch_data in coll1.find({"channel_information.channel_Name": channel_name_s},{"_id":0}):
        single_videos_details.append(ch_data["video_information"]) 
    df_single_videos_details=pd.DataFrame(single_videos_details[0])
    
    for index, row in df_single_videos_details.iterrows():
        insert_query = '''INSERT INTO videos(channel_Name,
                                             channel_Id,
                                             video_Id,
                                             Title,
                                             Tags,
                                             Thumbnail,
                                             Description,
                                             Published_Date,
                                             Duration,
                                             views,
                                             Likes,
                                             comments,
                                             Favorite_Count,
                                             Definition,
                                             Caption_Status)
                                                     
                                             VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s)
                                             ON CONFLICT (video_Id) DO NOTHING'''
            
        values = (row['channel_Name'],
                  row['channel_Id'],
                  row['video_Id'],
                  row['Title'],
                  row['Tags'],
                  row['Thumbnail'],
                  row['Description'],
                  row['Published_Date'],
                  row['Duration'],
                  row['views'],
                  row['Likes'],
                  row['comments'],
                  row['Favorite_Count'],
                  row['Definition'],
                  row['Caption_Status']
                  )
            
           
        cursor.execute(insert_query, values)
        mydb.commit()

def comments_table(channel_name_s):
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="password",
                            database="youtube_data",
                            port="5432")
    cursor = mydb.cursor()
    
    create_query = '''CREATE TABLE IF NOT EXISTS comments(Comment_Id varchar(100) primary key,
                                                          video_Id varchar(50),
                                                          comment_Text text,
                                                          comment_Author varchar(150),
                                                          comment_Published timestamp)'''
    
    cursor.execute(create_query)
    mydb.commit()
    
    single_comments_details = []
    db = client["Youtube_data"] 
    coll1 = db["channel_details"]
    for ch_data in coll1.find({"channel_information.channel_Name": channel_name_s},{"_id":0}):
        single_comments_details.append(ch_data["comment_information"]) 
    df_single_comments_details=pd.DataFrame(single_comments_details[0])
    
    for index, row in df_single_comments_details.iterrows():
        insert_query = '''INSERT INTO comments(Comment_Id, 
                                               video_Id, 
                                               comment_Text, 
                                               comment_Author, 
                                               comment_Published)      
                                               VALUES (%s, %s, %s, %s, %s)
                                               ON CONFLICT (Comment_Id) DO NOTHING'''
            
        values = (row['Comment_Id'],
                      row['video_Id'],
                      row['comment_Text'],
                      row['comment_Author'],
                      row['comment_Published'])
                      
        cursor.execute(insert_query, values)
        mydb.commit()

def tables(single_channel):
    channel_details(single_channel)
    playlist_table(single_channel)
    videos_table(single_channel)
    comments_table(single_channel)

    return "Tables Created Successfully"

def show_channels_table():
    ch_list = []
    db = client["Youtube_data"] 
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}): 
        ch_list.append(ch_data["channel_information"]) 
    df = st.dataframe(ch_list)

    return df

def show_playlists_table():
    pl_list = []
    db = client["Youtube_data"] 
    coll1 = db["channel_details"]
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}): 
        for i in range(len(pl_data["playlist_information"])): 
            pl_list.append(pl_data["playlist_information"][i]) 
    df1=st.dataframe(pl_list)

    return df1

def show_videos_table():
    vi_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        print("vi_data:", vi_data)  # Print the contents of vi_data to verify the structure
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    print("vi_list:", vi_list)  # Print the collected video information
    df2 = st.dataframe(vi_list)

    return df2

def show_comments_table():
    com_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        print("com_data:", com_data)  # Print the contents of com_data to verify the structure
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    print("com_list:", com_list)  # Print the collected comment information
    df3 = st.dataframe(com_list)

    return df3


# Streamlit Space

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WHAREHOUSING]")
    st.header("SubDetails of Data Taken")
    st.caption("Python SCripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Collection of Datas To Getting By MongoDB and SQL")

# Ensure channel_id is defined
channel_id = st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    ch_ids = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        if "channel_information" in ch_data:
            if "Channel_Id" in ch_data["channel_information"]:
                ch_ids.append(ch_data["channel_information"]["Channel_Id"])
            else:
                print("Channel_Id key not found in channel_information")
        else:
            print("channel_information key not found in ch_data")

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")
    else:
        insert = channel_details(channel_id)
        st.success(insert)

all_channels = []
db = client["Youtube_data"]
coll1 = db["channel_details"]

for ch_data in coll1.find({}, {"_id":0, "channel_information":1}):
    print(ch_data)
    if "channel_Name" in ch_data["channel_information"]:
        all_channels.append(ch_data["channel_information"]["channel_Name"])
    else:
        print("Key 'Channel_Name' not found in channel_information")

unique_channel = st.selectbox("select the channel", all_channels)

    
if st.button("Transfer to SQL"):
    Table=tables(unique_channel)
    st.success(Table)
show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlists_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()

#SQL Connection

mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="password",
                    database="youtube_data",
                    port="5432")
cursor=mydb.cursor()

question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                              "2. channels with most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. comments in each videos",
                                              "5. Videos with higest likes",
                                              "6. likes of all videos",
                                              "7. views of each channel",
                                              "8. videos published in the year of 2022",
                                              "9. average duration of all videos in each channel",
                                              "10. videos with highest number of comments"))

if question=="1. All the videos and the channel name":
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif question=="2. channels with most number of videos":
    query2='''select channel_name as channelname,total_videos as no_videos from channels 
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

elif question=="3. 10 most viewed videos":
    query3='''select views as views,channel_name as channelname,title as videotitle from videos 
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)

elif question=="4. comments in each videos":
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)

elif question=="5. Videos with higest likes":
    query5='''select title as videotitle,channel_name as channelname,likes as likecount
                from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=="6. likes of all videos":
    query6='''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=="7. views of each channel":
    query7='''select channel_name as channelname ,views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df7)

elif question=="8. videos published in the year of 2022":
    query8='''select title as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
    st.write(df8)

elif question=="9. average duration of all videos in each channel":
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)

elif question=="10. videos with highest number of comments":
    query10='''select title as videotitle, channel_name as channelname,comments as comments from videos where comments is
                not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
    st.write(df10)