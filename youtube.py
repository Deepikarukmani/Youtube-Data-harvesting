import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_option_menu import option_menu
import mysql.connector as sql
import pymongo
from googleapiclient.discovery import build
from PIL import Image
from pymongo import MongoClient
import mysql.connector

# BUILDING CONNECTION WITH YOUTUBE API
api_key = 'AIzaSyDspbbO9XAfwaUmzrwWdnZzIIMGY4WrmiU'
youtube = build('youtube', 'v3', developerKey=api_key)

# FUNCTION TO GET CHANNEL DETAILS
def get_channel_details(channel_id):
    ch_data = []
    response = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                     id= channel_id).execute()

    for i in range(len(response['items'])):
        data = dict(Channel_id = channel_id[i],
                    Channel_name = response['items'][i]['snippet']['title'],
                    Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    Description = response['items'][i]['snippet']['description'],
                    Country = response['items'][i]['snippet'].get('country')
                    )
        ch_data.append(data)
    return ch_data

# FUNCTION TO GET VIDEO IDS
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=50,
                                           pageToken=next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids
print(get_channel_videos)

# FUNCTION TO GET VIDEO DETAILS
def get_video_details(v_ids):
    video_stats = []
    
    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(v_ids[i:i+50])).execute()
        for video in response['items']:
            video_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Tags = video['snippet'].get('tags'),
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Caption_status = video['contentDetails']['caption']
                               )
            video_stats.append(video_details)
    return video_stats


# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=v_id,
                                                    maxResults=100,
                                                    pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id = cmt['id'],
                            Video_id = cmt['snippet']['videoId'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = cmt['snippet']['totalReplyCount']
                           )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data

# Bridging a connection with MongoDB Atlas and Creating a new database(youtube_data)
client = MongoClient("mongodb+srv://deepikarukmani:Titi@cluster0.ichg4lz.mongodb.net/?retryWrites=true&w=majority")
db = client.youtube_data
#output=db.youtube_data

# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():   
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['Channel_name'])
    return ch_name


# CONNECTING WITH MYSQL DATABASE
myconnection = mysql.connector.connect(host ='localhost', port ='3306', user='root', password='Titi@123', database = "Youtube")
mycursor = myconnection.cursor()

mycursor.execute("show databases")
for x in mycursor:
    print(x)

mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube")
mycursor.execute("USE youtube")


create_channel_table = '''
        CREATE TABLE IF NOT EXISTS channel_data (
        channel_name VARCHAR(100),
        channel_id VARCHAR(500) PRIMARY KEY,
        subscribers INT,
        views_count INT,
        total_videos INT,
        playlist_id VARCHAR(100),
        channel_description TEXT
    );
'''
mycursor.execute(create_channel_table)
results = mycursor.fetchall()

mycursor.fetchall()  # Consume the result

create_video_table = '''
    CREATE TABLE IF NOT EXISTS video_details (
    title VARCHAR(500),
    video_id VARCHAR(255) PRIMARY KEY,
    channel_id VARCHAR(500),
    published_date DATE,
    video_description TEXT,
    views INT,
    likes INT,
    comments VARCHAR(500),
    time_duration VARCHAR(200)
);
'''
mycursor.execute(create_video_table)
mycursor.fetchall()  # Consume the result

create_comment_table = '''
    CREATE TABLE IF NOT EXISTS comment_details (
    comment_id INT PRIMARY KEY,
    comment_text TEXT,
    comment_author TEXT,
    comment_published_at DATE,
    video_id VARCHAR(255)
    
    );
'''
mycursor.execute(create_comment_table)
mycursor.fetchall()

# Inserting channel details
def insert_into_channels():
    collections = db.channel_details
    query = '''INSERT IGNORE INTO channel_data (channel_name, channel_id, subscribers, views_count, total_videos, playlist_id,channel_description) VALUES (%s, %s, %s, %s, %s, %s, %s)'''
    for i in collections.find({"Channel_name" : user_inp},{'_id' : 0}):
        mycursor.execute(query,tuple(i.values()))     
        myconnection.commit()
        mycursor.executemany(insert_channel_details, values)

# Inserting video details
def insert_into_videos():
    collections1 = db.video_details
    query1 = '''INSERT IGNORE INTO video_details (title, video_id, channel_id, published_date, video_description, views, likes, comments, time_duration)VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    for i in collections1.find({"Channel_name" : user_inp},{'_id' : 0}):
        mycursor.execute(query1,tuple(i.values()))
        myconnection.commit()
        mycursor.executemany(insert_video_details, values)

# Inserting comment details
def insert_into_comments():
    collections1 = db.video_details
    collections2 = db.comments_details
    query2 = '''INSERT IGNORE INTO comment_details (comment_id, comment_text, comment_author, comment_published_at, video_id) VALUES (%s, %s, %s, %s, %s)'''
    for vid in collections1.find({"Channel_name" : user_inp},{'_id' : 0}):
        for i in collections2.find({'Video_id': vid['Video_id']},{'_id' : 0}):
            mycursor.execute(query2,tuple(i.values()))
            myconnection.commit()
            mycursor.executemany(insert_comment_details, values)

# SETTING PAGE CONFIGURATIONS
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing",
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': """# This app is created by *Deepika!*"""})

# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home","Extract & Transform","View"], 
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "30px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "#C80101"},
                                   "icon": {"font-size": "30px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#C80101"}})

# HOME PAGE
if selected == "Home":
    col1,col2 = st.columns(2,gap= 'medium')
    col1.markdown("## :blue[Domain] : Social Media")
    col1.markdown("## :blue[Technologies used] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
    col1.markdown("## :blue[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")
    col2.markdown("#   ")
    col2.markdown("#   ")
    col2.markdown("#   ")
    
# EXTRACT AND TRANSFORM PAGE
if selected == "Extract & Transform":
    tab1,tab2 = st.tabs(["$\huge  EXTRACT $", "$\huge TRANSFORM $"])
    
    # EXTRACT TAB
    with tab1:
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID below :")
        ch_id = st.text_input("Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')

        if ch_id and st.button("Extract Data"):
            ch_details = get_channel_details(ch_id)
            st.write(f'#### Extracted data from :green["{ch_details[0]["Channel_name"]}"] channel')
            st.table(ch_details)

        if st.button("Upload to MongoDB"):
            with st.spinner('Please Wait for it...'):
                ch_details = get_channel_details(ch_id)
                v_ids = get_channel_videos(ch_id)
                vid_details = get_video_details(v_ids)
                
                def comments():
                    com_d = []
                    for i in v_ids:
                        com_d+= get_comments_details(i)
                    return com_d
                comm_details = comments()

                collections1 = db.channel_details
                collections1.insert_many(ch_details)

                collections2 = db.video_details
                collections2.insert_many(vid_details)

                collections3 = db.comments_details
                collections3.insert_many(comm_details)
                st.success("Upload to MongoDB successful !!")

    with tab2:
        st.markdown("#   ")
        st.markdown("### Select a channel to begin Transformation to SQL")
        
        ch_names = channel_names()
        user_inp = st.selectbox("Select channel",options= ch_names)
        

        if st.button("Submit"):
            try:
                insert_into_channels()
                insert_into_videos()
                insert_into_comments()
                st.success("Transformation to MySQL Successful !!")
            except:
                st.error("Channel details already transformed !!")

# VIEW PAGE
if selected == "View":
    st.subheader("Select a  question!!")
    ques1 = '1.	What are the names of all the videos and their corresponding channels?'
    ques2 = '2.	Which channels have the most number of videos, and how many videos do they have?'
    ques3 = '3.	What are the top 10 most viewed videos and their respective channels?'
    ques4 = '4.	How many comments were made on each video, and what are their corresponding video names?'
    ques5 = '5.	Which videos have the highest number of likes, and what are their corresponding channel names?'
    ques6 = '6.	What is the total number of likes and dislikes for each video, and what are their corresponding video names?'
    ques7 = '7.	What is the total number of views for each channel, and what are their corresponding channel names?'
    ques8 = '8.	What are the names of all the channels that have published videos in the year 2022?'
    ques9 = '9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?'
    ques10 = '10.Which videos have the highest number of comments, and what are their corresponding channel names?'
    question = st.selectbox('Queries!!',(ques1,ques2,ques3,ques4,ques5,ques6,ques7,ques8,ques9,ques10))
    clicked = st.button("MYSQL")

    if clicked:
        myconnection = mysql.connector.connect(
        host ='localhost', 
        port ='3306', 
        user='root', 
        password='Titi@123', 
        database = "Youtube")

        mycursor = myconnection.cursor()

        mycursor.execute("show databases")
        for x in mycursor:
            print(x)

        mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube")
        mycursor.execute("USE youtube")
        databases = mycursor.fetchall() 

        if question == ques1:
            query = "select title,channel_name FROM video_details AS A INNER JOIN channel_data AS B ON A.channel_id=B.channel_id;"
            mycursor.execute(query)
            results =mycursor.fetchall()
            st.dataframe(results)
        elif question == ques2:
            query = "select channel_name,total_videos from channel_data where total_videos=(SELECT MAX(total_videos) FROM channel_data);"
            mycursor.execute(query)
            results =mycursor.fetchall()
            st.dataframe(results)
        elif question == ques3:
            query = "select title,views,channel_name from video_details as a inner join channel_data as b on a.channel_id=b.channel_id order by views desc limit 10;"
            mycursor.execute(query)
            results =mycursor.fetchall()
            st.dataframe(results)
        elif question == ques4:
            query = "select title,comments from video_details order by comments desc;"
            mycursor.execute(query)
            results =mycursor.fetchall()
            st.dataframe(results)
        elif question == ques5:
            query = "select title,likes,channel_name from video_details as a inner join channel_data as b on a.channel_id=b.channel_id where likes=(select max(likes) from video_details);"
            mycursor.execute(query)
            results =mycursor.fetchall()
            st.dataframe(results)
        elif question == ques6:
            query = "select likes,title from video_details order by likes asc;"
            mycursor.execute(query)
            results =mycursor.fetchall()
            st.dataframe(results)
        elif question == ques7:
            query = "select channel_name,sum(views) as total_video_count from video_details as a inner join channel_data as b on a.channel_id=b.channel_id group by b.channel_name order by sum(views);"
            mycursor.execute(query)
            results =mycursor.fetchall()
            st.dataframe(results)
        elif question == ques8:
            query = "select published_date,channel_name from video_details as a inner join channel_data as b on a.channel_id=b.channel_id WHERE published_date BETWEEN '2022-01-01' AND '2022-12-31';"
            mycursor.execute(query)
            results =mycursor.fetchall()
            st.dataframe(results)
        elif question == ques9:
            query = "select channel_name,avg(time_duration) from video_details as a inner join channel_data as b on a.channel_id=b.channel_id group by b.channel_name;"
            mycursor.execute(query)
            results =mycursor.fetchall()
            st.dataframe(results)
        elif question == ques10:
            query = "select title,comments,channel_name from video_details as a inner join channel_data as b on a.channel_id=b.channel_id where comments=(select max(comments) from video_details);"
            mycursor.execute(query)
            results =mycursor.fetchall()
            st.dataframe(results)
