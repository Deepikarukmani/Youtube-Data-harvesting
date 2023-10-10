import googleapiclient.discovery
from googleapiclient.discovery import build

import pandas as pd
import streamlit as st

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo import MongoClient
import pymongo as pg

import mysql.connector
import mysql
from mysql.connector import Error
import time
from datetime import datetime, timedelta
from datetime import datetime


# Define the API key
api_key = 'AIzaSyDspbbO9XAfwaUmzrwWdnZzIIMGY4WrmiU'

channel_id = ['UCQH-o7L0zZ-jicCS_5wgdig',  #1- Naattu Nadappu
              'UCBJycsmduvYEL83R_U4JriQ',   #2- Marques Brownlee
              'UCRtAu8OVYVfuNZmTV-Ug0zA',   #3- Buddhism
              'UCE_M8A5yxnLfW0KghEeajjw',   #4- Apple
              'UC16niRr50-MSBwiO3YDb3RA',    #5- BBC News
              'UCmJlSkSkgdXama3GSUgMC4g',    #6- TrakinTechTamilOfficial
              'UCTIJerXXeEuYuYPSTPblqVg',    #7- Adam Tech
              'UC2Zs9v2hL2qZZ7vsAENsg4w',    #8- Justin Sung
              'UCI_EXjUIOC1caoKECKrOT7Q',    #9- Chinese with Jessie
              'UCRmAofy-eike2_yARG2Td4w']    #10- Street Light

youtube = build('youtube', 'v3', developerKey=api_key)

#Function to get Channel Statistics

def get_channel_stats(youtube, channel_id):
  all_data=[]

  request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id= ','.join(channel_id))

  response = request.execute()

  for i in range(len(response["items"])):
    data =dict(channel_name = response['items'][i]['snippet']['title'],
               subscribers = response['items'][i]['statistics']['subscriberCount'],
               views = response['items'][i]['statistics']['viewCount'],
               Total_videos = response['items'][i]['statistics']['videoCount'],
               playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])

    all_data.append(data)

  return all_data

print(get_channel_stats(youtube, channel_id))

channel_statistics = get_channel_stats(youtube, channel_id)

channel_data = pd.DataFrame(channel_statistics)

print(channel_data)

print(channel_data.dtypes)

channel_data['subscribers'] = pd.to_numeric(channel_data['subscribers'])
channel_data['views'] = pd.to_numeric(channel_data['views'])
channel_data['Total_videos'] = pd.to_numeric(channel_data['Total_videos'])
print(channel_data.dtypes)

print(channel_data)

playlist_id = channel_data.loc[channel_data ['channel_name']=='Adam Tech', 'playlist_id'].iloc[0]

print(playlist_id)

#Function to get Video ids

def get_video_ids(youtube, playlist_id):

  request = youtube.playlistItems().list(
            part = 'contentDetails',
            playlistId = playlist_id)

  response = request.execute()
  return response

print(get_video_ids(youtube, playlist_id))

#Function to get Video ids

def get_video_ids(youtube, playlist_id):

  request = youtube.playlistItems().list(
            part = 'contentDetails',
            playlistId = playlist_id,
            maxResults = 50)

  response = request.execute()

  video_ids = []

  for i in range(len(response['items'])):
    video_ids.append(response['items'][i]['contentDetails']['videoId'])

  next_page_token = response.get('nextPageToken')
  more_pages = True

  while more_pages:
      if next_page_token is None:
          more_pages = False
      else:
          request = youtube.playlistItems().list(
                         part='contentDetails',
                         playlistId = playlist_id,
                         maxResults = 50,
                         pageToken = next_page_token)

          response = request.execute()

          for i in range(len(response['items'])):
              video_ids.append(response['items'][i]['contentDetails']['videoId'])

          next_page_token = response.get('nextPageToken')

  return video_ids

video_ids = get_video_ids(youtube, playlist_id)

print(video_ids)

def convert_duration(duration_string):
    # By calling timedelta() without any arguments, the duration
    # object is initialized with a duration of 0 days, 0 seconds, and 0 microseconds. Essentially, it sets the initial duration to zero.
    duration_string = duration_string[2:]  # Remove "PT" prefix
    duration = timedelta()
    
    # Extract hours, minutes, and seconds from the duration string

    if 'H' in duration_string:
        hours, duration_string = duration_string.split('H')
        duration += timedelta(hours=int(hours))
    
    if 'M' in duration_string:
        minutes, duration_string = duration_string.split('M')
        duration += timedelta(minutes=int(minutes))
    
    if 'S' in duration_string:
        seconds, duration_string = duration_string.split('S')
        duration += timedelta(seconds=int(seconds))
    
    # Format duration as H:MM:SS

    duration_formatted = str(duration)
    if '.' in duration_formatted:
        hours, rest = duration_formatted.split(':')
        minutes, seconds = rest.split('.')
        duration_formatted = f'{int(hours)}:{int(minutes):02d}:{int(seconds):02d}'
    else:
        duration_formatted = duration_formatted.rjust(8, '0')
    
    return duration_formatted

def convert_timestamp(timestamp):
    datetime_obj = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    formatted_time = datetime_obj.strptime("%Y-%m-%d%H:%M:%S")
    return formatted_time

#function to get video details

def get_video_details(youtube, video_ids):
    all_video_stats = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
                    part='snippet,statistics',
                    id = ','.join(video_ids[i:i+50]))
        response = request.execute()


        for video in response['items']:
            video_stats = dict(Title = video['snippet']['title'],
                          Published_date = video['snippet']['publishedAt'],
                          Views = video['statistics']['viewCount'],
                          Likes = video['statistics'].get('likeCount',0),
                          Dislikes = video['statistics'].get('dislikeCount',0),
                          Comments = video['statistics']['commentCount']
                          )

        all_video_stats.append(video_stats)

    return all_video_stats


video_details = get_video_details(youtube, video_ids)
video_data = pd.DataFrame(video_details)

def get_comment_details(youtube, video_ids):
    all_video_stats = []

    for video_id in video_ids:
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id
        )
        response = request.execute()

        # Process the response and append the comment details to all_video_stats
        for comments in response['items']:
            comment = dict(
                comment_id=comments["id"],
                comment_text=comments["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                comment_author=comments["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                comment_published_at=convert_timestamp(comments["snippet"]["topLevelComment"]["snippet"]["publishedAt"]),
                video_id= video_id[0]
            )
            all_video_stats.append(comment)

    return all_video_stats

video_data['Published_date'] = pd.to_datetime(video_data['Published_date']).dt.date
video_data['Views'] = pd.to_numeric(video_data['Views'])
video_data['Likes'] = pd.to_numeric(video_data['Likes'])
video_data['Comments'] = pd.to_numeric(video_data['Comments'])
print(video_data)

video_data['Month'] = pd.to_datetime(video_data['Published_date']).dt.strftime('%b')

print(video_data)

video_data['Published_date'] = pd.to_datetime(video_data['Published_date']).dt.date

videos_per_month = video_data.groupby('Month', as_index=False).size()

print(videos_per_month)

def get_all_data(youtube,channel_id,video_ids):
            # Get the channel data
            channel_statistics = get_channel_stats(youtube, channel_ids)
            st.subheader('Channel statistics')
            st.write(channel_statistics)

           # Get the video details
            video_details = get_video_details(youtube, video_ids)
            st.subheader('Video Details')
            st.write(video_details)

            # Comment data
            comment_details = get_comment_details(youtube, video_ids)
            st.subheader('Comments')
            st.write(comment_details)

            data_file = {'ch_id': channel_data,
                         'video_info': video_details,
                         'comments': comment_details}

            return data_file

# CONNECT TO MONGODB

def store_information_in_mongodb(dataharvest):
    client = MongoClient("mongodb+srv://deepikarukmani:Titi@cluster0.ichg4lz.mongodb.net/?retryWrites=true&w=majority")
    youtube = MongoClient("mongodb+srv://deepikarukmani:Titi@cluster0.ichg4lz.mongodb.net/?retryWrites=true&w=majority")
    db=client.youtube1
    output=db.project1

    output.insert_one({"name":"dataharvest1"})

    client

    client.list_database_names()
    db = client['youtube1']
    db.list_collection_names()
    client.list_database_names()
    db = client["youtube1"]
    my_project = db['project']
    import csv
    import json

    file = open("Adam Tech.csv","r")

    for i in file:
        print(i)

    print(file)

    c =0

    for i in file:
        if c<=5:
            print(i)
        else:
            break
        c=c+1

    new = db["youtube2"]

    print(client.list_database_names())

    print(db.list_collection_names())

    for i in file:
        x = json.loads(i)
        new.insert_one(x)

        file = open("Adam Tech.json","r")

        for i in file:
            print(i)

        for i in open("Adam Tech.json","r"):
            x = json.loads(i)
            my_project.insert_one(x)

        print(my_project)

        for i in my_project.find():
            print(i)

# Connect to MySQL
def create_tables():

    import mysql.connector
    import pymysql

# Connect to MySQL
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        password='',)

    print(mydb)
    mycursor = mydb.cursor(buffered=True)

    mycursor.execute("show databases")
    for x in mycursor:
        print(x)

    mycursor = mydb.cursor()

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

    mydb.commit()

# Close the cursor and the connection
    mycursor.close()
    mydb.close()

    return True

def store_information_in_sql():

    # Connect to MySQL
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        password='',
        database='youtube'
    )
    
    mycursor = mydb.cursor()

    client = MongoClient("mongodb+srv://deepikarukmani:Titi@cluster0.ichg4lz.mongodb.net/?retryWrites=true&w=majority")
    db=client.youtube
    output=db.project
    new = db["youtube"]

    document_names=[]
        
    for i in my_project.find():
        document_names.append(i)

    mongodata = pd.DataFrame(document_names)

    channel_file = mongodata['ch_id']
    video_file = mongodata['video_info']
    comments = mongodata['comments']
    
    channel_dat = []
    
    for i in range(len(channel_file)):
        channel_dataframe = channel_file[i]
        channel_dat.append(channel_dataframe)
    channel_dataframe = pd.DataFrame(channel_dat)
    
    channel_id_playlist_id = channel_dataframe[['channel_id', 'playlist_id']]

    video_dat = []
    for i in range(len(video_file)):
        all_videos = video_file[i]
        video_dat.extend(all_videos)
    video_info = pd.DataFrame(video_dat)
    
    comment_dat = []
    for i in range(len(comments)):
        all_comments = comments[i]
        comment_dat.extend(all_comments)
    comment_data = pd.DataFrame(comment_dat)
    
    # Inserting channel details
    insert_channeldetails = '''INSERT IGNORE INTO channel_data (channel_name, channel_id, subscribers, views_count, total_videos, playlist_id,channel_description) VALUES (%s, %s, %s, %s, %s, %s, %s)'''
    values = channel_dataframe.values.tolist()
    mycursor.executemany(insert_channeldetails, values)
    

    # Inserting video details
    insert_videodetails = '''INSERT IGNORE INTO video_details (title, video_id, channel_id, published_date, video_description, views, likes, comments, time_duration)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
'''
    values = video_info.values.tolist()
    mycursor.executemany(insert_videodetails, values)

    # Inserting comment details
    insert_commentdetails = '''INSERT IGNORE INTO comment_details (comment_id, comment_text, comment_author, comment_published_at, video_id) VALUES (%s, %s, %s, %s, %s)'''
    values = comment_data.values.tolist()
    mycursor.executemany(insert_commentdetails, values)
    
    mydb.commit()
    
    # Close the cursor and the connection
    mycursor.close()
    mydb.close()

menu = ["Channel information", "SQL Data Warehouse","Channel queries"]

choice = st.sidebar.selectbox("Select an option", menu)
if choice=='Channel information':
    st.title("Welcome to the YouTube Data Warehousing App")
    channel=st.text_input("Enter channel_id")
    submit=st.button('submit')

    if submit:
        c = get_channel_stats(youtube, channel_id)
        st.dataframe(c)
        playlist_id = c[0]['playlist_id']
        v=get_video_ids(youtube,playlist_id)
        # vds=[]
        # for i in v:
        #     vds.append(i[0])
        # st.write(vds)
        vd= get_video_details(youtube, v)
        #st.dataframe(vd)
        cd= get_comment_details(youtube,v)
        #st.dataframe(cd)
        all_data_info=get_all_dada(youtube, channel,v)
        st.write(all_data_info)
        st.header(':blue[Data Collection]')
        store_information_in_mongodb(all_data_info)
        st.write('successfull inserted to mongodb')

def project_sql():
    if choice=="SQL Data Warehouse":
        import_to_sql=st.button('Import_to_SQL')
        st.write("click the button to import data")
        if import_to_sql:
            store_information_in_sql()
            st.write('inserted succcesfully')
            # st.experimental_rerun()


def main():
    if choice=="Channel queries":
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
        clicked4 = st.button("MYSQL")

        if clicked4:
            mysql_connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            database='youtube')
            
            mycursor = mysql_connection.cursor()

            mycursor.execute("SHOW DATABASES")
            databases = mycursor.fetchall() 
            mycursor.execute("USE youtube")

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

         