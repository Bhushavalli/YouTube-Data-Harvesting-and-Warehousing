from googleapiclient.discovery import build
import streamlit as st 
from streamlit_option_menu import option_menu
import pandas as pd
import mysql.connector
import numpy as np
import json
from datetime import datetime
import re


st.sidebar.title('Youtube Data Harvesting and Warehousing')


# Creating a side navigation bar in streamlit:
with st.sidebar:
    selected = option_menu("Content", ["Home","Data Collection & Migration","SQL Queries"],
                           icons = ['house', 'cloud-upload', 'database'],
                           menu_icon='menu-up',
                           default_index=0,
                           orientation="vertical")

if selected == "Home":
    # Title
    st.title(':blue[YouTube Data Harvesting and Warehousing using SQL and STREAMLIT]')
    st.markdown("### :white_check_mark: Domain :") 
    st.write('Social Media')
    st.markdown("### :white_check_mark: Project overview :")
    st.markdown(''' Collecting information from youtube channel ID using Python to display 
                channel, video and comments data retrieved from YouTube with the help of the YouTube API, 
                stored in MySQL and then displayed on the streamlit.''')
    st.markdown("### :white_check_mark: Skills take away From This Project :") 
    st.markdown("API integration, Python, MySQL, Streamlit")

elif selected == "Data Collection & Migration":
    st.markdown('### :blue[Data extraction area :]')
    st.markdown('''**collecting the specific channel, video and comment data**
                of the channels that the user provides as input and migrate them to our dedicated MySQL database.''')
    st.markdown('''
                - Step 1: **Input Channel ID :** Insert the channel ID of the desired channel in the text box below. 
                - Step 2: **Extract :** Click on the **Fetch data** button below the box for the data extraction from YouTube for the channel.
                ''')
    
    def Api_key_connection():
        Api_id = "AIzaSyBydbhtXU9z_9lI1H9HWWkqYdhHMCpyQDU"
        Api_service_name = "youtube"
        Api_version = "v3"
        youtube = build(Api_service_name,Api_version, developerKey = Api_id)
        return youtube
    youtube = Api_key_connection()


    # Text input for YouTube channel ID
    channel_id = st.text_input("Enter the Channel ID:")

    # channel_information:
    def get_youtubechannel_info(channel_id):
        if channel_id:
            request = youtube.channels().list(
                                                part  = "snippet,contentDetails,statistics",
                                                id = channel_id                
                                            )
                                                                            
            response = request.execute()

            for i in response["items"]:
                youtubedata = dict( Channel_ID = i['id'],
                                    Channel_Name =i["snippet"]['title'],
                                    Subscription_Count = i['statistics']['subscriberCount'],
                                    Channel_Views =i['statistics']['viewCount'],
                                    Total_videos = i['statistics']['videoCount'],
                                    Channel_Description = i['snippet']['description'], 
                                    Playlist_id = i['contentDetails']['relatedPlaylists']['uploads'])
                                                                                            
                return youtubedata
        if not channel_id:
            st.warning("Please enter a valid YouTube Channel ID before proceeding.")
    
    channel_data = get_youtubechannel_info(channel_id)
    if channel_data:
        channel_df = pd.DataFrame(channel_data,index=[0]) 
        st.dataframe(channel_df)

    #To get multiple video ids

    def multi_video_id(channel_id):
        if channel_id:
            video_id = []
            response = youtube.channels().list(id = channel_id,
                                                    part = 'contentDetails').execute()
            Playlist_id =response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
            nextpage = None
            while True:
                addresponse = youtube.playlistItems().list(
                                                part = 'snippet', 
                                                playlistId = Playlist_id,maxResults = 50,pageToken = nextpage).execute()

                for i in range(len(addresponse['items'])):   
                        video_id.append(addresponse['items'][i]['snippet']['resourceId']['videoId'])
                nextpage =  addresponse.get('nextPageToken')  
                if nextpage is None:
                        break            
            return video_id

    multiplevideo = multi_video_id(channel_id)
    st.dataframe(multiplevideo)

    #To get information about video ids

    def videoinformation(video_info):
        if video_info:    
            video_data = []
            for vidID in video_info:
                request = youtube.videos().list(
                        part = "snippet, contentDetails, statistics",
                        id = vidID
                )
                response = request.execute()
                
                for vidinfo in response['items']:
                    youtubedata = dict(Channel_Id = vidinfo['snippet']['channelId'],
                                    Channel_Name = vidinfo['snippet']['channelTitle'],
                                    Video_Id = vidinfo['id'],
                                    Video_Name = vidinfo['snippet']['title'],
                                    View_Counts = vidinfo['statistics']['viewCount'],
                                    Like_Count = vidinfo.get('likeCount'),
                                    PublishedAt = vidinfo['snippet']['publishedAt'],
                                    Duration = vidinfo['contentDetails']['duration'],
                                    Thumbnail = vidinfo['snippet']['thumbnails']['default']['url'],
                                    Description = vidinfo['snippet']['description'],
                                    Caption_Status = vidinfo['contentDetails']['caption'])
                                
                    video_data.append(youtubedata)
            return video_data
    videos_data = videoinformation(multiplevideo)
    video_df = pd.DataFrame(videos_data)
    st.dataframe(video_df)

    # To get video comment information

    def get_videocomments(comment_info):
        if comment_info:    
            videocomment_data = []
            try:
                for videocomments in comment_info:
                    request = youtube.commentThreads().list(
                        part = 'snippet',
                        videoId = videocomments,
                        maxResults = 50)
                    response = request.execute()
                
                    for vidinfo in response.get('items',[]):
                        
                        youtubedata = dict(Comment_Id = vidinfo['snippet']['topLevelComment']['id'],
                                        Video_Id = vidinfo['snippet']['videoId'],
                                        Comment_Text = vidinfo['snippet']['topLevelComment']['snippet']['textDisplay'],
                                        Comment_Author = vidinfo['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                        Comment_PublishedAt = vidinfo['snippet']['topLevelComment']['snippet']['publishedAt']
                                        )
                        
                        videocomment_data.append(youtubedata)
            except:
                pass
            return(videocomment_data)
    comment_data = get_videocomments(multiplevideo)
    comment_df = pd.DataFrame(comment_data)
    st.dataframe(comment_df)

# Mysql connection:
    mydb = mysql.connector.connect(host='127.0.0.1',
                                user='root',
                                database = 'youtube_project', 
                                passwd='1234')
    cursor = mydb.cursor(buffered=True)
    cursor.execute('create database if not exists youtube_project')
    cursor.execute('use youtube_project')

# Creating Tables:
    def create_table(cursor):
            cursor = mydb.cursor()
            channel_table = ''' create table channels(Channel_ID varchar(255) primary key,
                                                    Channel_Name varchar(255),
                                                    Subscription_Count INT,
                                                    Channel_Views INT,
                                                    Total_videos INT,
                                                    Channel_Description TEXT,
                                                    Playlist_id varchar(255)
                                                    )'''
            
            video_table = ''' create table if not exists videos1(Channel_Id varchar(100),
                                                                Channel_Name varchar(100),
                                                                Video_Id varchar(30) primary key,
                                                                Video_Name varchar(150),
                                                                View_Counts INT,
                                                                Like_Count INT,
                                                                Duration varchar(50) 
                                                                PublishedAt varchar(255), 
                                                                Comment_Count INT
                                                                Thumbnail varchar(200),
                                                                Description text,
                                                                Caption_Status varchar(50)
                                                                )''' 
            comment_table = ''' create table if not exists comment(Comment_Id varchar(255),
                                                                Video_Id varchar(255),
                                                                Comment_Text TEXT,
                                                                Comment_Author varchar(100),
                                                                Comment_PublishedAt varchar(100)
                                                                )'''
            cursor.execute(channel_table)
            cursor.execute(video_table)
            cursor.execute(comment_table)
            mydb.commit()
            mydb.close()

# Mysql data insertion:
    def insert_channel_data(data_list):
                    
        cursor = mydb.cursor()
        for index,row in channel_df.iterrows():
                        insert_query = '''insert into channels(Channel_ID,
                                                Channel_Name, 
                                                Subscription_Count, 
                                                Channel_Views, 
                                                Total_videos, 
                                                Channel_Description,
                                                Playlist_id) 
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
                        values = (row['Channel_ID'],
                                row['Channel_Name'],
                                row['Subscription_Count'],
                                row['Channel_Views'],
                                row['Total_videos'],
                                row['Channel_Description'],
                                row['Playlist_id'])
            
        cursor.execute(insert_query, values)
        mydb.commit()

    # Example data list of dictionaries
    # Convert the single dictionary to a list containing that dictionary
    channel_data_insert = [channel_data]
            
    def insert_video_data(cursor):    
                cursor = mydb.cursor()
                
                for index,row in video_df.iterrows():
                    insert_query = '''insert ignore into videos(Channel_Id,Channel_Name,Video_Id,Video_Name,View_Counts,Like_Count,Duration,PublishedAt,Comment_Count,Thumbnail,Description,Caption_Status) 
                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                    values = (row['Channel_Id'],
                            row['Channel_Name'],
                            row['Video_Id'],
                            row['Video_Name'],
                            row['View_Counts'],
                            row['Like_Count'],
                            row['Duration'],
                            row['PublishedAt'],
                            row['Comment_Count'],
                            row['Thumbnail'],
                            row['Description'],
                            row['Caption_Status'])
            
                    cursor.execute(insert_query, values)
                    mydb.commit()


    def insert_comment_data(cursor, comment_data):
                cursor = mydb.cursor()

                for index,row in comment_df.iterrows():
                    insert_query = '''insert into comment(Comment_Id,Video_Id,Comment_Text,Comment_Author,Comment_PublishedAt
                                                            ) 
                                                        values(%s,%s,%s,%s,%s)'''
                    values = (row['Comment_Id'],
                                row['Video_Id'],
                                row['Comment_Text'],
                                row['Comment_Author'],
                                row['Comment_PublishedAt'],
                                )
        
                    cursor.execute(insert_query, values)
                    mydb.commit()

    # Function to check if data already exists in MySQL
    def check_data_exists(channelid):
            # Placeholder function to check if data already exists in MySQL
        return False

    def migrate_to_sql(channel_id):
        cursor = mydb.cursor()
        if check_data_exists(channel_id):
            st.error("Data already present in MySQL.")
        else:
            # Create tables in MySQL
            create_table(cursor)
                
            # Fetch data from YouTube API
            channel_data = get_youtubechannel_info(channel_id)
            multiplevideo = multi_video_id(channel_id)
            videos_data = videoinformation(multiplevideo)
            comment_data = get_videocomments(multiplevideo)
                
            # Insert data into MySQL
            insert_channel_data(channel_data_insert)
            insert_video_data(cursor, videos_data)
            insert_comment_data(cursor, comment_data)
                
            st.success("Transfer successful.")
        

    # Check if a channel ID is provided
        if channel_id:
            if st.button("Fetch Channel Details"):
                channel_data = get_youtubechannel_info(channel_id)
                if channel_data:
                    st.write(f"Title of Channel: {channel_data['Channel_Name']}")
                    st.write(f"Channel Description: {channel_data['Channel_Description']}")
                    st.write(f"Playlist ID with video uploads: {channel_data['Playlist_id']}")
                    st.write(f"Channel Viewcount: {channel_data['Channel_Views']}")
                    st.write(f"Channel SubscriberCount: {channel_data['Subscription_Count']}")
                else:
                    st.error("Channel details not found.")
            
            if st.button("Migrate to MySQL"):
                migrate_to_sql(channel_id)
        

#SQL QUERIES Page:
elif selected == "SQL Queries":
    st.header(':blue[Queries and Results ]')
    st.write('''In this page, we have the results to the queries that have been asked of us based on the channel data we have collected and migrated to SQL ''')

    
    mydb = mysql.connector.connect(host='127.0.0.1',
                               user='root',
                               database = 'youtube_project', 
                               passwd='1234')
    cursor = mydb.cursor()
                
    # Function to display SQL queries and their solutions
    def predefined_queries():
        st.title("SQL queries")
            
        #SQL queries and their descriptions
        question = st.selectbox('Select your Question from dropdown :',
                                    ['1. What are the names of all the videos and their corresponding channels?',
                                    '2. Which channels have the most number of videos, and how many videos do they have?',
                                    '3. What are the top 10 most viewed videos and their respective channels?',
                                    '4. How many comments were made on each video, and what are their corresponding video names?',
                                    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                    '8. What are the names of all the channels that have published videos in the year 2022?',
                                    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
                                    

        if question == '1. What are the names of all the videos and their corresponding channels?':
            query1 = '''SELECT Channel_Name, Video_Name FROM videos ORDER BY Channel_Name;'''
            cursor.execute(query1)
            row1=cursor.fetchall()
            df1=pd.DataFrame(row1,columns=['Channel Name', 'Video Name'])
            st.write(df1) 
        
        elif question == '2. Which channels have the most number of videos, and how many videos do they have?':
            query2 = """SELECT Channel_Name as Chan_name, Total_videos as Tot_videos FROM channels ORDER BY Total_videos DESC"""
            cursor.execute(query2)
            row2=cursor.fetchall()
            df2 = pd.DataFrame(row2,columns=['Chan_name','Tot_videos'])
            st.write(df2) 

        elif question == '3. What are the top 10 most viewed videos and their respective channels?':
            query3 = '''SELECT Video_Id, Video_Name, View_Counts, Channel_Name FROM videos ORDER BY View_Counts DESC LIMIT 10;'''
            cursor.execute(query3)
            row3=cursor.fetchall()
            df3 = pd.DataFrame(row3,columns=['Video_Id','Video Name', 'View Counts','Channel Name'])
            st.write(df3) 

        elif question == '4. How many comments were made on each video, and what are their corresponding video names?':
            query4 = '''SELECT Video_Id, Video_Name, Comment_Count FROM videos ORDER BY Comment_Count DESC;'''
            cursor.execute(query4)
            row4=cursor.fetchall()
            df4 = pd.DataFrame(row4, columns=['Video ID','Video Name','Comment count'])
            st.write(df4)

        elif question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            query5 = '''SELECT Video_Id, Video_Name, Like_Count, Channel_Name FROM videos ORDER BY Like_Count DESC;'''
            cursor.execute(query5)
            row5=cursor.fetchall()
            df5 = pd.DataFrame(row5, columns=['Video ID','Video Name', 'Like count','Channel Name'])
            st.write(df5)

        elif question == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
            query6 = '''SELECT Video_Id, Video_Name, Like_Count FROM videos ORDER BY Like_Count DESC;"'''
            cursor.execute(query6)
            row6=cursor.fetchall()
            df6 = pd.DataFrame(row6, columns=['Video ID','Video Name','Like count'])
            st.write(df6)

        elif question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
            query7 = '''SELECT Channel_ID, Channel_Name, Channel_Views FROM channels ORDER BY Channel_Views DESC;'''
            cursor.execute(query7)
            row7=cursor.fetchall()
            df7 = pd.DataFrame(row7, columns=['Channel ID', 'Channel Name','Total number of views'])
            st.write(df7)

        elif question == '8. What are the names of all the channels that have published videos in the year 2022?':
            query8 = '''SELECT distinct(Channel_Name), YEAR(PublishedAt) FROM videos WHERE YEAR(PublishedAt) = 2022 ORDER BY Channel_Name;'''
            cursor.execute(query8)
            row8=cursor.fetchall()
            df8 = pd.DataFrame(row8, columns=['Channel Name', 'Video Published Year'])
            st.write(df8)

        elif question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            query9 = '''SELECT Channel_Id, Channel_Name, ROUND(AVG(Duration/60), 2) AS avg_duration 
                    FROM videos GROUP BY Channel_Id, Channel_Name ORDER BY ROUND(AVG(Duration/60), 2) DESC;''' 
            cursor.execute(query9)
            row9=cursor.fetchall()
            df9 = pd.DataFrame(row9, columns=['Channel ID','Channel Name', 'avg_duration'])
            st.write(df9)

        elif question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
            query10 = '''SELECT Channel_Name, Video_Name, Comment_Count FROM videos ORDER BY Comment_Count DESC;'''
            cursor.execute(query10)
            row10=cursor.fetchall()
            df10 = pd.DataFrame(row10, columns=['Channel Name', 'Video Name', 'Number of Comments'])
            st.write(df10)

        mydb.close()
    predefined_queries()
