from googleapiclient.discovery import build   #This function is used to create an API client for interacting with Google services.
import pymongo
import pandas as pd
import psycopg2
import streamlit as st
from streamlit_option_menu import option_menu

from psycopg2.extensions import register_adapter, AsIs
import json

def adapt_dict(dict_var):
    return AsIs("'" + json.dumps(dict_var) + "'")

register_adapter(dict, adapt_dict)

#API key connector
def api_connect():
    api_id="AIzaSyCSYSE6sCOab__Hthi_YXNIubtItG6Q2O4"   #which is like a password that allows access to the YouTube API.
    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=api_id)
    return youtube  #Once the door opens, we create a friendly guide named youtube who knows the vault inside out.

youtube=api_connect()   #Finally, you run the api_connect() function like inserting your own key, creating your own youtube guide.

#Get channel information

def get_channel_info(channel_id):      #collecting information about a specific channel.
        request=youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=channel_id)
        response=request.execute()    #This line sends our request to YouTube and gets back a response containing the gathered clues.

        for i in response['items']:
                data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i["statistics"]["subscriberCount"],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
        return data

#Get Videos ids

def get_videos_id(channel_details):     #creating a list of videos from a channel.
    video_ids=[]
    response=youtube.channels().list(id=channel_details,part="contentDetails").execute()
    playlist_Id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(part="snippet",
                                            playlistId=playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1["items"])):
            video_ids.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])
        next_page_token=response1.get("nextPageToken")

        if next_page_token is None:
            break
    return video_ids


#get video information
def get_video_info(Video_ids):
    Video_data=[]
    for video_id in Video_ids: #collecting a video details separate 
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id)
        response=request.execute()

        for item in response["items"]: #collecting a specified video details info
            data=dict(Channel_Name=item["snippet"]["channelTitle"],
                    Channel_id=item["snippet"]["channelId"],
                    Video_Id=item["id"],
                    Title=item["snippet"]["title"],
                    Tages=item["snippet"].get("tags"),
                    Thumbnails=item["snippet"]["thumbnails"],
                    Description=item["snippet"].get("description"),
                    Published=item["snippet"]["publishedAt"],
                    Duration=item["contentDetails"]["duration"],
                    Views=item["statistics"].get("viewCount"),
                    Likes=item["statistics"].get("likeCount"),
                    Comments=item["statistics"].get("commentCount"),
                    Favorite=item["statistics"].get("favoriteCount"),
                    Contentdetails=item["contentDetails"]["definition"],
                    Caption_Status=item["contentDetails"]["caption"])
            Video_data.append(data)
    return Video_data       


#Get Comment information

def get_comment_info(Video_ids):
    Comment_data=[]
    try:
        for video_id in Video_ids: #collecting a comment details separate 
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50)
            response=request.execute()

            for item in response["items"]: #specified comment details info         
                data=dict(video_id=item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                        comment_id=item["snippet"]["topLevelComment"]["id"],
                        comment_text=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                        comment_author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                        comment_published=item["snippet"]["topLevelComment"]["snippet"]["updatedAt"])
                
                Comment_data.append(data)
    except:
        pass
    
    return Comment_data

#Get Playlist details

def get_playlist_info(channel_id):
    next_page_token=None
    playlist_data=[]

    while True:
        request=youtube.playlists().list(part="snippet,contentDetails",
                                        channelId=channel_id,
                                        maxResults=50,
                                        pageToken=next_page_token)
        response=request.execute()

        for item in response["items"]:
            data=dict(Playlist_id=item["id"],
                        Playlist_name=item["snippet"]["title"],
                        Channel_id=item["snippet"]["channelId"],
                        Channel_name=item["snippet"]["channelTitle"],
                        PublishedAt=item["snippet"]["publishedAt"],
                        Video_count=item["contentDetails"]["itemCount"])
                
            playlist_data.append(data)
        next_page_token=response.get("nextPageToken")
        if next_page_token is None:
            break
    return playlist_data

#Upload to mongoDB
client=pymongo.MongoClient("mongodb+srv://sumankumar:1234abcd@cluster0.8wmkdrj.mongodb.net/?retryWrites=true&w=majority")
db=client["youtube_data"]

def channel_details(channel_id):      #Upload channel details to mongoDB and call all the above codes
    ch_details=get_channel_info(channel_id)
    play_details=get_playlist_info(channel_id)
    vid_ids=get_videos_id(channel_id)
    vid_details=get_video_info(vid_ids)
    com_details=get_comment_info(vid_ids)
    

    collection1=db["channel_details"]
    collection1.insert_one({"Channel_information":ch_details,"Playlist_information":play_details,
                            "Video_information":vid_details,"Comment_information":com_details})
    return "upload completed successfully"

#Table creation for channels

def channels_table():
    mydb=psycopg2.connect(host="localhost",    
                        user= "postgres",
                        password="Sugu1234",
                        database="youtube_data",
                        port="5432")  #Connecting to SQL server 
    cursor=mydb.cursor()

    drop_query='''drop table if exists channels'''    #This command is help for drop the table 
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(100) primary key,
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_Videos bigint,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(100))''' #Creating tables in SQL and set primary key beacause to avoid dublicate 
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("channel table already created")


    ch_list=[]
    db=client["youtube_data"]
    collection1=db["channel_details"]

    for ch_data in collection1.find({},{"_id":0,"Channel_information":1}): #dataframe extraction from mongoDB
        ch_list.append(ch_data["Channel_information"])

    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows(): #it helps to iterrate the rows into index wise to make more visible 
        insert_query='''insert into Channels(Channel_Name,     
                                            Channel_Id,
                                            Subscribers,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)
                                            
                                            Values(%s,%s,%s,%s,%s,%s,%s)'''   #Here we give column variables of our data table
        Values=(row["Channel_Name"],
                row["Channel_Id"],
                row["Subscribers"],
                row["Views"],
                row["Total_Videos"],
                row["Channel_Description"],
                row["Playlist_Id"]) #Here we give variables to get row wise data in our dataframe
        try:
            cursor.execute(insert_query,Values)
            mydb.commit()
        except:
            print("Channels values are already inserted")

#Table creation for playlists

def playlists_table():
    mydb=psycopg2.connect(host="localhost",    
                    user= "postgres",
                    password="Sugu1234",
                    database="youtube_data",
                    port="5432")  #Connecting to SQL server 
    cursor=mydb.cursor()

    drop_query='''drop table if exists playlists'''    #This command is help for drop the table 
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists playlists(Playlist_id varchar(100) primary key,
                                                            Playlist_name varchar(100),
                                                            Channel_id varchar(100),
                                                            Channel_name varchar(100),
                                                            PublishedAt timestamp,
                                                            Video_count int)''' #Creating tables in SQL and set primary key beacause to avoid dublicate 

        cursor.execute(create_query)
        mydb.commit()
    except:
        print("playlists values are already created")

    play_list=[]
    db=client["youtube_data"]
    collection1=db["channel_details"]

    for play_data in collection1.find({},{"_id":0,"Playlist_information":1}): #dataframe extraction from mongoDB
            for i in range(len(play_data["Playlist_information"])):
                play_list.append(play_data["Playlist_information"][i])

    df1=pd.DataFrame(play_list)

    for index,row in df1.iterrows(): #it helps to iterrate the rows into index wise to make more visible 
            insert_query='''insert into playlists(Playlist_id,     
                                                Playlist_name,
                                                Channel_id,
                                                Channel_name,
                                                PublishedAt,
                                                Video_count)
                                            
                                                Values(%s,%s,%s,%s,%s,%s)'''   #Here we give column variables of our data table
            Values=(row["Playlist_id"],
                    row["Playlist_name"],
                    row["Channel_id"],
                    row["Channel_name"],
                    row["PublishedAt"],
                    row["Video_count"]) #Here we give variables to get row wise data in our dataframe
            try:
                cursor.execute(insert_query,Values)
                mydb.commit()
            except:
                print("playlists values are already inserted")

#Table creation for videos 

def videos_table():
    mydb=psycopg2.connect(host="localhost",    
                    user= "postgres",
                    password="Sugu1234",
                    database="youtube_data",
                    port="5432")  #Connecting to SQL server 
    cursor=mydb.cursor()

    drop_query='''drop table if exists videos'''    #This command is help for drop the table 
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                            Channel_id varchar(100),
                                                            Video_Id varchar(100),
                                                            Title text,
                                                            Tages text,
                                                            Thumbnails varchar(1000),
                                                            Description text,
                                                            Published timestamp,
                                                            Duration interval,
                                                            Views bigint,
                                                            Likes bigint,
                                                            Comments bigint,
                                                            Favorite bigint,
                                                            Contentdetails varchar(100),
                                                            Caption_Status varchar(100))''' #Creating tables in SQL and set primary key beacause to avoid dublicate 
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("videos values are already created")


    video_list=[]
    db=client["youtube_data"]
    collection1=db["channel_details"]

    for video_data in collection1.find({},{"_id":0,"Video_information":1}): #dataframe extraction from mongoDB
            for i in range(len(video_data["Video_information"])):
                    video_list.append(video_data["Video_information"][i])

    df2=pd.DataFrame(video_list)

    for index,row in df2.iterrows(): #it helps to iterrate the rows into index wise to make more visible 
                insert_query='''insert into videos(Channel_Name,
                                                    Channel_id,
                                                    Video_Id,
                                                    Title,
                                                    Tages,
                                                    Thumbnails,
                                                    Description,
                                                    Published,
                                                    Duration,
                                                    Views,
                                                    Likes,
                                                    Comments,
                                                    Favorite,
                                                    Contentdetails,
                                                    Caption_Status)                     
                                                    Values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''   #Here we give column variables of our data table

                Values=(row["Channel_Name"],
                        row["Channel_id"],
                        row["Video_Id"],
                        row["Title"],
                        row["Tages"],
                        row["Thumbnails"],
                        row["Description"],
                        row["Published"],
                        row["Duration"],
                        row["Views"],
                        row["Likes"],
                        row["Comments"],
                        row["Favorite"],
                        row["Contentdetails"],
                        row["Caption_Status"]) #Here we give variables to get row wise data in our dataframe
                try:
                    cursor.execute(insert_query,Values)
                    mydb.commit()
                except:
                    print("videos values are already inserted")


#table creation for comments

def comments_table():
     
    mydb=psycopg2.connect(host="localhost",    
                        user= "postgres",
                        password="Sugu1234",
                        database="youtube_data",
                        port="5432")  #Connecting to SQL server 
    cursor=mydb.cursor()

    drop_query='''drop table if exists comments'''    #This command is help for drop the table 
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists comments(video_id varchar(100),
                                                            comment_id varchar(100),
                                                            comment_text text,
                                                            comment_author varchar (200),
                                                            comment_published timestamp)''' #Creating tables in SQL and set primary key beacause to avoid dublicate 

        cursor.execute(create_query)
        mydb.commit()
    except:
        print("comment values are already created")

    comment_list=[]
    db=client["youtube_data"]
    collection1=db["channel_details"]

    for comment_data in collection1.find({},{"_id":0,"Comment_information":1}): #dataframe extraction from mongoDB
            for i in range(len(comment_data["Comment_information"])):
                comment_list.append(comment_data["Comment_information"][i])

    df3=pd.DataFrame(comment_list)

    for index,row in df3.iterrows(): #it helps to iterrate the rows into index wise to make more visible 
            insert_query='''insert into comments(video_id,     
                                                comment_id,
                                                comment_text,
                                                comment_author,
                                                comment_published)
                                            
                                                Values(%s,%s,%s,%s,%s)'''   #Here we give column variables of our data table
            Values=(row["video_id"],
                    row["comment_id"],
                    row["comment_text"],
                    row["comment_author"],
                    row["comment_published"]) #Here we give variables to get row wise data in our dataframe
            try:
                cursor.execute(insert_query,Values)
                mydb.commit()
            except:
                print("comments values are already inserted")

def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()

    return "tables created successfully"

def show_channels_table():

    ch_list=[]
    db=client["youtube_data"]
    collection1=db["channel_details"]

    for ch_data in collection1.find({},{"_id":0,"Channel_information":1}): #dataframe extraction from mongoDB
        ch_list.append(ch_data["Channel_information"])

    df=st.dataframe(ch_list)

    return df

def show_playlists_table():
      
    play_list=[]
    db=client["youtube_data"]
    collection1=db["channel_details"]

    for play_data in collection1.find({},{"_id":0,"Playlist_information":1}): #dataframe extraction from mongoDB
            for i in range(len(play_data["Playlist_information"])):
                play_list.append(play_data["Playlist_information"][i])

    df1=st.dataframe(play_list)

    return df1

def show_videos_table():
        
    video_list=[]
    db=client["youtube_data"]
    collection1=db["channel_details"]

    for video_data in collection1.find({},{"_id":0,"Video_information":1}): #dataframe extraction from mongoDB
            for i in range(len(video_data["Video_information"])):
                    video_list.append(video_data["Video_information"][i])

    df2=st.dataframe(video_list)
    return df2

def show_comments_table():
    
    comment_list=[]
    db=client["youtube_data"]
    collection1=db["channel_details"]
    for comment_data in collection1.find({},{"_id":0,"Comment_information":1}): #dataframe extraction from mongoDB
            for i in range(len(comment_data["Comment_information"])):
                comment_list.append(comment_data["Comment_information"][i])

    df3=st.dataframe(comment_list)

    return df3

#streamlit part

st.set_page_config(
    page_title="YouTube Data",
    page_icon="▶",)

with st.sidebar:                                   #Creating sidebars and options
    selected= option_menu(
        menu_title="YouTube Data Harvasting and WareHousing",
        options=['Home','Project Learnings','Contact'],
        icons=['house','book','envelope'],
        menu_icon='cast',
        default_index=0,
        )
    
if selected == 'Home':
    st.header(f'Welcome!')

    channel_id=st.text_input("Enter the Channel ID")   

    if st.button("Collect and Store Data in MongoDB"):                 #This button is help to transfer the data into MongoDB 
        ch_ids=[]
        db=client["youtube_data"]
        collection1=db["channel_details"]

        for ch_data in collection1.find({},{"_id":0,"Channel_information":1}):
            ch_ids.append(ch_data["Channel_information"]["Channel_Id"])

        if channel_id in ch_ids:                            # To find out the Duplicate channel id (already exists in our database)
            st.success("Channel details already exists")
        else:
            insert=channel_details(channel_id)
            st.success(insert)
        
    if st.button("Migrate to SQL"):
        Table=tables()
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
                        user= "postgres",
                        password="Sugu1234",
                        database="youtube_data",
                        port="5432")  #Connecting to SQL server 
    cursor=mydb.cursor()

    ##########################

    # Query to retrieve distinct channel names
    query_get_channel_names = "SELECT DISTINCT channel_name FROM videos"
    cursor.execute(query_get_channel_names)
    channel_name_result = cursor.fetchall()
    channel_name = [row[0] for row in channel_name_result]

    # Add a special option for selecting all channels
    special_option = "All"
    channel_name_with_all = [special_option] + channel_name

    # Use a conditional statement to handle the selection
    selected_channel = st.selectbox("Select a Channel:", options=channel_name_with_all)

    question = st.selectbox("Select a Question:", options=[
                                                            "1.All the videos and channels",
                                                            "2.Channels with most number of videos",
                                                            "3.10 most viewed videos",
                                                            "4.Comments in each videos",
                                                            "5.Videos with highest likes",
                                                            "6.Likes of all videos",
                                                            "7.Views of each channel",
                                                            "8.Videos published in the year of 2022",
                                                            "9.Average duration of all videos in each channel",
                                                            "10.Videos with highest number of comments"
                                                        ])

    if question == "1.All the videos and channels":
    # Adjust the WHERE clause to include all channels if selected_channel is "All"
        if selected_channel == "All":
            query1 = '''SELECT title AS videos, channel_name AS channelname 
                        FROM videos'''
        else:
            query1 = f'''SELECT title AS videos, channel_name AS channelname 
                        FROM videos
                        WHERE channel_name = '{selected_channel}' '''

        cursor.execute(query1)
        table1 = cursor.fetchall()
        df = pd.DataFrame(table1, columns=["videos title", "channel name"])
        st.write(df)

    elif question == "2.Channels with most number of videos":
        # Adjust the WHERE clause to include all channels if selected_channel is "All"
        if selected_channel == "All":
            query2 = '''SELECT channel_name AS channelname, total_videos AS no_videos 
                        FROM channels
                        ORDER BY total_videos DESC'''
        else:
            query2 = f'''SELECT channel_name AS channelname, total_videos AS no_videos 
                        FROM channels
                        WHERE channel_name = '{selected_channel}'
                        ORDER BY total_videos DESC'''

        cursor.execute(query2)
        table2 = cursor.fetchall()
        df2 = pd.DataFrame(table2, columns=["channel name", "No of videos"])
        st.write(df2)


    elif question == "3.10 most viewed videos":
        # Adjust the WHERE clause to include all channels if selected_channel is "All"
        if selected_channel == "All":
            query3 = '''SELECT views AS views, channel_name AS channelname, title AS videotitle 
                        FROM videos
                        WHERE views IS NOT NULL
                        ORDER BY views DESC
                        LIMIT 10'''
        else:
            query3 = f'''SELECT views AS views, channel_name AS channelname, title AS videotitle 
                        FROM videos
                        WHERE views IS NOT NULL AND channel_name = '{selected_channel}'
                        ORDER BY views DESC
                        LIMIT 10'''

        cursor.execute(query3)
        table3 = cursor.fetchall()
        df3 = pd.DataFrame(table3, columns=["views", "channel name", "video title"])
        st.write(df3)

    elif question == "4.Comments in each videos":
        # Adjust the WHERE clause to include all channels if selected_channel is "All"
        if selected_channel == "All":
            query4 = '''SELECT comments AS No_comments, title AS videotitle 
                        FROM videos
                        WHERE comments IS NOT NULL'''
        else:
            query4 = f'''SELECT comments AS No_comments, title AS videotitle 
                        FROM videos
                        WHERE comments IS NOT NULL AND channel_name = '{selected_channel}' '''

        cursor.execute(query4)
        table4 = cursor.fetchall()
        df4 = pd.DataFrame(table4, columns=["No of comments", "video title"])
        st.write(df4)

    elif question == "5.Videos with highest likes":
        # Adjust the WHERE clause to include all channels if selected_channel is "All"
        if selected_channel == "All":
            query5 = '''SELECT channel_name AS channelname, title AS videotitle, likes AS No_likes 
                        FROM videos
                        WHERE likes IS NOT NULL
                        ORDER BY likes DESC'''
        else:
            query5 = f'''SELECT channel_name AS channelname, title AS videotitle, likes AS No_likes 
                        FROM videos
                        WHERE likes IS NOT NULL AND channel_name = '{selected_channel}'
                        ORDER BY likes DESC'''

        cursor.execute(query5)
        table5 = cursor.fetchall()
        df5 = pd.DataFrame(table5, columns=["channel name", "video title", "No of likes"])
        st.write(df5)


    elif question == "6.Likes of all videos":
    # Adjust the WHERE clause to include all channels if selected_channel is "All"
        if selected_channel == "All":
            query6 = '''SELECT likes AS likescount, title AS videotitle 
                        FROM videos
                        WHERE likes IS NOT NULL
                        ORDER BY likes DESC'''
        else:
            query6 = f'''SELECT likes AS likescount, title AS videotitle 
                        FROM videos
                        WHERE likes IS NOT NULL AND channel_name = '{selected_channel}'
                        ORDER BY likes DESC'''

        cursor.execute(query6)
        table6 = cursor.fetchall()
        df6 = pd.DataFrame(table6, columns=["likes count", "video title"])
        st.write(df6)


    elif question == "7.Views of each channel":
    # Adjust the WHERE clause to include all channels if selected_channel is "All"
        if selected_channel == "All":
            query7 = '''SELECT channel_name AS channename, views AS viewscount 
                        FROM channels
                        WHERE views IS NOT NULL
                        ORDER BY views'''
        else:
            query7 = f'''SELECT channel_name AS channename, views AS viewscount 
                        FROM channels
                        WHERE views IS NOT NULL AND channel_name = '{selected_channel}'
                        ORDER BY views'''

        cursor.execute(query7)
        table7 = cursor.fetchall()
        df7 = pd.DataFrame(table7, columns=["channel name", "Total views"])
        st.write(df7)

    elif question == "8.Videos published in the year of 2022":
        # Adjust the WHERE clause to include all channels if selected_channel is "All"
        if selected_channel == "All":
            query8 = '''SELECT title AS videotitle, published AS published, channel_name AS channelname 
                        FROM videos
                        WHERE EXTRACT(YEAR FROM published) = 2022'''
        else:
            query8 = f'''SELECT title AS videotitle, published AS published, channel_name AS channelname 
                        FROM videos
                        WHERE EXTRACT(YEAR FROM published) = 2022 AND channel_name = '{selected_channel}' '''

        cursor.execute(query8)
        table8 = cursor.fetchall()
        df8 = pd.DataFrame(table8, columns=["video title", "published date", "channel name"])
        st.write(df8)


    elif question == "9.Average duration of all videos in each channel":
    # Adjust the WHERE clause to include all channels if selected_channel is "All"
        if selected_channel == "All":
            query9 = '''SELECT channel_name AS channelname, AVG(duration) AS averageduration 
                        FROM videos
                        GROUP BY channel_name'''
        else:
            query9 = f'''SELECT channel_name AS channelname, AVG(duration) AS averageduration 
                        FROM videos
                        WHERE channel_name = '{selected_channel}'
                        GROUP BY channel_name'''

        cursor.execute(query9)
        table9 = cursor.fetchall()
        df9 = pd.DataFrame(table9, columns=["channel name", "Average duration"])

        table9 = []

        for index, row in df9.iterrows():
            channel_title = row["channel name"]
            average_duration = row["Average duration"]
            average_duration_str = str(average_duration)

            table9.append(dict(channel=channel_title, averageduration=average_duration_str))

        df1 = pd.DataFrame(table9)
        st.write(df1)

    elif question == "10.Videos with highest number of comments":
        # Adjust the WHERE clause to include all channels if selected_channel is "All"
        if selected_channel == "All":
            query10 = '''SELECT title AS videotitle, channel_name AS channelname, comments AS comments 
                        FROM videos
                        WHERE comments IS NOT NULL
                        ORDER BY comments DESC'''
        else:
            query10 = f'''SELECT title AS videotitle, channel_name AS channelname, comments AS comments 
                        FROM videos
                        WHERE comments IS NOT NULL AND channel_name = '{selected_channel}'
                        ORDER BY comments DESC'''

        cursor.execute(query10)
        mydb.commit()
        table10 = cursor.fetchall()
        df10 = pd.DataFrame(table10, columns=["channel name", "video title", "No of comments"])
        st.write(df10)


if selected == 'Project Learnings':
    st.header("Python Scripting")
    st.header("Data Collection")
    st.header("MongoDB")
    st.header("SQL")
    st.header("API Integration")
    st.header("Data Management using MongoDB and SQL")

if selected == 'Contact':
    st.header("https://github.com/suman98kumar")
    st.header("My Email - sumankumarmba2020@gmail.com")
    st.header("My Number - 9943042892")
    st.caption("Thankyou")
    
