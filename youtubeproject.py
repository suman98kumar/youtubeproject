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
def get_channel_info(channel_id):      
        #collecting information about a specific channel.
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
def get_videos_id(channel_id):     #creating a list of videos from a channel.
    video_ids=[]
    response=youtube.channels().list(id=channel_id,part="contentDetails").execute()
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

#Get video information
def get_video_info(Video_ids):
    Video_data=[]
    for video_id in Video_ids: #collecting a video details separate 
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id)
        response=request.execute()

        for item in response["items"]: #collecting a specified video details info
            data=dict(Channel_Name=item["snippet"]["channelTitle"],
                    Channel_Id=item["snippet"]["channelId"],
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
                data=dict(video_Id=item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                        Comment_Id=item["snippet"]["topLevelComment"]["id"],
                        Comment_Text=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                        Comment_Author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                        Comment_Published=item["snippet"]["topLevelComment"]["snippet"]["updatedAt"])
                
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
            data=dict(Playlist_Id=item["id"],
                        Playlist_Name=item["snippet"]["title"],
                        Channel_Id=item["snippet"]["channelId"],
                        Channel_Name=item["snippet"]["channelTitle"],
                        PublishedAt=item["snippet"]["publishedAt"],
                        Video_Count=item["contentDetails"]["itemCount"])
                
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


# MongoDB connection
client = pymongo.MongoClient("mongodb+srv://sumankumar:1234abcd@cluster0.8wmkdrj.mongodb.net/?retryWrites=true&w=majority")
db = client["youtube_data"]
collection1 = db["channel_details"]

# PostgreSQL connection
mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="Sugu1234",
    database="youtube_database",
    port="5432"
)
cursor = mydb.cursor()

# Function to create channels table
def create_channels_table():
    create_query = '''
        CREATE TABLE IF NOT EXISTS channels (
            Channel_Id VARCHAR(100) PRIMARY KEY,
            Channel_Name VARCHAR(100),
            Subscribers BIGINT,
            Views BIGINT,
            Total_Videos BIGINT,
            Channel_Description TEXT,
            Playlist_Id VARCHAR(100)
        )
    '''
    cursor.execute(create_query)
    mydb.commit()

# Function to create playlists table
def create_playlists_table():
    create_query = '''
        CREATE TABLE IF NOT EXISTS playlists (
            Playlist_Id VARCHAR(100) PRIMARY KEY,
            Playlist_Name VARCHAR(100),
            Channel_Id VARCHAR(100),
            Channel_Name VARCHAR(100),
            Video_Count BIGINT,  -- Add Video_Count column
            FOREIGN KEY (Channel_Id) REFERENCES channels(Channel_Id)
        )
    '''
    cursor.execute(create_query)
    mydb.commit()

# Function to create videos table
def create_videos_table():
    create_query = '''
        CREATE TABLE IF NOT EXISTS videos (
            Video_Id VARCHAR(100) PRIMARY KEY,
            Channel_Id VARCHAR(100),
            Title VARCHAR(255),
            Description TEXT,
            Published TIMESTAMP,
            Duration INTERVAL,
            Views BIGINT,
            Likes BIGINT,
            Comments BIGINT,
            Favorite BIGINT,
            Contentdetails VARCHAR(20),
            Caption_Status BOOLEAN,
            Channel_Name VARCHAR(100),
            Thumbnails VARCHAR(1000),
            Tages TEXT,
            FOREIGN KEY (Channel_Id) REFERENCES channels(Channel_Id)
        )
    '''
    cursor.execute(create_query)
    mydb.commit()

# Function to create comments table
def create_comments_table():
    create_query = '''
        CREATE TABLE IF NOT EXISTS comments (
            Comment_Id VARCHAR(100) PRIMARY KEY,
            Video_Id VARCHAR(100),
            Comment_Text TEXT,
            Comment_Author VARCHAR(100),
            Comment_Published TIMESTAMP,
            FOREIGN KEY (Video_Id) REFERENCES videos(Video_Id)
        )
    '''
    cursor.execute(create_query)
    mydb.commit()

def drop_tables():
    drop_queries = [
        'DROP TABLE IF EXISTS comments',
        'DROP TABLE IF EXISTS videos',
        'DROP TABLE IF EXISTS playlists',
        'DROP TABLE IF EXISTS channels'
    ]

    for query in drop_queries:
        cursor.execute(query)
        mydb.commit()

# Function to insert data into channels table
def insert_into_channels(channel_info):
    insert_query = '''
        INSERT INTO channels (
            Channel_Id,
            Channel_Name,
            Subscribers,
            Views,
            Total_Videos,
            Channel_Description,
            Playlist_Id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    '''
    values = (
        channel_info.get("Channel_Id", ""),
        channel_info.get("Channel_Name", ""),
        channel_info.get("Subscribers", 0),
        channel_info.get("Views", 0),
        channel_info.get("Total_Videos", 0),
        channel_info.get("Channel_Description", ""),
        channel_info.get("Playlist_Id", "")
    )
    cursor.execute(insert_query, values)
    mydb.commit()

# Function to insert data into playlists table
def insert_into_playlists(playlist_info, channel_id):
    playlist_id = playlist_info.get("Playlist_Id")
    if playlist_id:  # Only insert if playlist_id is not empty or None
        insert_query = '''
            INSERT INTO playlists (
                Playlist_Id,
                Playlist_Name,
                Channel_Id,
                Channel_Name,
                Video_Count
            ) VALUES (%s, %s, %s, %s, %s)
        '''
        values = (
            playlist_id,
            playlist_info.get("Playlist_Name", ""),
            channel_id,
            playlist_info.get("Channel_Name", ""),
            int(playlist_info.get("Video_Count", 0)),
        )
        cursor.execute(insert_query, values)
        mydb.commit()

# Function to insert data into videos table
def insert_into_videos(video_info, channel_id):
    insert_query = '''
        INSERT INTO videos (
            Video_Id,
            Channel_Id,
            Title,
            Description,
            Published,
            Duration,
            Views,
            Likes,
            Comments,
            Favorite,
            Contentdetails,
            Caption_Status,
            Channel_Name,
            Thumbnails,
            Tages
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    values = (
        video_info.get("Video_Id", ""),
        channel_id,
        video_info.get("Title", ""),
        video_info.get("Description", ""),
        video_info.get("Published", ""),
        video_info.get("Duration", ""),
        int(video_info.get("Views", 0)),
        int(video_info.get("Likes", 0)),
        int(video_info.get("Comments", 0)),
        int(video_info.get("Favorite", 0)),
        video_info.get("Contentdetails", ""),
        video_info.get("Caption_Status", False),
        video_info.get("Channel_Name", ""),
        video_info.get("Thumbnails", ""),
        video_info.get("Tages", "")
    )
    cursor.execute(insert_query, values)
    mydb.commit()

# Function to insert data into comments table
def insert_into_comments(comment_info, video_id):
    insert_query = '''
        INSERT INTO comments (
            Comment_Id,
            Video_Id,
            Comment_Text,
            Comment_Author,
            Comment_Published
        ) VALUES (%s, %s, %s, %s, %s)
    '''
    values = (
        comment_info.get("comment_id", ""),
        video_id,
        comment_info.get("comment_text", ""),
        comment_info.get("comment_author", ""),
        comment_info.get("comment_published", "")
    )
    cursor.execute(insert_query, values)
    mydb.commit()

# Function to migrate data to SQL for channels, playlists, videos, and comments tables
def migrate_to_sql(channel_data):

    # Drop existing tables
    drop_tables()

    create_channels_table()
    create_playlists_table()
    create_videos_table()
    create_comments_table()

    channel_info = channel_data.get("Channel_information")
    playlists_information = channel_data.get("Playlist_information", [])
    video_information = channel_data.get("Video_information", [])
    comment_information = channel_data.get("Comment_information", [])

    # Insert channel information
    insert_into_channels(channel_info)

    # Insert playlists information
    for playlist_info in playlists_information:
        insert_into_playlists(playlist_info, channel_info.get("Channel_Id", ""))

    # Insert video information
    for video_info in video_information:
        insert_into_videos(video_info, channel_info.get("Channel_Id", ""))

    # Insert comment information
    for comment_info in comment_information:
        video_id = comment_info.get("Video_Id", "")
        if video_id:
            insert_into_comments(comment_info, video_id)

# Function to migrate all channels data to SQL
def migrate_all_channels_to_sql():

    # Drop existing tables
    drop_tables()

    create_channels_table()
    create_playlists_table()
    create_videos_table()
    create_comments_table()

    # Fetch all records from the MongoDB collection
    all_records = collection1.find()

    for record in all_records:
        channel_info = record.get("Channel_information")
        playlists_information = record.get("Playlist_information", [])
        video_information = record.get("Video_information", [])
        comment_information = record.get("Comment_information", [])

        # Insert channel information
        insert_into_channels(channel_info)

        # Insert playlists information
        for playlist_info in playlists_information:
            insert_into_playlists(playlist_info, channel_info.get("Channel_Id", ""))

        # Insert video information
        for video_info in video_information:
            insert_into_videos(video_info, channel_info.get("Channel_Id", ""))

        # Insert comment information
        for comment_info in comment_information:
            video_id = comment_info.get("Video_Id", "")
            if video_id:
                insert_into_comments(comment_info, video_id)


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
    page_icon="▶",
    )

with st.sidebar:                                   #Creating sidebars and options
    selected= option_menu(
        menu_title="YouTube Data",
        options=['Home','Project Learnings','Contact',],
        icons=['house','book','envelope'],
        menu_icon='cast',
        default_index=0,
        )
    
if selected == 'Home':
    st.subheader(f'Welcome! YouTube Data Harvasting and WareHousing')

    client = pymongo.MongoClient("mongodb+srv://sumankumar:1234abcd@cluster0.8wmkdrj.mongodb.net/?retryWrites=true&w=majority")

    def channel_exists(channel_id):
        db = client["youtube_data"]
        collection1 = db["channel_details"]
        return collection1.count_documents({"Channel_information.Channel_Id": channel_id}) > 0

    def channel_details(channel_id):
        if channel_exists(channel_id):
            return "Channel details already exist in MongoDB"

        data_to_insert = {
            "Channel_information": get_channel_info(channel_id),
            "Playlist_information": get_playlist_info(channel_id),
            "Video_ids": get_videos_id(channel_id),
            "Video_information": get_video_info(get_videos_id(channel_id)),
            "Comment_information": get_comment_info(get_videos_id(channel_id))
        }

        db = client["youtube_data"]
        collection1 = db["channel_details"]

        result = collection1.insert_one(data_to_insert)
        return f"Channel details uploaded to MongoDB with document ID: {result.inserted_id}"

    # Use st.form to handle form submissions
    with st.form("Collect and Store Data in MongoDB"):
        
        client = pymongo.MongoClient("mongodb+srv://sumankumar:1234abcd@cluster0.8wmkdrj.mongodb.net/?retryWrites=true&w=majority")
        channel_id = st.text_input("Enter Channel ID:")
        submitted1 = st.form_submit_button("Submit")

        if submitted1:
            if channel_exists(channel_id):
                st.success("Channel details already exist")
            else:
                insert = channel_details(channel_id)
                st.success(insert)

        db = client["youtube_data"]
        collection1 = db["channel_details"]

    # Streamlit UI
    with st.form("Migrate to SQL"):
        st.subheader("Migrate to SQL")

        # Fetch all channel names directly from MongoDB collection
        all_channel_options = ["All Channels"] + [ch_data["Channel_information"]["Channel_Name"] for ch_data in collection1.find({}, {"_id": 0, "Channel_information": 1})]

        # Allow the user to select channels for migration
        selected_option = st.selectbox("Select channels to migrate:", all_channel_options)

        # Handle form submission
        submitted = st.form_submit_button("Submit")

        if submitted:
            if selected_option == "All Channels":
                # Migrate all channels, playlists, videos, and comments to SQL
                migrate_all_channels_to_sql()
                st.success("All channels data migrated successfully.")
            else:
                # Migrate channels, playlists, videos, and comments for the selected channel to SQL
                selected_channel_data = collection1.find_one({"Channel_information.Channel_Name": selected_option})

                if selected_channel_data:
                    migrate_to_sql(selected_channel_data)
                    st.success(f"Data for the selected channel '{selected_option}' migrated successfully.")
                else:
                    st.warning(f"No data found for the selected channel: {selected_option}")

    # Close PostgreSQL connection
    cursor.close()
    mydb.close()

    show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

    if show_table=="CHANNELS":
        show_channels_table()

    elif show_table=="PLAYLISTS":
        show_playlists_table()

    elif show_table=="VIDEOS":
        show_videos_table()

    elif show_table=="COMMENTS":
        show_comments_table()

    # Connect to PostgreSQL
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Sugu1234",
        database="youtube_database",
        port="5432"
    )
    cursor = mydb.cursor()

    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb+srv://sumankumar:1234abcd@cluster0.8wmkdrj.mongodb.net/?retryWrites=true&w=majority")
    db = client["youtube_data"]
    collection1 = db["channel_details"]

    # Get distinct channel names
    distinct_channel_names = collection1.distinct("Channel_information.Channel_Name")

    # Add a special option for selecting all channels
    special_option = "All Channels"
    channel_names_with_all = [special_option] + distinct_channel_names

    # Use a conditional statement to handle the selection
    selected_channel = st.selectbox("Select a Channel:", options=channel_names_with_all)

    question = st.selectbox("Select a Question:", options=[
        "1. All THE VIDEOS AND CHANNELS",
        "2. CHANNELS WITH MOST NUMBER OF VIDEOS",
        "3. TOP 10 MOST VIEWED VIDEOS",
        "4. COMMENTS IN EACH VIDEOS",
        "5. VIDEOS WITH HIGHEST LIKES",
        "6. LIKES OF ALL VIDEOS",
        "7. VIEWS OF EACH CHANNEL",
        "8. VIDEOS PUBLISHED IN THE YEAR OF 2022",
        "9. AVERAGE DURATION OF ALL VIDEOS IN EACH CHANNEL",
        "10. VIDEOS WITH THE HIGHEST NUMBER OF COMMENTS"
    ])

    if question == "1. All THE VIDEOS AND CHANNELS":
        # Adjust the WHERE clause to include all channels if selected_channel is "All"
        if selected_channel == "All Channels":
            query1 = '''SELECT title AS "Video Title", channel_name AS "Channel Name"
                        FROM videos'''
        else:
            query1 = f'''SELECT Title AS "Video Title", Channel_Name AS "Channel Name"
                        FROM videos
                        WHERE channel_name = '{selected_channel}' '''

        cursor.execute(query1)
        table1 = cursor.fetchall()
        df1 = pd.DataFrame(table1, columns=["Video Title", "Channel Name"])
        st.write(df1)

    elif question == "2. CHANNELS WITH MOST NUMBER OF VIDEOS":
        # Adjust the WHERE clause to include all channels if selected_channel is "All"
        if selected_channel == "All Channels":
            query2 = '''SELECT Channel_Name AS "Channel Name", Total_Videos AS "Number of Videos"
                        FROM channels
                        ORDER BY total_videos DESC'''
        else:
            query2 = f'''SELECT Channel_Name AS "Channel Name", Total_Videos AS "Number of Videos"
                        FROM channels
                        WHERE channel_name = '{selected_channel}'
                        ORDER BY total_videos DESC'''

        cursor.execute(query2)
        table2 = cursor.fetchall()
        df2 = pd.DataFrame(table2, columns=["Channel Name", "Number of Videos"])
        st.write(df2)

    elif question == "3. TOP 10 MOST VIEWED VIDEOS":
        # Adjust the WHERE clause to include all channels if selected_channel is "All"
        if selected_channel == "All Channels":
            query3 = '''SELECT Title AS "Video Title", Channel_Name AS "Channel Name", Views AS "Number of Views"
                        FROM videos
                        WHERE Views IS NOT NULL
                        ORDER BY Views DESC
                        LIMIT 10'''
        else:
            query3 = f'''SELECT title AS "Video Title", channel_name AS "Channel Name", views AS "Number of Views"
                        FROM videos
                        WHERE views IS NOT NULL AND channel_name = '{selected_channel}'
                        ORDER BY views DESC
                        LIMIT 10'''

        cursor.execute(query3)
        table3 = cursor.fetchall()
        df3 = pd.DataFrame(table3, columns=["Video Title", "Channel Name", "Number of Views"])
        st.write(df3)

    elif question == "4. COMMENTS IN EACH VIDEOS":
        # Adjust the WHERE clause to include all channels if selected_channel is "All"
        if selected_channel == "All Channels":
            query4 = '''SELECT title AS "Video Title", comments AS "Number of Comments"
                        FROM videos
                        WHERE comments IS NOT NULL'''
        else:
            query4 = f'''SELECT title AS "Video Title", comments AS "Number of Comments"
                        FROM videos
                        WHERE comments IS NOT NULL AND channel_name = '{selected_channel}' '''

        cursor.execute(query4)
        table4 = cursor.fetchall()
        df4 = pd.DataFrame(table4, columns=["Video Title", "Number of Comments"])
        st.write(df4)

    elif question == "5. VIDEOS WITH HIGHEST LIKES":
        # Adjust the WHERE clause to include all channels if selected_channel is "All"
        if selected_channel == "All Channels":
            query5 = '''SELECT title AS "Video Title", channel_name AS "Channel Name", likes AS "Number of Likes"
                        FROM videos
                        WHERE likes IS NOT NULL
                        ORDER BY likes DESC'''
        else:
            query5 = f'''SELECT title AS "Video Title", channel_name AS "Channel Name", likes AS "Number of Likes"
                        FROM videos
                        WHERE likes IS NOT NULL AND channel_name = '{selected_channel}'
                        ORDER BY likes DESC'''

        cursor.execute(query5)
        table5 = cursor.fetchall()
        df5 = pd.DataFrame(table5, columns=["Video Title", "Channel Name", "Number of Likes"])
        st.write(df5)

    elif question == "6. LIKES OF ALL VIDEOS":
        # Adjust the WHERE clause to include all channels if selected_channel is "All"
        if selected_channel == "All Channels":
            query6 = '''SELECT title AS "Video Title", likes AS "Number of Likes"
                        FROM videos
                        WHERE likes IS NOT NULL
                        ORDER BY likes DESC'''
        else:
            query6 = f'''SELECT title AS "Video Title", likes AS "Number of Likes"
                        FROM videos
                        WHERE likes IS NOT NULL AND channel_name = '{selected_channel}'
                        ORDER BY likes DESC'''

        cursor.execute(query6)
        table6 = cursor.fetchall()
        df6 = pd.DataFrame(table6, columns=["Video Title", "Number of Likes"])
        st.write(df6)

    elif question == "7. VIEWS OF EACH CHANNEL":
        # Adjust the WHERE clause to include all channels if selected_channel is "All"
        if selected_channel == "All Channels":
            query7 = '''SELECT channel_name AS "Channel Name", views AS "Total Views"
                        FROM channels
                        WHERE views IS NOT NULL
                        ORDER BY views'''
        else:
            query7 = f'''SELECT channel_name AS "Channel Name", views AS "Total Views"
                        FROM channels
                        WHERE views IS NOT NULL AND channel_name = '{selected_channel}'
                        ORDER BY views'''

        cursor.execute(query7)
        table7 = cursor.fetchall()
        df7 = pd.DataFrame(table7, columns=["Channel Name", "Total Views"])
        st.write(df7)

    elif question == "8. VIDEOS PUBLISHED IN THE YEAR OF 2022":
        # Adjust the WHERE clause to include all channels if selected_channel is "All"
        if selected_channel == "All Channels":
            query8 = '''SELECT title AS "Video Title", published AS "Published Date", channel_name AS "Channel Name"
                        FROM videos
                        WHERE EXTRACT(YEAR FROM published) = 2022'''
        else:
            query8 = f'''SELECT title AS "Video Title", published AS "Published Date", channel_name AS "Channel Name"
                        FROM videos
                        WHERE EXTRACT(YEAR FROM published) = 2022 AND channel_name = '{selected_channel}' '''

        cursor.execute(query8)
        table8 = cursor.fetchall()
        df8 = pd.DataFrame(table8, columns=["Video Title", "Published Date", "Channel Name"])
        st.write(df8)

    elif question == "9. AVERAGE DURATION OF ALL VIDEOS IN EACH CHANNEL":
        # Adjust the WHERE clause to include all channels if selected_channel is "All"
        if selected_channel == "All Channels":
            query9 = '''SELECT channel_name AS "Channel Name", AVG(duration) AS "Average Duration"
                        FROM videos
                        GROUP BY channel_name'''
        else:
            query9 = f'''SELECT channel_name AS "Channel Name", AVG(duration) AS "Average Duration"
                        FROM videos
                        WHERE channel_name = '{selected_channel}'
                        GROUP BY channel_name'''

        cursor.execute(query9)
        table9 = cursor.fetchall()
        df9 = pd.DataFrame(table9, columns=["Channel Name", "Average Duration"])
        st.write(df9)

    elif question == "10. VIDEOS WITH THE HIGHEST NUMBER OF COMMENTS":
        # Adjust the WHERE clause to include all channels if selected_channel is "All"
        if selected_channel == "All Channels":
            query10 = '''SELECT title AS "Video Title", channel_name AS "Channel Name", comments AS "Number of Comments"
                        FROM videos
                        WHERE comments IS NOT NULL
                        ORDER BY comments DESC'''
        else:
            query10 = f'''SELECT title AS "Video Title", channel_name AS "Channel Name", comments AS "Number of Comments"
                        FROM videos
                        WHERE comments IS NOT NULL AND channel_name = '{selected_channel}'
                        ORDER BY comments DESC'''

        cursor.execute(query10)
        table10 = cursor.fetchall()
        df10 = pd.DataFrame(table10, columns=["Video Title", "Channel Name", "Number of Comments"])
        st.write(df10)

    # Close PostgreSQL connection
    cursor.close()
    mydb.close()

if selected == 'Project Learnings':
    st.subheader("Python Scripting")
    st.subheader("Data Collection")
    st.subheader("MongoDB")
    st.subheader("SQL")
    st.subheader("API Integration")
    st.subheader("Data Management using MongoDB and SQL")

if selected == 'Contact':
    st.subheader("My GitHub - https://github.com/suman98kumar")
    st.subheader("My Email - sumankumarmba2020@gmail.com")
    st.subheader("Thankyou for visiting My project")

# Function to delete channel details from MongoDB
def delete_channel_mongodb(channel_name):
    client = pymongo.MongoClient("mongodb+srv://sumankumar:1234abcd@cluster0.8wmkdrj.mongodb.net/?retryWrites=true&w=majority")
    db = client["youtube_data"]
    collection1 = db["channel_details"]
    result = collection1.delete_one({"Channel_information.Channel_Name": channel_name})

    client.close()

    if result.deleted_count > 0:
        st.success(f"Successfully deleted channel: {channel_name} from MongoDB")
    else:
        st.warning(f"Channel '{channel_name}' not found in MongoDB.")

# Function to delete channel details from PostgreSQL
def delete_channel_postgresql(channel_name):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Sugu1234",
        database="youtube_database",
        port="5432"
    )
    cursor = mydb.cursor()

    try:
        # Fetch Channel_Id using the channel_name
        cursor.execute('SELECT Channel_Id FROM channels WHERE Channel_Name = %s', (channel_name,))
        channel_id = cursor.fetchone()

        if channel_id:
            channel_id = channel_id[0]

            # Delete records from the child tables first
            cursor.execute('DELETE FROM comments WHERE video_id IN (SELECT Video_Id FROM videos WHERE Channel_Id = %s)', (channel_id,))
            cursor.execute('DELETE FROM videos WHERE Channel_Id = %s', (channel_id,))
            cursor.execute('DELETE FROM playlists WHERE Channel_Id = %s', (channel_id,))

            # Finally, delete the channel from the main table
            cursor.execute('DELETE FROM channels WHERE Channel_Id = %s', (channel_id,))

            mydb.commit()
            st.success(f"Successfully deleted channel: {channel_name} from PostgreSQL")
        else:
            st.warning(f"Channel '{channel_name}' not found in PostgreSQL.")
    except Exception as e:
        st.warning(f"Error deleting channel from PostgreSQL: {e}")
    finally:
        cursor.close()
        mydb.close()

# Streamlit app
def main():
    # Sidebar for individual and all channels deletion
    st.sidebar.title("Channel Deletion")

    # Get channel name from user input for individual deletion
    channel_name = st.sidebar.text_input("Enter Channel Name")

    # Button to delete individual channel from MongoDB
    if st.sidebar.button("Delete Individual from MongoDB"):
        delete_channel_mongodb(channel_name)

    # Button to delete individual channel from PostgreSQL
    if st.sidebar.button("Delete Individual from PostgreSQL"):
        delete_channel_postgresql(channel_name)

if __name__ == "__main__":
    main()

