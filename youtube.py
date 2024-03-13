#conntion object for mongodb and api key for youtube and api connection and sql postgre connection
import googleapiclient.discovery
import pymongo
import pandas as pd
import streamlit as st
import psycopg2
api_key="AIzaSyCUeG2E2O1SUGKOhqU_7CETTbK81bF9QE4"
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
conn=pymongo.MongoClient("mongodb://Vignesh:Vignesh3@ac-dsiib9v-shard-00-00.fvjkqe9.mongodb.net:27017,ac-dsiib9v-shard-00-01.fvjkqe9.mongodb.net:27017,ac-dsiib9v-shard-00-02.fvjkqe9.mongodb.net:27017/?ssl=true&replicaSet=atlas-l1j7no-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")
youtube=conn["youtube"]
coll=youtube["channel_data"]
mydb = psycopg2.connect(host="localhost", user="postgres", password="vignesh", port=5432, database="youtube")
mycursor = mydb.cursor()

#to get the channel datas
def get_channel_data(cid):
  youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
  request = youtube.channels().list(part="snippet,contentDetails,statistics",id=cid)
  response = request.execute()
  chaneldata=dict(name_chanel=response["items"][0]["snippet"]["title"],channel_id=response["items"][0]["id"],subscriber_count=response["items"][0]["statistics"]["subscriberCount"],
  video_count=response["items"][0]["statistics"]["videoCount"],
  viewcount=response["items"][0]["statistics"]["viewCount"],upload_id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],published_at=response["items"][0]["snippet"]["publishedAt"])
  return(chaneldata)
# to get the video ids for further steps
def get_videoid(cid):
  video_id=[]
  youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
  response = youtube.channels().list(part="snippet,contentDetails,statistics",id=cid).execute()
  upload_id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
  next_page=None
  while True:
    response1 = youtube.playlistItems().list(part="snippet", playlistId =upload_id,maxResults=50,pageToken=next_page).execute()
    next_page=response1.get("nextPageToken")
    for i in range (len(response1["items"])):
      video_id.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])
    if next_page is None:
      break
  return(video_id)
# to get the video datas for the given channel
def get_video_data(video_id):
  videos_data=[]
  youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
  for videos_id in video_id:
    response2=youtube.videos().list(part="snippet,ContentDetails,statistics",id=videos_id).execute()
    for item in response2["items"]:
      data=dict(Video_Title=item["snippet"]["title"],video_id=item['id'],channel_title=item["snippet"]["channelTitle"],
                channel_id=item["snippet"]["channelId"], view_count=item["statistics"].get("viewCount"),
                  duration=item["contentDetails"].get("duration"),like_count=item["statistics"].get('likeCount'),
                  comment_count=item["statistics"].get('commentCount'),video_descriptions=item["snippet"]["localized"]["description"],
                  thumbnail=item["snippet"]["thumbnails"]["default"]["url"],tags=item["snippet"].get("tags"),publish_at=item["snippet"]["publishedAt"])
      videos_data.append(data)
  return(videos_data)
#to get the comment datas 
def get_commentdata(cid):
  api_service_name = "youtube"
  api_version = "v3"
  api_key = "AIzaSyCUeG2E2O1SUGKOhqU_7CETTbK81bF9QE4"
  youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
  cm=[]
  next_page=None
  while True:
    request = youtube.commentThreads().list(
          part="snippet,replies",
          allThreadsRelatedToChannelId=cid,maxResults=50,pageToken=next_page
      )
    response = request.execute()
    next_page=response.get("nextPageToken")
    for item in response["items"]:
      data=dict(comment_id=item["id"],text_display=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],publishedat=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"],video_id= item["snippet"]["videoId"],
                channel_id=item["snippet"]["channelId"]
                )
      cm.append(data)
    if next_page is None:
      break
  return(cm)

# to get the palylist datas
def get_playlistdata(cid):
  youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
  next_page_token=None
  while True:
    playlist_data=[]
    response4 = youtube.playlists().list(part="snippet,contentDetails",channelId=cid,maxResults=25,pageToken=next_page_token).execute()
    for item in response4["items"]:
      p_data=dict(playlist_id=item["id"],
              playlist_title=item["snippet"]["title"],              
              count_of_playlist=item["contentDetails"]["itemCount"],              
              channel_title=item['snippet']["channelTitle"],channel_id=item["snippet"]["channelId"],playlist_publishedAt=item["snippet"]["publishedAt"])
      playlist_data.append(p_data)
      next_page_token=response4.get("nextPageToken")
    if next_page_token==None:
        break
  return(playlist_data)
#gatheing channel datas and video datas for channel and insert the datas into mongodb
def  insert_to_mongodb(cid):
  cd=get_channel_data(cid)
  pld=get_playlistdata(cid)
  video_id=get_videoid(cid)
  vd=get_video_data(video_id)
  cmt=get_commentdata(cid)
  coll.insert_one({"channel_detail":cd,"video-id":video_id,"playlistdata":pld,"video_data":vd,"comment_data":cmt})
#migrating datas from mongodb to sql
def chl(slected_name):
  mydb = psycopg2.connect(host="localhost", user="postgres", password="vignesh", port=5432, database="youtube")
  mycursor = mydb.cursor()
  ch_list=[]
  for i in coll.find({"channel_detail.name_chanel":slected_name}, {"_id": 0}):
    ch_list.append(i["channel_detail"])
    df=pd.DataFrame(ch_list)
    mycursor.execute("""create table if not exists channel_info(Channel_Name varchar(50), Channel_ID varchar(50) primary key, Subscriber_count bigint, video_count bigint, Views_count bigint, upload_id varchar(50), published_at varchar(255))""")
    mydb.commit()
    for i, j in df.iterrows():
        query = '''INSERT INTO Channel_info (Channel_Name, Channel_ID, Subscriber_count, video_count, Views_count, upload_id, published_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)'''

        values = (j['name_chanel'], j['channel_id'], j['subscriber_count'], j['video_count'], j['viewcount'], j['upload_id'], j['published_at'])
        try:
            mycursor.execute(query, values)
            mydb.commit()
        except:
            news=f"{slected_name} datas are already migrated and stored in sql tables"
            return news


def pla1(slected_name):
    mydb = psycopg2.connect(host="localhost", user="postgres", password="vignesh", port=5432, database="youtube")
    mycursor = mydb.cursor()
    play=[]
    for i  in coll.find({"channel_detail.name_chanel":slected_name}, {"_id": 0}):
        play.append(i['playlistdata'])
    df2=pd.DataFrame(play[0])
    
    querry=('''create table if not exists playlist(playlist_title varchar ,playlist_id varchar(50),count_of_playlist bigint, channel_title varchar(50) ,channel_id varchar(50),published_at varchar(255))''')
    mycursor.execute(querry)
    mydb.commit()
    for i, j in df2.iterrows():
      querry = '''INSERT INTO playlist (playlist_title,playlist_id, count_of_playlist,channel_title ,channel_id, published_at)
          VALUES (%s, %s, %s, %s, %s, %s)'''

      values = ( j['playlist_title'],j['playlist_id'], j['count_of_playlist'], j['channel_title'],j['channel_id'], j['playlist_publishedAt'])

      try:
        mycursor.execute(querry, values)
        mydb.commit()
      except:
        news=f"{slected_name} datas are already migrated and stored in sql tables"
        return news

def vdi(slected_name):
    vd=[]
    for i in coll.find({"channel_detail.name_chanel":slected_name}, {"_id": 0}):
        vd.append(i['video_data'])
    df3=pd.DataFrame(vd[0])
    
    querry=('''create table if not exists video_data(Video_Title varchar(255), video_id varchar(255) ,channel_name varchar(255),channel_id varchar(255) ,view_count bigint,like_count bigint,comment_count bigint,duration varchar(225), video_descriptions varchar, publish_at varchar(255), thumbnail varchar(255),tags varchar)''')
    mycursor.execute(querry)
    mydb.commit()
    for i, j in df3.iterrows():
      query = '''INSERT INTO  video_data(Video_Title,video_id,channel_name ,channel_id, view_count,like_count,comment_count,duration, video_descriptions,publish_at, thumbnail,tags)
                  VALUES (%s,%s,%s , %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

      values = (j['Video_Title'], j['video_id'], j['channel_title'],j['channel_id'] ,j['view_count'],j['like_count'],j['comment_count'],j['duration'],j["video_descriptions"],j['publish_at'],j['thumbnail'],j["tags"])
      try:
        mycursor.execute(query, values)
        mydb.commit()
      except:
        news=f"{slected_name} datas are already migrated and stored in sql tables"
        return news
    
def cm(slected_name):
    cd=[]
    for i in coll.find({"channel_detail.name_chanel":slected_name}, {"_id": 0}):
        cd.append(i['comment_data'])
    df4=pd.DataFrame(cd[0])
    querry=('''create table IF NOT EXISTS comment_data(comment_id varchar, text_display varchar , author varchar,publishedat varchar , video_id varchar, channel_id varchar)''')
    mycursor.execute(querry)
    mydb.commit()
    for i, j in df4.iterrows():
        query = '''INSERT INTO  comment_data(comment_id,text_display,author, publishedat, video_id, channel_id)
                   VALUES (%s, %s, %s, %s, %s, %s)'''
        
        values = (j['comment_id'], j['text_display'], j['author'], j['publishedat'],j['video_id'],j['channel_id'])
        try:
            mycursor.execute(query, values)
            mydb.commit()
        except:
           news=f"{slected_name} datas are already migrated and stored in sql tables"
           return news
def table(slected_name):
   sqlinsert=chl(slected_name)
   if sqlinsert:
      return (sqlinsert)
   else:
      pla1(slected_name)
      vdi(slected_name)
      cm(slected_name)
      return "datas are sucessfully migrated"
   

def cha():
  mycursor.execute("select * from channel_info")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["channel name","channel id","subscriber count","video count","view count","upload id","published at"])
  st.write(dfq1)
  
def pla():
  mycursor.execute("select * from playlist")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["playlist name","playlist id","no of videos","channel name","channel id","published at"])
  st.write(dfq1)
def vid():
  mycursor.execute("select * from video_data")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["video name","video id","channel name","channel id","view count","like count","comment count","duration","video discreption","published at","thumbnail","tags"])
  st.write(dfq1)
    
def cmt():
  mycursor.execute("select * from comment_data")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["comment id","comment","author","published at","video id","channel id"])
  st.write(dfq1)

#streamlit code
st.title(":black[YOUTUBE DATA HARVESTIND]")
channel_id=st.sidebar.text_input("Enter the Channel_id")

if st.sidebar.button("collect and store datas"):
    ch_id=[]
    for i in coll.find({},{"_id":0,"channel_detail":1}):
        ch_id.append(i['channel_detail']["channel_id"])
    if channel_id in ch_id:
       st.success("data of the channel is already exist")
    else:
       insert_to_mongodb(channel_id)
       st.success("datas of the channel is stored sucessfully")

all_name=[]
for i in coll.find({},{"_id":0,"channel_detail":1}):
  all_name.append(i['channel_detail']["name_chanel"])
slected_name=st.sidebar.selectbox("select the channel",all_name)

if st.sidebar.button("Migrate data to sql"):
  Table=table(slected_name)
  st.sidebar.success(Table)

show_table=st.sidebar.radio("select the option to view table",("Channels","Playlists","Videos","Comments"))
if show_table=="Channels":
  st.success(f"you are viewing the {show_table} details")
  cha()
elif show_table=="Playlists":
  st.success(f"you are viewing the {show_table} details")
  pla()
elif show_table=="Videos":
  st.success(f"you are viewing the {show_table} details")
  vid()
elif show_table=="Comments":
  st.success(f"you are viewing the {show_table} details")
  cmt()

question=st.selectbox("select the querry to run for the querry",("1.What are the names of all the videos and their corresponding channels?",
                                              "2.Which channels have the most number of videos, and how many videos do they have?",
                                              "3.What are the top 10 most viewed videos and their respective channels?",
                                              "4.How many comments were made on each video, and what are their corresponding video names?",
                                              "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                              "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                              "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                              "8.What are the names of all the channels that have published videos in the year 2022?",
                                              "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                              "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

mydb = psycopg2.connect(host="localhost", user="postgres", password="vignesh", port=5432, database="youtube")
mycursor = mydb.cursor()
if question=="1.What are the names of all the videos and their corresponding channels?":
  mycursor.execute("select video_title,channel_name from video_data")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["video title","channel name"])
  st.write(dfq1)
elif question=="2.Which channels have the most number of videos, and how many videos do they have?":
  mycursor.execute("SELECT channel_name, video_count FROM channel_info WHERE video_count = (SELECT MAX(video_count) FROM channel_info)")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["video count","channel name"])
  st.write(dfq1)
elif question=="3.What are the top 10 most viewed videos and their respective channels?":
  mycursor.execute("SELECT video_title, view_count,channel_name FROM video_data order by view_count desc limit 10")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["video name","view_count","channel name"])
  st.write(dfq1)
elif question=="4.How many comments were made on each video, and what are their corresponding video names?":
  mycursor.execute("select video_title,comment_count from video_data ORDER BY COALESCE(comment_count, 0) DESC")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["video name","comment count"])
  st.write(dfq1)
elif question=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
  mycursor.execute("SELECT like_count, video_title, channel_name FROM video_data WHERE like_count IS NOT NULL ORDER BY like_count DESC LIMIT 10")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["like count","video name","channel name"])
  st.write(dfq1)
elif question=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
  mycursor.execute("SELECT  like_count,video_title FROM video_data ORDER BY COALESCE(like_count, 0) desc")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["like count","video name"])
  st.write("youtube does not providing the dislike count")
  st.write(dfq1)
elif question=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
  mycursor.execute("select channel_name,views_count from channel_info")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["channel name","views count"])
  st.write(dfq1)
elif question=="8.What are the names of all the channels that have published videos in the year 2022?":
  mycursor.execute("SELECT DISTINCT channel_name FROM video_data WHERE publish_at LIKE '%2022%'")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["channel name"])
  st.write("in the year 2022 name of the channel have published video",dfq1)
elif question=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
  mycursor.execute("SELECT channel_name,TO_CHAR(INTERVAL '1 minute' * AVG(SUBSTRING(duration FROM 'PT(\d+)M')::INT) + INTERVAL '1 second' * AVG(SUBSTRING(duration FROM 'M(\d+)S')::INT),'HH24:MI:SS') AS average_duration FROM video_data GROUP BY channel_name;")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["channel name","average duraition"])
  st.write(dfq1)
elif question=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
  mycursor.execute("SELECT video_title, comment_count, channel_name from video_data ORDER BY COALESCE(comment_count, 0) DESC limit 10")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["video name","comment_count","channel name"])
  st.write(dfq1)