#streamlit
import googleapiclient.discovery
import pymongo
api_key="AIzaSyC62JKqx4J_EyhmhC2TLg8etAPmnAnaUmQ"
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
conn=pymongo.MongoClient("mongodb://vigneshvrthn1:Vignesh3@ac-wpzyylr-shard-00-00.0mqmrnq.mongodb.net:27017,ac-wpzyylr-shard-00-01.0mqmrnq.mongodb.net:27017,ac-wpzyylr-shard-00-02.0mqmrnq.mongodb.net:27017/?ssl=true&replicaSet=atlas-lc9y68-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")
youtube=conn["youtube"]
coll=youtube["channel_data"]
def get_channel_data(cid):
  api_service_name = "youtube"
  api_version = "v3"
  api_key="AIzaSyC62JKqx4J_EyhmhC2TLg8etAPmnAnaUmQ"
  youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
  request = youtube.channels().list(part="snippet,contentDetails,statistics",id=cid)
  response = request.execute()
  chaneldata=dict(name_chanel=response["items"][0]["snippet"]["title"],channel_id=response["items"][0]["id"],
  published_at=response["items"][0]["snippet"]["publishedAt"],
  upload_id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
  subscriber_count=response["items"][0]["statistics"]["subscriberCount"],
  video_count=response["items"][0]["statistics"]["videoCount"],
  viewcount=response["items"][0]["statistics"]["viewCount"])
  return(chaneldata)
def get_videoid(cid):
  api_key="AIzaSyC62JKqx4J_EyhmhC2TLg8etAPmnAnaUmQ"
  api_service_name = "youtube"
  api_version = "v3"
  youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
  video_id=[]
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
def get_video_data(video_id):
  api_key="AIzaSyC62JKqx4J_EyhmhC2TLg8etAPmnAnaUmQ"
  api_service_name = "youtube"
  api_version = "v3"
  youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
  videos_data=[]
  for videos_id in video_id:
    response2=youtube.videos().list(part="snippet,ContentDetails,statistics",id=videos_id).execute()
    for item in response2["items"]:
      data=dict(Title=item["snippet"]["title"],video_id=item['id'],video_descriptions=item["snippet"]["localized"]["description"],channel_title=item["snippet"]["channelTitle"],
                channel_id=item["snippet"]["channelId"],
                publish_at=item["snippet"]["publishedAt"],
                  thumbnail=item["snippet"]["thumbnails"]["default"]["url"],
                  view_count=item["statistics"].get("viewCount"),
                  duration=item["contentDetails"].get("duration"),
                  like_count=item["statistics"].get('likeCount'),
                  comment_count=item["statistics"].get('commentCount'),
                  tags=item["snippet"].get("tags"))
      videos_data.append(data)
  return(videos_data)
def get_commentdata(video_id):
  api_key="AIzaSyC62JKqx4J_EyhmhC2TLg8etAPmnAnaUmQ"
  api_service_name = "youtube"
  api_version = "v3"
  youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
  comment_data=[]
  for videos_id in video_id:
      request = youtube.commentThreads().list(
        part="snippet,replies",
        videoId = "6aEFSCOHAWE"
    )
      response3 = request.execute()

      for item in response3["items"]:
        c_data=dict(video_id=item["snippet"]["videoId"],channel_id=item["snippet"]["channelId"],
            comment_id=item["snippet"]["topLevelComment"]["id"],
            video_comment=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
            author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
            publishedAt=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
      comment_data.append(c_data)
  return(comment_data)
def get_playlistdata(cid):
  api_key="AIzaSyC62JKqx4J_EyhmhC2TLg8etAPmnAnaUmQ"
  api_service_name = "youtube"
  api_version = "v3"
  youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
  next_page_token=None
  while True:
    playlist_data=[]
    response4 = youtube.playlists().list(part="snippet,contentDetails",channelId=cid,maxResults=25,pageToken=next_page_token).execute()
    for item in response4["items"]:
      p_data=dict(playlist_id=item["id"],
              playlist_title=item["snippet"]["title"],
              playlist_publishedAt=item["snippet"]["publishedAt"],
              count_of_playlist=item["contentDetails"]["itemCount"],
              channel_id=item["snippet"]["channelId"],
              channel_title=item['snippet']["channelTitle"])
      playlist_data.append(p_data)
      next_page_token=response4.get("nextPageToken")
    if next_page_token==None:
        break
  return(playlist_data)
def  insert_to_mongodb(cid):
  cd=get_channel_data(cid)
  pld=get_playlistdata(cid)
  video_id=get_videoid(cid)
  vd=get_video_data(video_id)
  cmt=get_commentdata(video_id)
  conn=pymongo.MongoClient("mongodb://vigneshvrthn1:Vignesh3@ac-wpzyylr-shard-00-00.0mqmrnq.mongodb.net:27017,ac-wpzyylr-shard-00-01.0mqmrnq.mongodb.net:27017,ac-wpzyylr-shard-00-02.0mqmrnq.mongodb.net:27017/?ssl=true&replicaSet=atlas-lc9y68-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")
  youtube=conn["youtube"]
  coll=youtube["channel_data"]
  coll.insert_one({"channel_detail":cd,"video-id":video_id,"playlistdata":pld,"video_data":vd,"comment_data":cmt})
import pymongo
import pandas as pd
conn=pymongo.MongoClient("mongodb://vigneshvrthn1:Vignesh3@ac-wpzyylr-shard-00-00.0mqmrnq.mongodb.net:27017,ac-wpzyylr-shard-00-01.0mqmrnq.mongodb.net:27017,ac-wpzyylr-shard-00-02.0mqmrnq.mongodb.net:27017/?ssl=true&replicaSet=atlas-lc9y68-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")
youtube=conn["youtube"]
coll=youtube["channel_data"]
import psycopg2
mydb = psycopg2.connect(host="localhost", user="postgres", password="vignesh", port=5432, database="postgres")
mycursor = mydb.cursor()




def chl(slected_name):
    ch_list=[]
    for i in coll.find({"channel_detail.name_chanel":slected_name}, {"_id": 0}):
      ch_list.append(i["channel_detail"])
    df=pd.DataFrame(ch_list)
    df
    mydb = psycopg2.connect(host="localhost", user="postgres", password="vignesh", port=5432, database="postgres")
    mycursor = mydb.cursor()
    mycursor.execute("CREATE TABLE IF NOT EXISTS Channel_table("
                 "Channel_Name varchar(50), "
                 "Channel_ID varchar(50) PRIMARY KEY, "
                 "published_at varchar(50), "
                 "upload_id varchar(50), "
                 "Subscriber_count bigint, "
                 "video_count bigint, "                 
                 "Views bigint)")
    mydb.commit()
    for i, j in df.iterrows():
        query = '''INSERT INTO Channel_table (Channel_Name, Channel_ID, published_at, upload_id, Subscriber_count, video_count, Views)
                VALUES (%s, %s, %s, %s, %s, %s, %s)'''

        values = (j['name_chanel'], j['channel_id'], j['published_at'], j['upload_id'], j['subscriber_count'], j['video_count'], j['viewcount'])
        try:
            mycursor.execute(query, values)
            mydb.commit()
        except:
           news=f"{slected_name} datas are already migrated and stored in sql tables"
           return news
def pla1(slected_name):
  play=[]
  for i  in coll.find({"channel_detail.name_chanel":slected_name}, {"_id": 0}):
      play.append(i['playlistdata'])
  df2=pd.DataFrame(play[0])
  mydb = psycopg2.connect(host="localhost", user="postgres", password="vignesh", port=5432, database="postgres")
  mycursor = mydb.cursor()
  querry=('''create table if not exists playlist(playlist_id varchar(50), playlist_title varchar , published_at varchar,count_of_playlist bigint, channel_id varchar(50), channel_title varchar(50))''')
  mycursor.execute(querry)
  mydb.commit()
  for i, j in df2.iterrows():
    query = '''INSERT INTO playlist (playlist_id,playlist_title, published_at, count_of_playlist, channel_id,channel_title)
          VALUES (%s, %s, %s, %s, %s, %s)'''

    values = (j['playlist_id'], j['playlist_title'], j['playlist_publishedAt'], j['count_of_playlist'], j['channel_id'],j['channel_title'])
    
    try:
            mycursor.execute(query, values)
            mydb.commit()
    except:
           news=f"{slected_name} datas are already migrated and stored in sql tables"
           return news

def vdi(slected_name):
  vd=[]
  for i in coll.find({"channel_detail.name_chanel":slected_name}, {"_id": 0}):
      vd.append(i['video_data'])
  df3=pd.DataFrame(vd[0])
  df3
  mydb = psycopg2.connect(host="localhost", user="postgres", password="vignesh", port=5432, database="postgres")
  mycursor = mydb.cursor()
  querry=('''create table if not exists video_data(Title varchar(255), video_id varchar(255) , video_descriptions varchar,channel_name varchar(255),channel_id varchar(255) , publish_at varchar(255), thumbnail varchar(255),view_count bigint,duration varchar(225),like_count bigint,comment_count bigint)''')
  mycursor.execute(querry)
  mydb.commit()
  for i, j in df3.iterrows():
    query = '''INSERT INTO  video_data(Title,video_id, video_descriptions,channel_name ,channel_id, publish_at, thumbnail,view_count,duration,like_count,comment_count)
                VALUES (%s,%s , %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

    values = (j['Title'], j['video_id'], j['video_descriptions'],j['channel_title'] ,j['channel_id'],j['publish_at'],j['thumbnail'],j['view_count'],j["duration"],j['like_count'],j['comment_count'])
    try:
            mycursor.execute(query, values)
            mydb.commit()
    except:
           news=f"{slected_name} datas are already migrated and stored in sql tables"
           return news
    
def cm(slected_name):
    cd=[]
    for i in coll.find({"channel_detail.name_chanel":slected_name}, {"_id": 0}):
        cd.append(i['comment_data'][0])
    df4=pd.DataFrame(cd)
    mydb = psycopg2.connect(host="localhost", user="postgres", password="vignesh", port=5432, database="postgres")
    mycursor = mydb.cursor()
    querry=('''create table IF NOT EXISTS comment_data(video_id varchar, channel_id varchar , comment_id varchar,video_comment varchar , author varchar, publishedAt varchar)''')
    mycursor.execute(querry)
    mydb.commit()
    for i, j in df4.iterrows():
        query = '''INSERT INTO  comment_data(video_id,channel_id, comment_id, video_comment, author, publishedAt)
                   VALUES (%s, %s, %s, %s, %s, %s)'''
        
        values = (j['video_id'], j['channel_id'], j['comment_id'], j['video_comment'],j['author'],j['publishedAt'])
        try:
            mycursor.execute(query, values)
            mydb.commit()
        except:
           news=f"{slected_name} datas are already migrated and stored in sql tables"
           return news
def table(slected_name):
    news=chl(slected_name)
    if news:
      return (news)
    else:
      pla1(slected_name)
      vdi(slected_name)
      cm(slected_name)
      return "datas are sucessfully migrated"
   
import streamlit as st
import pandas as pd
import pymongo
conn=pymongo.MongoClient("mongodb://vigneshvrthn1:Vignesh3@ac-wpzyylr-shard-00-00.0mqmrnq.mongodb.net:27017,ac-wpzyylr-shard-00-01.0mqmrnq.mongodb.net:27017,ac-wpzyylr-shard-00-02.0mqmrnq.mongodb.net:27017/?ssl=true&replicaSet=atlas-lc9y68-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")
youtube=conn["youtube"]
coll=youtube["channel_data"]
def cha():
  conn=pymongo.MongoClient("mongodb://vigneshvrthn1:Vignesh3@ac-wpzyylr-shard-00-00.0mqmrnq.mongodb.net:27017,ac-wpzyylr-shard-00-01.0mqmrnq.mongodb.net:27017,ac-wpzyylr-shard-00-02.0mqmrnq.mongodb.net:27017/?ssl=true&replicaSet=atlas-lc9y68-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")
  youtube=conn["youtube"]
  coll=youtube["channel_data"]
  ch_list=[]
  for i in coll.find({},{"_id":0,"channel_detail":1}):
      ch_list.append(i['channel_detail'])
  df=st.dataframe(ch_list)

  return(df)
def pla():
    play=[]
    for i in coll.find({},{"_id":0,"playlistdata":1}):
        for j in range(len(i['playlistdata'])):
            play.append(i['playlistdata'][j])
    df2=st.dataframe(play)
    return(df2)
def vid():
    vd=[]
    for i in coll.find({},{"_id":0,"video_data":1}):
        for j in range(len(i['video_data'])):
            vd.append(i['video_data'][j])
    df3=st.dataframe(vd)
    return(df3)
def cmt():
    cd=[]
    for i in coll.find({},{"_id":0,"comment_data":1}):
        for j in range(len(i['comment_data'])):
            cd.append(i['comment_data'][j])
    df4=st.dataframe(cd)
    return(df4)


#streamlit code
st.title(":black[YOUTUBE DATA HARVESTIND]")
channel_id=st.text_input("Enter the Channel_id")

if st.button("collect and store datas"):
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
slected_name=st.selectbox("select the channel",all_name)

if st.button("Migrate data to sql"):
  Table=table(slected_name)
  st.success(Table)

show_table=st.radio("select the option to view table",("Channels","Playlists","Videos","Comments"))
if show_table=="Channels":
  cha()
elif show_table=="Playlists":
  pla()
elif show_table=="Videos":
  vid()
elif show_table=="Comments":
  cmt()

question=st.selectbox("select your question",("1.What are the names of all the videos and their corresponding channels?",
                                              "2.Which channels have the most number of videos, and how many videos do they have?",
                                              "3.What are the top 10 most viewed videos and their respective channels?",
                                              "4.How many comments were made on each video, and what are their corresponding video names?",
                                              "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                              "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                              "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                              "8.What are the names of all the channels that have published videos in the year 2022?",
                                              "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                              "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

mydb = psycopg2.connect(host="localhost", user="postgres", password="vignesh", port=5432, database="postgres")
mycursor = mydb.cursor()

if question=="1.What are the names of all the videos and their corresponding channels?":
  mycursor.execute("select title as video_title,channel_name as channel_name from video_data")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["video title","channel name"])
  st.write(dfq1)
elif question=="2.Which channels have the most number of videos, and how many videos do they have?":
  mycursor.execute("SELECT channel_name, video_count FROM channel_table WHERE video_count = (SELECT MAX(video_count) FROM channel_table)")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["video count","channel name"])
  st.write(dfq1)
elif question=="3.What are the top 10 most viewed videos and their respective channels?":
  mycursor.execute("SELECT title, view_count,channel_name FROM video_data order by view_count desc limit 10")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["video name","view_count","channel name"])
  st.write(dfq1)
elif question=="4.How many comments were made on each video, and what are their corresponding video names?":
  mycursor.execute("SELECT  comment_count,title FROM video_data")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["comment count","video name"])
  st.write(dfq1)
elif question=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
  mycursor.execute("SELECT like_count, title, channel_name FROM video_data WHERE like_count IS NOT NULL ORDER BY like_count DESC LIMIT 10")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["like count","video name","channel name"])
  st.write(dfq1)
elif question=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
  mycursor.execute("SELECT  like_count,title FROM video_data ")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["like count","video name"])
  st.write(dfq1)
elif question=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
  mycursor.execute("select channel_name,views from channel_table")
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
  mycursor.execute("SELECT channel_name,comment_count FROM video_data WHERE comment_count IS NOT NULL ORDER BY like_count DESC LIMIT 10")
  mydb.commit()
  tab=mycursor.fetchall()
  dfq1=pd.DataFrame(tab,columns=["channel name","comment_count"])
  st.write(dfq1)