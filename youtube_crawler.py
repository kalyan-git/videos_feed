import urllib
import urllib2
import json
import MySQLdb
import re
import traceback
import sys

videosProcessed={}
depthLimit = 1
file = open('videos_processed.txt')
while 1:
    line=file.readline()
    if len(line)==0:
       break
    id = line.strip()
    videosProcessed[id]=1
file.close()

def addVideoDetails(videoDetails):
    db=MySQLdb.connect(host="localhost", user="root")
    curr = db.cursor()
    curr.execute("use videos_feed")
    youtubeUrl = "https://www.googleapis.com/youtube/v3/videos?key=AIzaSyCR8uscOtMhJZ9fh-G1hNMAE7L50VJJvI0&part=contentDetails,statistics,topicDetails&id="
    for id in videoDetails:
        youtubeUrl=youtubeUrl+id+","
    youtubeUrl = youtubeUrl.strip(",")
    print 'youtube url is ', youtubeUrl
    try:
        youtubeResponse = urllib2.urlopen(youtubeUrl).read()
    except Exception:
        traceback.print_exc(file=sys.stdout)
        db.close()
        return
    jsonResponse = json.loads(youtubeResponse)
    videos = jsonResponse['items']
    for video in videos:
       id = video['id']
       if id in videosProcessed:
           continue
       contentDetails = video.get('contentDetails',{})
       duration = contentDetails.get('duration','')
       dimension = contentDetails.get('dimension','')
       definition = contentDetails.get('definition','')
       statistics = video.get('statistics',{})
       views = int(statistics.get('viewCount',0))
       likes = int(statistics.get('likeCount',0))
       dislikes = int(statistics.get('dislikeCount',0))
       favourites = int(statistics.get('favoriteCount',0))
       commentsCount = int(statistics.get('commentCount',0))
       topicDetails = video.get('topicDetails',{})
       topicIds = json.dumps(topicDetails.get('topicIds','[]'))
       relevantTopicIds = json.dumps(topicDetails.get('relevantTopicIds','[]'))
       videoDetails[id].append(duration)
       videoDetails[id].append(dimension)
       videoDetails[id].append(definition)
       videoDetails[id].append(views)
       videoDetails[id].append(likes)
       videoDetails[id].append(dislikes)
       videoDetails[id].append(favourites)
       videoDetails[id].append(commentsCount)
       videoDetails[id].append(topicIds)
       videosProcessed[id]=1
       insertString = "insert into youtube_videos(video_id,published_date,title,description,thumbnails,channelId,channelTitle,duration,dimension,definition,views,likes,dislikes,favourites,commentsCount,topicIds) values("
       insertString  = insertString + '"' + id + '"' + ","
       try:
          for attr in videoDetails[id]:
               insertString = insertString + '"' + str(MySQLdb.escape_string(str(attr))) + '"' +  ","
       except Exception:
          continue
       insertString = insertString.strip(",")
       insertString = insertString + ")"
       print 'insert string is ',insertString
       curr.execute(insertString)
    db.commit()   
    db.close()

def getRelatedVideos(id,depth):
    if depth > depthLimit:
       return
    youtubeUrl = "https://www.googleapis.com/youtube/v3/search?key=AIzaSyCR8uscOtMhJZ9fh-G1hNMAE7L50VJJvI0&part=snippet&type=video&maxResults=50&&relatedToVideoId="+id
    print 'youtube url is ', youtubeUrl
    try:
        youtubeResponse = urllib2.urlopen(youtubeUrl).read()
    except Exception:
        traceback.print_exc(file=sys.stdout)
        return
    jsonResponse = json.loads(youtubeResponse)
    videos = jsonResponse['items']
    videoDetails = {}
    for video in videos:
        id = video['id']['videoId']
        if id in videosProcessed:
                continue
        snippet = video['snippet']
        published_date = snippet['publishedAt']
        title = snippet['title']
        description = snippet.get('description','')
        thumbnails = json.dumps(snippet.get('thumbnails',{}))
        channelId = snippet.get('channelId','')
        channelTitle = snippet.get('channelTitle','')
        videoDetails[id]=[published_date,title,description,thumbnails,channelId,channelTitle]
    addVideoDetails(videoDetails)    
    for id in videoDetails:
        getRelatedVideos(id,depth+1)    

def crawlSeed(seed):
    youtubeUrl = "https://www.googleapis.com/youtube/v3/search?key=AIzaSyCR8uscOtMhJZ9fh-G1hNMAE7L50VJJvI0&part=snippet,id&type=video&maxResults=50&q="+seed
    print 'youtube url is ', youtubeUrl
    try:
        youtubeResponse = urllib2.urlopen(youtubeUrl).read()
    except Exception:
        traceback.print_exc(file=sys.stdout)
        return
    jsonResponse = json.loads(youtubeResponse)
    videos = jsonResponse['items']
    videoDetails = {}
    for video in videos:
        id = video['id']['videoId']
        snippet = video['snippet']
        published_date = snippet['publishedAt']
        title = snippet['title']
        description = snippet.get('description','')
        thumbnails = json.dumps(snippet.get('thumbnails',{}))
        channelId = snippet.get('channelId','')
        channelTitle = snippet.get('channelTitle','')
        videoDetails[id]=[published_date,title,description,thumbnails,channelId,channelTitle]
    addVideoDetails(videoDetails)    
    for id in videoDetails:
          getRelatedVideos(id,1)
file=open('seed_queries.txt')
while 1:
    line=file.readline()
    if len(line)==0:
        break
    seed = line.strip()
    crawlSeed(urllib.quote(seed)) 
file.close()
          
