import MySQLdb
import urllib2
import json
import urllib

topics_dir={}
db=MySQLdb.connect(host="localhost", user="root")
curr = db.cursor()
curr.execute("use videos_feed")
curr.execute("select topicIds,topics from youtube_videos where topics is NOT NULL")
for topic_ids,topics in curr.fetchall():
    topic_ids = json.loads(topic_ids)
    topics = json.loads(topics)
    if type(topic_ids)!=type([]):
        continue
    for i in range(len(topic_ids)):
        topic_id = topic_ids[i]
        topic = topics[i]
        if topic_id not in topics_dir:
            topics_dir[topic_id]=topic           
print 'length of topics dir is ', len(topics_dir)
curr.execute("select video_id,topicIds from youtube_videos where topics is NULL")
count=0
baseUrl = "https://www.googleapis.com/freebase/v1/mqlread?autorun=true&key=AIzaSyCR8uscOtMhJZ9fh-G1hNMAE7L50VJJvI0&query="
query_json={}
query_json["id"]=None
query_json["name"]=None
query_json["id|="]=[]
query=[query_json]
count = 0
videos_topics={}
for video_id,topic_ids in curr.fetchall():
    topic_ids = json.loads(topic_ids)
    if type(topic_ids)!=type([]):
        topic_ids = []
    retrieve_topic = 0
   
    for topic_id in topic_ids:
        if topic_id not in topics_dir and len(topic_id)>0:
            query_json["id|="].append(topic_id)
            topics_dir[topic_id]=[]
        if topics_dir[topic_id] is not None and len(topics_dir[topic_id])==0:
            retrieve_topic = 1
    if retrieve_topic==1:
        count=count+1
        videos_topics[video_id]=topic_ids
    else:
        topics = []
        for topic_id in topic_ids:
            topics.append(topics_dir.get(topic_id,''))
        insertString = "update youtube_videos set topics=" + '"' + MySQLdb.escape_string(json.dumps(topics)) + '"' +  " where video_id=\"" + video_id + '"'
        print 'insert string for one video is ',insertString
        curr.execute(insertString)
    if count%50==0 and len(query_json["id|="])>0:
        freebaseUrl = baseUrl + urllib.quote(json.dumps(query))
        print 'freebaseUrl is ',freebaseUrl
        freebaseResponse = json.loads(urllib2.urlopen(freebaseUrl).read())
        result = freebaseResponse['result']
        for topicJSON in result:
            topic_id = topicJSON["id"]
            topic_name = topicJSON["name"]
            topics_dir[topic_id]=topic_name
        for video_id in videos_topics:
            topic_ids = videos_topics[video_id]
            topics=[]
            for topic_id in topic_ids:
                topics.append(topics_dir[topic_id])
            insertString = "update youtube_videos set topics=" + '"' + MySQLdb.escape_string(json.dumps(topics)) + '"' +  " where video_id=\"" + video_id + '"'
            print 'insertString is ',insertString
            curr.execute(insertString)
        db.commit()
        query_json["id|="] = []
        videos_topics = {}
db.close()
