import MySQLdb
import json

def add_xml_field(fieldName,fieldValue,CDATA):
    if not CDATA:
        return '<field name=\"' + fieldName + "\">" + fieldValue + "</field>\n"
    else:
        return '<field name=\"' + fieldName + "\"><![CDATA[" + fieldValue + "]]></field>\n"
        
db=MySQLdb.connect(host="localhost", user="root")
curr = db.cursor()
curr.execute("use videos_feed")
curr.execute("select video_id,published_date,title,description,thumbnails,channelId,channelTitle,duration,dimension,definition,views,likes,dislikes,favourites,commentsCount,topics from youtube_videos")
filew=open('index_xmls/0.xml','w')
count=0
fileCount=0
filew.write('<add>\n')
for video in curr.fetchall():
    try:
        filew.write("<doc>\n")
        filew.write(add_xml_field('id',video[0],False))
        filew.write(add_xml_field('published_date',video[1],False))
        filew.write(add_xml_field('title',video[2],True))
        filew.write(add_xml_field('description',video[3],True))
        filew.write(add_xml_field('thumbnails',video[4],True))
        filew.write(add_xml_field('channelId',video[5],False))
        filew.write(add_xml_field('channelTitle',video[6],True))
        filew.write(add_xml_field('duration',video[7],False))
        filew.write(add_xml_field('dimension',video[8],False))
        filew.write(add_xml_field('definition',video[9],False))
        filew.write(add_xml_field('views',str(video[10]),False))
        filew.write(add_xml_field('likes',str(video[11]),False))
        filew.write(add_xml_field('dislikes',str(video[12]),False))
        filew.write(add_xml_field('favourites',str(video[13]),False))
        filew.write(add_xml_field('commentsCount',str(video[14]),False))
        topics = video[15]
        if topics == None:
            topics = []
        else:
            topics = json.loads(topics)
        for topic in topics:
            filew.write(add_xml_field('topic',topic,True))
        filew.write("</doc>\n\n")
        count=count+1
        if count%10000==0:
            filew.write('</add>\n')
            filew.close()
            fileCount = fileCount+1
            filew=open('index_xmls/'+str(fileCount)+'.xml','w')
            filew.write('<add>\n')
    except Exception:
            count=count+1
            filew.write("</doc>\n\n")
            continue
filew.write('</add>\n')
filew.close()
