'''
This program should be executed after the dataParse execution is done.
Before running this program, please execute the following statements in PostgreSQL to create tables.
This program is to parse the tag entity of the Stack Overflow network.
--------------------------------------------------------------

create table tag(
Tid int not null primary key,
Tag_Label text);
--------------------------------------------------------------
@author: keshan
'''
import xml.sax
import psycopg2
import time


class TagExecuter:
    def __init__(self, dataArray, conn, cur):
        self.tagId = dataArray[0]
        self.tagLabel = "'" + dataArray[1] +"'"

        #self.body = dataArray[7]
        self.dataInsert(conn, cur)

    def dataInsert(self, conn, cur):
        cur.execute("INSERT INTO tag VALUES(%s, %s)" %
                        (self.tagId, self.tagLabel))
        conn.commit()



class StackContentHandler(xml.sax.ContentHandler):
    def __init__(self):
        xml.sax.ContentHandler.__init__(self)
    
    # get values from file
    def startElement(self, name, attrs):
        # only 'row' contains elements
        if name != "row":
            return
        
        # get post type:1 is question, 2 is answer
        ptype = "null"
        if attrs.has_key("PostTypeId"):
            ptype = attrs.getValue("PostTypeId")
        
        # question's typeId is 1 
        if ptype != '1':
            return

        if attrs.has_key("Tags"):
            global tagSet, rcount
            #split Tags into several parts, maximum 5 parts
            tagList = ((attrs.getValue("Tags"))[1:-1]).split("><")
            tagSet = tagSet | set(tagList)
            for each in tagSet:
                if(self.checkLabel(each)):
                    if(rcount > 38205):
                        print "Done"
                    else:
                        print "Duplicate Entry"+each
                else:
                    rcount += 1
                    answerData = []
                    answerData.append(rcount)
                    answerData.append(each)
                    TagExecuter(answerData, conn, cur)
                    print "added ", rcount, "tags into database"
    def checkLabel (self, tagLabel):
        #print("Shit",qusID)
        cur = conn.cursor()
        cur.execute("SELECT tid FROM tag WHERE tag_label = %s",(tagLabel,))
        return cur.fetchone() is not None



#Here is connect to your PostgreSQL
#Change you database, user and port here
db = "heft"
dbUser = "keshan"
dbPort = 5432

conn = psycopg2.connect(database=db, user=dbUser, port=dbPort)
cur = conn.cursor()

#the biggest id of answer is 25829713 (select * from answer order by answerid desc limit 10;)
#we intend to ensure each identifier representing only one instance
tcount = 30000000
rcount = 0
tagSet = {'c#'}
#Here is the path of the raw data file
f = open('/media/keshan/66C24863C2483A17/Data/Posts.xml')
before_time = time.time()
xml.sax.parse(f, StackContentHandler())
conn.close()
cur.close()
print("Total Data Transfer time is: ", (time.time() - before_time)/60)
f.close()



#
# for each in tagSet:
#     tcount += 1
#     tagLabel = "'" + each + "'"
#     print tcount, tagLabel
#     cur.execute("INSERT INTO tag VALUES(%s, %s)" % (tcount, tagLabel))
#     conn.commit()
#     print("added ", tcount, " tags into database")