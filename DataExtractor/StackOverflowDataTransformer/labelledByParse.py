'''
This program should be executed after the tagParse execution is done.
Before running this program, please execute the following statements in PostgreSQL to create tables.
This program is to parse the labelled_by relationship of the Stack Overflow network.
--------------------------------------------------------------

create table labelled_by(
Qid int,
Tid int,
foreign key (Qid) references question (Qid),
foreign key (Tid) references tag (Tid));
--------------------------------------------------------------
@author: keshan
'''
import xml.sax
import time
import psycopg2

rcount = 0

class TagLabelExecuter:
    def __init__(self, dataArray, conn, cur):
        self.questionId = dataArray[0]
        self.tagLabel =  dataArray[1]
        print "questionId" + self.questionId + "tagLabel"+self.tagLabel
        self.dataInsert(conn, cur)

    def dataInsert(self, conn, cur):
        cur = conn.cursor()
        #print("SELECT tid FROM tag WHERE tag_label = %s",(self.tagLabel,))
        cur.execute("SELECT tid FROM tag WHERE tag_label = %s",(self.tagLabel,))
        rows = cur.fetchone()
        print rows
        tagId = str(rows[0])
        global rcount
        rcount += 1
        cur.execute("INSERT INTO labelled_by VALUES(%s, %s)" %
        (self.questionId, tagId))
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
        
        questionId = 'null'
        if attrs.has_key("Id"):
            questionId = attrs.getValue("Id")
            
        if attrs.has_key("Tags"):
            if (self.checkQuestion(questionId)):
                #split Tags into several parts, maximum 5 parts
                tagList = ((attrs.getValue("Tags"))[1:-1]).split("><")
                for each_tag in tagList:
                    if(self.checkTag(each_tag)):
                        answerData = []
                        answerData.append(questionId)
                        answerData.append(each_tag)
                        TagLabelExecuter(answerData, conn, cur)
                        print("added ", rcount, " labelled_by records into database")

    def checkQuestion (self, qusID):
        #print("Shit",qusID)
        cur = conn.cursor()
        cur.execute("SELECT Qid FROM question WHERE Qid = %s",(qusID,))
        return cur.fetchone() is not None
    def checkTag (self, tag):
        #print("Shit",tag)
        cur = conn.cursor()
        cur.execute("SELECT tid FROM tag WHERE tag_label = %s",(tag,))
        return cur.fetchone() is not None

db = "heft"
dbUser = "keshan"
dbPort = 5432

conn = psycopg2.connect(database=db, user=dbUser, port=dbPort)
cur = conn.cursor()
#Here is the path of the raw data file      
f = open('/media/keshan/66C24863C2483A17/Data/Posts.xml')
before_time = time.time()
xml.sax.parse(f, StackContentHandler())
conn.close()
cur.close()
print("Total Data Transfer time is: ", (time.time() - before_time)/60)
f.close()