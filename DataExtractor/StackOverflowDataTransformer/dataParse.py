'''
Before running this program, please execute the following statements in PostgreSQL to create tables.
This program is to parse the entities of the Stack Overflow network.
--------------------------------------------------------------

create table question(
Qid int not null primary key,
Accepted_Aid int,
Owner_Id int,
Creation_Date text,
Last_Activity_Date text,
Score int,
View_Count int,
Answer_Count int,
Comment_Count int,
Favorite_Count int);
foreign key (Accepted_Aid) references answer (Aid));

create table answer(
Aid int not null primary key,
Parent_Qid int,
Owner_Id int,
Creation_Date text,
Last_Activity_Date text,
Score int,
Comment_Count int,
foreign key (Parent_Qid) references question (Qid));
--------------------------------------------------------------
@author: keshan
'''
import psycopg2
import xml.sax
import time

#this class is to insert question records into database
class QuestionExecuter:
    def __init__(self, dataArray, conn, cur):
        self.questionId = dataArray[0] 
        self.acceptedAnswerId = dataArray[1]
        self.questionOwnerId = dataArray[2]
        self.creationDate = dataArray[3]
        self.lastActivityDate = dataArray[4]
        self.score = dataArray[5]
        self.viewCount = dataArray[6]
        self.answerCount = dataArray[7]
        self.commentCount = dataArray[8]
        self.favoriteCount = dataArray[9]
        #self.title = dataArray[11]
        #self.body = dataArray[12]
        self.dataInsert(conn, cur)
        
    def dataInsert(self, conn, cur):
        # connect to db
        cur.execute("INSERT INTO question VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" %
                    (self.questionId, self.acceptedAnswerId, self.questionOwnerId,
                     self.creationDate, self.lastActivityDate, self.score,
                     self.viewCount, self.answerCount, self.commentCount,
                     self.favoriteCount))
        conn.commit()

#this class is to insert answer records into database        
class AnswerExecuter:
    def __init__(self, dataArray, conn, cur):
        self.answerId = dataArray[0] 
        self.parentQuestionId = dataArray[1]
        self.answerOwnerId = dataArray[2]
        self.creationDate = dataArray[3]
        self.lastActivityDate = dataArray[4]
        self.score = dataArray[5]
        self.commentCount = dataArray[6]
        #self.body = dataArray[7]
        self.dataInsert(conn, cur)
    
    def dataInsert(self, conn, cur):
        # connect to db
        cur.execute("INSERT INTO answer VALUES(%s, %s, %s, %s, %s, %s, %s)",
                    (self.answerId, self.parentQuestionId, self.answerOwnerId,
                     self.creationDate, self.lastActivityDate, self.score,
                     self.commentCount))
        conn.commit()

#this class is to parse raw data
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
        
        #question's typeId is 1
        # if ptype == '1':
        #     questionId = "null"
        #     global qcount
        #     qcount += 1
        #     if attrs.has_key("Id"):
        #         questionId = int(attrs.getValue("Id"))
        #
        #     acceptedAnswerId = "null"
        #     if attrs.has_key("AcceptedAnswerId"):
        #         acceptedAnswerId = int(attrs.getValue("AcceptedAnswerId"))
        #
        #     questionOwnerId = "null"
        #     if attrs.has_key("OwnerUserId"):
        #         questionOwnerId = int(attrs.getValue("OwnerUserId"))
        #
        #     creationDate = "null"
        #     if attrs.has_key("CreationDate"):
        #         creationDate = "'" + attrs.getValue("CreationDate") + "'"
        #
        #     lastActivityDate = "null"
        #     if attrs.has_key("LastActivityDate"):
        #         lastActivityDate = "'" + attrs.getValue("LastActivityDate") + "'"
        #
        #     score = 0
        #     if attrs.has_key("Score"):
        #         score = int(attrs.getValue("Score"))
        #
        #     viewCount = 0
        #     if attrs.has_key("ViewCount"):
        #         viewCount = int(attrs.getValue("ViewCount"))
        #
        #     answerCount = 0
        #     if attrs.has_key("AnswerCount"):
        #         answerCount = int(attrs.getValue("AnswerCount"))
        #
        #     commentCount = 0
        #     if attrs.has_key("CommentCount"):
        #         commentCount = int(attrs.getValue("CommentCount"))
        #
        #     favoriteCount = 0
        #     if attrs.has_key("FavoriteCount"):
        #         viewCount = int(attrs.getValue("FavoriteCount"))
        #     '''
        #     title = "null"
        #     if attrs.has_key("Title"):
        #         title = "'" + (attrs.getValue("Title")).replace("'", "''") + "'"
        #
        #     body = "null"
        #     if attrs.has_key("Body"):
        #         body = "'" + (attrs.getValue("Body")).replace("'", "''") + "'"
        #     '''
        #     # collect data into a list and put it into quesitonExecuter
        #     questionData = []
        #     questionData.append(questionId)
        #     questionData.append(acceptedAnswerId)
        #     questionData.append(questionOwnerId)
        #     questionData.append(creationDate)
        #     questionData.append(lastActivityDate)
        #     questionData.append(score)
        #     questionData.append(viewCount)
        #     questionData.append(answerCount)
        #     questionData.append(commentCount)
        #     questionData.append(favoriteCount)
        #     #questionData.append(title)
        #     #questionData.append(body)
        #     QuestionExecuter(questionData, conn, cur)
        #     print("added ", qcount, " questions into database")
 
        if ptype == '2' :
            if attrs.has_key("ParentId"):
                if (self.checkQuestion((attrs.getValue("ParentId")))):
                    global acount
                    acount += 1
                    answerId = "null"
                    if attrs.has_key("Id"):
                        answerId = int(attrs.getValue("Id"))

                    parentQuestionId = "null"
                    if attrs.has_key("ParentId"):
                        parentQuestionId = int(attrs.getValue("ParentId"))

                    answerOwnerId = "null"
                    if attrs.has_key("OwnerUserId"):
                        answerOwnerId = int(attrs.getValue("OwnerUserId"))
                    else:
                        answerOwnerId = None

                    creationDate = "null"
                    if attrs.has_key("CreationDate"):
                        creationDate = attrs.getValue("CreationDate")

                    lastActivityDate = "null"
                    if attrs.has_key("LastActivityDate"):
                        lastActivityDate = attrs.getValue("LastActivityDate")

                    score = 0
                    if attrs.has_key("Score"):
                        score = int(attrs.getValue("Score"))

                    commentCount = 0
                    if attrs.has_key("CommentCount"):
                        commentCount = int(attrs.getValue("CommentCount"))
                    '''
                    body = "null"
                    if attrs.has_key("Body"):
                        body = "'" + (attrs.getValue("Body")).replace("'", "''") + "'"
                    '''
                    # collect data into a list and put it into answerExecuter
                    answerData = []
                    answerData.append(answerId)
                    answerData.append(parentQuestionId)
                    answerData.append(answerOwnerId)
                    answerData.append(creationDate)
                    answerData.append(lastActivityDate)
                    answerData.append(score)
                    answerData.append(commentCount)
                    #answerData.append(body)
                    AnswerExecuter(answerData, conn, cur)
                    print("added ", acount, " answers into database")

    def checkQuestion (self, qusID):
        #print("Shit",qusID)
        cur = conn.cursor()
        cur.execute("SELECT Qid FROM question WHERE Qid = %s",(qusID,))
        return cur.fetchone() is not None




#Here is connect to your PostgreSQL
#Change you database, user and port here
db = "heft"
dbUser = "keshan"
dbPort = 5432
      
conn = psycopg2.connect(database=db, user=dbUser, port=dbPort)
cur = conn.cursor()
               
qcount = 0
acount = 0
#Here is the path of the raw data file
f = open('/media/keshan/66C24863C2483A17/Data/Posts.xml')
before_time = time.time()
xml.sax.parse(f, StackContentHandler())
conn.close()
cur.close()
print("Total Data Transfer time is: ", (time.time() - before_time)/60)
f.close()
