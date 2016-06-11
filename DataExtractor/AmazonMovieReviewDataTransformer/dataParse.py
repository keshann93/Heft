'''
Before running this program, please execute the following statements in PostgreSQL to create tables.
This program is to parse the entities of the Stack Overflow network.
--------------------------------------------------------------

create table movies(
Mid int not null primary key,
Mname text);

create table reviewers(
Rid int not null primary key,
PName text);


create table reviews(
Mid int not null,
Rid int not null,
helpness text,
score int,
primary key (Mid, Rid),
foreign key (Mid) references movies (Mid),
foreign key (Rid) references reviewers (Rid)
);
--------------------------------------------------------------
@author: keshan
'''
import psycopg2
import xml.sax
import time

class movieExecutor:
    movieCount = 0
    def __init__(self, dataArray, conn, cur):
        self.movieId = dataArray[0].split(':')[-1]
        self.movieId=''.join(e for e in self.movieId if e.isdigit())[:8]
        print "Shit"+self.movieId
        if(self.movieId == ""):
            print "Empty Skip"
        else:
            if (self.checkMovie(self.movieId)):
                print "movie already exists"
            else:
                movieExecutor.movieCount += 1
                self.movieName = "Movie" + str(movieExecutor.movieCount)
                print self.movieId
                self.dataInsert(conn, cur)


    def checkMovie (self, mID):
        #print("Shit",qusID)
        cur = conn.cursor()
        cur.execute("SELECT Mid FROM movies WHERE Mid = %s",(mID,))
        return cur.fetchone() is not None

    def dataInsert(self, conn, cur):
        # connect to db
        cur.execute("INSERT INTO movies VALUES(%s, %s)" ,
                    (self.movieId, self.movieName))
        conn.commit()


class reviewerExecutor:
    def __init__(self, dataArray, conn, cur):
        self.reviwerId = dataArray[1].split(':')[-1]
        self.reviwerId=''.join(e for e in self.reviwerId if e.isdigit())[:8]
        if(self.reviwerId == ""):
            print "Empty REviewerID Skip"
        else:
            if (self.checkReviewer(self.reviwerId)):
                print "reviewer already exists"
            else:
                self.reviewerName = dataArray[2].split(':')[-1]
                self.reviewerName=''.join(e for e in self.reviewerName if e.isalnum())
                print "Reviewers"+ self.reviewerName
                self.dataInsert(conn, cur)

    def checkReviewer (self, rID):
        #print("Shit",qusID)
        cur = conn.cursor()
        cur.execute("SELECT Rid FROM reviewers WHERE Rid = %s",(rID,))
        return cur.fetchone() is not None

    def dataInsert(self, conn, cur):
        # connect to db
        cur.execute("INSERT INTO reviewers VALUES(%s, %s)",
                    (self.reviwerId, self.reviewerName))
        conn.commit()

class reviewExecutor:
    def __init__(self, dataArray, conn, cur):
        self.movieId = dataArray[0].split(':')[-1]
        self.movieId=''.join(e for e in self.movieId if e.isdigit())[:8]
        self.reviwerId = dataArray[1].split(':')[-1]
        self.reviwerId=''.join(e for e in self.reviwerId if e.isdigit())[:8]
        if(self.movieId == "" or self.reviwerId == ""):
            print "Empty Skip"
        else:
            if (self.checkReviews(self.movieId,self.reviwerId)):
                print "review already exists"
            else:
                self.helpness = dataArray[3].split(':')[-1].rstrip()
                self.score = dataArray[4].split(':')[-1]
                self.score = self.score.split('.')[0]
                self.score=''.join(e for e in self.score if e.isdigit())
                print  dataArray
                self.dataInsert(conn, cur)

    def checkReviews (self, mID, rID):
        #print("Shit",qusID)
        cur = conn.cursor()
        cur.execute("SELECT Rid FROM reviews WHERE Rid = %s AND Mid = %s",(rID,mID))
        return cur.fetchone() is not None

    def dataInsert(self, conn, cur):
        # connect to db
        cur.execute("INSERT INTO reviews VALUES(%s, %s, %s, %s)",
                    (self.movieId, self.reviwerId,self.helpness, self.score))
        conn.commit()

#Here is connect to your PostgreSQL
#Change you database, user and port here
db = "heft"
dbUser = "keshan"
dbPort = 5432

conn = psycopg2.connect(database=db, user=dbUser, port=dbPort)
cur = conn.cursor()

count = 0
rawData = []
#Here is the path of the raw data file
f = open("/media/keshan/66C24863C2483A17/Data/movies.txt")
before_time = time.time()
for eachline in f:
    count += 1
    rawData.append(eachline)
    if(eachline == '\n'):
        #movieExecutor(rawData,conn,cur)
        #reviewerExecutor(rawData,conn,cur)
        reviewExecutor(rawData,conn,cur)
        count = 0
        rawData = []


#print(rawData)


conn.close()
cur.close()
print("Total Data Transfer time is: ", (time.time() - before_time)/60)
f.close()