'''
The Static Graph Processor is to use materialized views to create materialized graphs.
@author: keshan
'''
import os

#operates the my_statgraphs catalog and returns the rewritten query to PostgreSQL for execution
def processStatGraphCommand(executeGraphCommand, connection ,cursor):

    lowerCaseGraphCommand = executeGraphCommand.lower()
    #pg_tables used here gives views and provides access to useful information about each table in the database.
    cursor.execute("select * from pg_tables where tablename = 'my_statgraphs';")
    statGraphRows = cursor.fetchall()
    
    #if there is no system catalog for static graph is created, create the system catalog about static graphs
    if len(statGraphRows) == 0:
        cursor.execute("create table my_statgraphs (statgraphname text primary key, graphType text);")
    connection.commit()
    
    if "create" in lowerCaseGraphCommand: #create a static graph
        if "undirgraph" in lowerCaseGraphCommand:
            startIndex = lowerCaseGraphCommand.index("undirgraph")+len("undirgraph")
            endIndex = lowerCaseGraphCommand.index("as")
            unDirGraphName = lowerCaseGraphCommand[startIndex:endIndex].strip()
            cursor.execute("INSERT INTO my_statgraphs VALUES(%s, %s)" % ("'" + unDirGraphName + "'", "'undirgraph'"))
            connection.commit()
            if executeGraphCommand.find("undirgraph") != -1:
                return executeGraphCommand.replace("undirgraph", "materialized view")
            elif executeGraphCommand.find("UNDIRGRAPH") != -1:
                return executeGraphCommand.replace("UNDIRGRAPH", "materialized view")
            else:
                raise RuntimeError, "Given Graph type is not correct."
                
                     
        elif "digraph" in lowerCaseGraphCommand:
            startIndex = lowerCaseGraphCommand.index("dirgraph")+len("dirgraph")
            endIndex = lowerCaseGraphCommand.index("as")
            dirGraphName = lowerCaseGraphCommand[startIndex:endIndex].strip()
            cursor.execute("INSERT INTO my_statgraphs VALUES(%s, %s)" % ("'" + dirGraphName + "'", "'dirgraph'"))
            connection.commit()
            if executeGraphCommand.find("dirgraph") != -1:
                return executeGraphCommand.replace("dirgraph", "materialized view")
            elif executeGraphCommand.find("DIRGRAPH") != -1:
                return executeGraphCommand.replace("DIRGRAPH", "materialized view")
            else:
                raise RuntimeError, "Given Graph type is not correct."
        
    elif "drop" in lowerCaseGraphCommand: #drop a static graph
            startIndex = lowerCaseGraphCommand.index("graph")+len("graph")
            endIndex = lowerCaseGraphCommand.index(";")
            dropGraphName = lowerCaseGraphCommand[startIndex:endIndex].strip()
            cursor.execute("DELETE FROM my_statgraphs where statgraphname = %s" % ("'" + dropGraphName + "'"))
            connection.commit()
            if executeGraphCommand.find("undirgraph") != -1 or executeGraphCommand.find("UNDIRGRAPH") != -1:
                return (executeGraphCommand.replace("undirgraph", "materialized view")).replace("UNDIRGRAPH", "materialized view")
            elif executeGraphCommand.find("dirgraph") != -1 or executeGraphCommand.find("DIRGRAPH") != -1:
                return (executeGraphCommand.replace("dirgraph", "materialized view")).replace("DIRGRAPH", "materialized view")
            else:
                raise RuntimeError, "Given Graph type is not correct."
            graphPath = os.environ['HOME']  + "/IRG_Stat_Graph/" + graphName
            os.system("rm -fr " + graphPath)
    