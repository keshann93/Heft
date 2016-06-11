'''
Sends info related to rank, cluster or path operations to its executors respectively
@author: keshan
'''
import os
import time

from Executor import rankExecutor as rankExe
from Executor import clusterExecutor as clusterExe
from Executor import pathExecutor as pathExe
from QueryOptimiser import queryOptimiser as queryOpt

#This array is used to store functions and its respective table name
graphQueryWithResult = dict()

#This function differentiates graph sub-queries
def graphQueryManipulationAnalyzer(executeGraphManipulationCommand, connection, cursor):
    lowerCaseGraphCommand = executeGraphManipulationCommand.lower()
    graphCommandList = executeGraphManipulationCommand.split()
    graphCommandList.reverse()  #This lets the inner sub queries to execute first
    
    #the ID illustrates the order that graph operators occur in the query
    graphOperatorID = 0
    
    graphOperationIndexMark = len(executeGraphManipulationCommand)
    for each in graphCommandList:
        if each.lower().startswith("rank("):
            graphOperatorID += 1
            
            #This avoids finding index like pagerank or other words 
            while lowerCaseGraphCommand[lowerCaseGraphCommand.rfind("rank",0, graphOperationIndexMark)-1] != ' ' or lowerCaseGraphCommand[lowerCaseGraphCommand.rfind("rank",0, graphOperationIndexMark)+4] != '(': 
                graphOperationIndexMark = lowerCaseGraphCommand.rfind("rank",0, graphOperationIndexMark)
            
            manipulationGraphIndex = lowerCaseGraphCommand.rindex("rank",0, graphOperationIndexMark)
            graphOperationIndexMark = manipulationGraphIndex
            rankManipulationCommands = getGraphManipulationQueryInfo(executeGraphManipulationCommand, manipulationGraphIndex, connection, cursor)
            
            graphManipulationQuery = queryOpt.getGraphQuery(executeGraphManipulationCommand, manipulationGraphIndex, rankManipulationCommands[-1])
            #print "graphQuery ", graphManipulationQuery  #for debug
            
            #avoid repreated graph operation again
            if graphManipulationQuery not in graphQueryWithResult:
                resultTableName = "rank_" + rankManipulationCommands[0] + str(graphOperatorID)
                rankManipulationCommands.append(resultTableName) #the last element is the result table name
                rankExe.processCommand(rankManipulationCommands, connection, cursor)
                graphQueryWithResult[graphManipulationQuery] = resultTableName
            #print "rankManipulationCommands ", rankManipulationCommands  #for debug
        
        if each.lower().startswith("cluster("):
            graphOperatorID += 1

            #This avoids finding index like clusterID or other words
            while lowerCaseGraphCommand[lowerCaseGraphCommand.rfind("cluster",0, graphOperationIndexMark)-1] != ' ' or lowerCaseGraphCommand[lowerCaseGraphCommand.rfind("cluster",0, graphOperationIndexMark)+7] != '(':  
                graphOperationIndexMark = lowerCaseGraphCommand.rfind("cluster",0, graphOperationIndexMark)
                
            manipulationGraphIndex = lowerCaseGraphCommand.rindex("cluster",0, graphOperationIndexMark)
            graphOperationIndexMark = manipulationGraphIndex
            clusterCommands = getGraphManipulationQueryInfo(executeGraphManipulationCommand, manipulationGraphIndex, connection, cursor)
            
            graphManipulationQuery = queryOpt.getGraphQuery(executeGraphManipulationCommand, manipulationGraphIndex, clusterCommands[-1])
            #print "graphQuery ", graphManipulationQuery  #for debug
            
            #not run the same graph command again
            if graphManipulationQuery not in graphQueryWithResult:
                resultTableName = "cluster_" + clusterCommands[0] + str(graphOperatorID) #the last element is tableName
                clusterCommands.append(resultTableName)
                clusterExe.processCommand(clusterCommands, connection, cursor)
                graphQueryWithResult[graphManipulationQuery] = resultTableName
            #print "clusterCommands ", clusterCommands  #for debug
        
        if each.lower().startswith("path("):
            graphOperatorID += 1

            #This avoids finding index like pathID or other words
            while lowerCaseGraphCommand[lowerCaseGraphCommand.rfind("path",0, graphOperationIndexMark)-1] != ' ' or lowerCaseGraphCommand[lowerCaseGraphCommand.rfind("path",0, graphOperationIndexMark)+4] != '(':  
                graphOperationIndexMark = lowerCaseGraphCommand.rfind("path",0, graphOperationIndexMark)
            
            manipulationGraphIndex = lowerCaseGraphCommand.rindex("path",0, graphOperationIndexMark)
            graphOperationIndexMark = manipulationGraphIndex + 4
            pathCommands = getGraphManipulationQueryInfo(executeGraphManipulationCommand, manipulationGraphIndex, connection, cursor)
            
            graphManipulationQuery = queryOpt.getGraphQuery(executeGraphManipulationCommand, manipulationGraphIndex, pathCommands[-1])
            #print "graphManipulationQuery ", graphManipulationQuery  #for debug
            
            #not run the same graph command again
            if graphManipulationQuery not in graphQueryWithResult:
                resultTableName = "path_" + pathCommands[0] + str(graphOperatorID) #the last element is tableName
                pathCommands.append(resultTableName)
                pathExe.processCommand(pathCommands, connection, cursor)
                graphQueryWithResult[graphManipulationQuery] = resultTableName
            #print "pathCommands ", pathCommands  #for debug
            
    #rewrite the query
    for eachStr in graphQueryWithResult.keys():
        executeGraphManipulationCommand = executeGraphManipulationCommand.replace(eachStr,graphQueryWithResult.get(eachStr))
    return executeGraphManipulationCommand
    

#use a list to store graph info like src, des, type of graphs, measurements/algorithms used in the operation, the related table name
def getGraphManipulationQueryInfo(executeGraphManipulationCommand, manipulationGraphIndex, connection, cursor):
    statGraphDir = os.environ['HOME'] + "/IRG_Stat_Graph/"
    tmpGraphDir = "/dev/shm/IRG_Tmp_Graph/"
    
    graphQueryInfo = []  # [graphName, graphParam, graphType, commandArray]
    
    leftBracketIndex = executeGraphManipulationCommand.index('(', manipulationGraphIndex)
    rightBracketIndex = executeGraphManipulationCommand.index(')', leftBracketIndex)
    graphManipulationCommand = (executeGraphManipulationCommand[leftBracketIndex+1:rightBracketIndex]).split(',')
    
    graphName = graphManipulationCommand[0].strip()
    graphParam = graphManipulationCommand[1].strip()
    graphQueryInfo.append(graphName)
    graphQueryInfo.append(graphParam)
    
    whereClause = executeGraphManipulationCommand[rightBracketIndex+1 :].strip()

    #query using a graph on-the-fly
    if whereClause.find(graphName,0,20) != -1:
        commandArray = queryOpt.getCoreGraphCommands(whereClause, graphName, graphParam)
        graphInfo = commandArray[0]
        graphQueryInfo.append(graphInfo[1]) #graph type
        
        if len(graphInfo) == 3:  #info about creating graph
            graphFile = open(tmpGraphDir + graphInfo[0], 'w')
            cursor.execute(graphInfo[2] + ";")
            connection.commit()
            rows = cursor.fetchall()
            startW_time = time.time()
            for i in rows:
                graphFile.write(str(i[0]) + '\t' + str(i[1]) + os.linesep)
            graphFile.close() 
            print "Graph writing time: ", time.time() - startW_time   
        else:
            raise RuntimeError, "Error creating Graph on-the-fly!!"
        
        graphQueryInfo.append(commandArray)

            
    #query using a materialized graph
    else:
        commandArray = queryOpt.getCoreGraphCommands(whereClause, "null", graphParam)

        cursor.execute("select * from pg_matviews where matviewname = '%s';" % (graphName))
        connection.commit()
        rows = cursor.fetchall()
        if len(rows) == 1: #find the mat_graph
            cursor.execute("select graphType from my_statgraphs where statgraphname = '%s';" % (graphName))
            connection.commit()
            myrow = cursor.fetchone()
            graphQueryInfo.append(myrow[0]) #get graph Type
            
            graphFile = open(statGraphDir + graphName, 'w')
            cursor.execute("select * from %s;" % (graphName))
            connection.commit()
            rows = cursor.fetchall()
            startW_time = time.time()
            for i in rows:
                graphFile.write(str(i[0]) + '\t' + str(i[1]) + os.linesep)
            graphFile.close()     
            print "Graph writing time: ", time.time() - startW_time 
            
            #print "find the view"
            graphQueryInfo.append(commandArray)
            
        else:
            raise RuntimeError, "No specified graph!!"
        
    return graphQueryInfo
        
    

    