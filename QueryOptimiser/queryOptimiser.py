'''
The queryOptimiser is to validate, analyze the graph sub-queries and also rewrite them.

@author: keshan
'''


# Checks the queries used to create dynamic graphs
def getCoreGraphCommands(whereClause, graphName, graphPara):
    lowerGraphWhereClause = whereClause.lower()

    graphCommandArray = [] #elements of array are [graph:mapper, node:condition,...]

    #retrieve info about dynamic graph
    if graphName != "null":
        graphConstructArray = [] #elements are string
        nameKeyIndex = whereClause.index(graphName)
        isKeyIndex = lowerGraphWhereClause.index("is", nameKeyIndex + len(graphName))
        asKeyIndex = lowerGraphWhereClause.index("as", isKeyIndex + 2)
        graphConstructType = whereClause[isKeyIndex + 2 : asKeyIndex].strip()
        graphConstructArray.append(graphName)
        graphConstructArray.append(graphConstructType)
        graphConstructArray.append(getGraphSubQuery(whereClause, asKeyIndex))
        graphCommandArray.append(graphConstructArray)

    #retrieve info about path expression
    if "/" in graphPara:
        nodes = graphPara.split("/")
        print nodes
        pointerIndex = 0
        for eachNode in nodes:
            if eachNode.isalnum():
                if eachNode[0] != 'v' and eachNode[0] != 'V':
                    raise RuntimeError, "Syntax Error in Path Expression!!"
                nodeConstruct = []
                foundNode = False
                while foundNode == False:
                    try:
                        pointerIndex = whereClause.index(eachNode)
                    except ValueError :
                        raise RuntimeError, "Syntax Error!!"
                    if (whereClause[pointerIndex-1] == " " or whereClause[pointerIndex-1] == ",") and (whereClause[pointerIndex + len(eachNode)] == " " or whereClause[pointerIndex + len(eachNode)] == ","):
                        asIndex = lowerGraphWhereClause.index("as", pointerIndex + len(eachNode))
                        nodeConstruct.append(eachNode)
                        nodeConstruct.append(getGraphSubQuery(whereClause, asIndex))
                        foundNode = True
                        graphCommandArray.append(nodeConstruct)
    return graphCommandArray

#retrieve the sub-query for creating dynamic graphs and node condition
def getGraphSubQuery(whereClause, asIndex):
    leftBracketIndexArrayQuery = []
    bracketIndexQueryPair = []
    foundIRGMapper = False
    pointerIndex = asIndex + 2

    while foundIRGMapper == False:
        pointerIndex += 1
        if whereClause[pointerIndex] == "(":
            leftBracketIndexArrayQuery.append(pointerIndex)

        if whereClause[pointerIndex] == ")":
            bracketIndexQueryPair.append((leftBracketIndexArrayQuery.pop(),pointerIndex))
            if len(leftBracketIndexArrayQuery) == 0:
                foundIRGMapper = True
    subQuery = whereClause[bracketIndexQueryPair[-1][0] + 1 : bracketIndexQueryPair[-1][1]].strip()
    return subQuery

#return the whole graph sub-query
def getGraphQuery(executeCommand, graphKeyIndex, graphCommandArray):
    lowerCaseGraphCommand = executeCommand.lower()

    queryEndKeyIndex = 0
    if lowerCaseGraphCommand[graphKeyIndex] == "p":  #for path operations
        for eachQuery in graphCommandArray:
            queryIndex = executeCommand.index(eachQuery[-1], graphKeyIndex)
            if queryIndex > queryEndKeyIndex:
                queryEndKeyIndex = queryIndex + len(eachQuery[-1])
        lastBracketKeyIndex = executeCommand.index(")", queryEndKeyIndex)
        command = executeCommand[graphKeyIndex : lastBracketKeyIndex+1]
        return command

    elif lowerCaseGraphCommand[graphKeyIndex] == "r" or lowerCaseGraphCommand[graphKeyIndex] == "c": # for rank and cluster operations
        if len(graphCommandArray) == 0:  #for materialized graph
            leftBracketKeyIndex = executeCommand.index('(', graphKeyIndex)
            rightBracketKeyIndex = executeCommand.index(')', leftBracketKeyIndex)
            command = executeCommand[graphKeyIndex:rightBracketKeyIndex+1]
            return command
        else:  #for graph on-the-fly
            queryEndIndex = executeCommand.index(graphCommandArray[0][2], graphKeyIndex) + len(graphCommandArray[0][2])
            lastBracketIndex = executeCommand.index(")", queryEndIndex)
            command = executeCommand[graphKeyIndex : lastBracketIndex+1]
            return command