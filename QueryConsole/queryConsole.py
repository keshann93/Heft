'''
The query Console is used to read the query input, show query results and display error information

@author: keshan
'''

import os
import psycopg2
import time
from Tkinter import * #GUI package

from pylsy import pylsytable #for print table

import GraphProcessor
from GraphProcessor import statGraphProcessor
from QueryParser import queryParser


#starts to execute the input query
def executeQuery(connection, cursor, commandToExecute, query_Result_Text):
    lowerCaseCommand = commandToExecute.lower()

    #graph query contains rank, cluster and path operation
    if ("rank" in lowerCaseCommand) or ("cluster" in lowerCaseCommand) or ("path" in lowerCaseCommand):
        newExecuteCommand = queryParser.graphQueryManipulationAnalyzer(commandToExecute, connection, cursor)
        cursor.execute(newExecuteCommand[:]) #remove the first space
        printResult(connection, cursor, query_Result_Text)

    #query about creating or dropping a materialised graph
    elif ("create" in lowerCaseCommand or "drop" in lowerCaseCommand) and ("undirgraph" in lowerCaseCommand or "dirgraph" in lowerCaseCommand):
        newExecuteCommand = GraphProcessor.statGraphProcessor.processStatGraphCommand(commandToExecute, connection, cursor)
        cursor.execute(newExecuteCommand[:]) #remove the first space
        connection.commit()
        query_Result_Text.insert(INSERT, "Graph Manipulation is Done")
        query_Result_Text.config(state=DISABLED)

    #normal relational query without any graph functions
    else:
        #print commandToExecute[:]
        cursor.execute(commandToExecute[:])  #remove the first space
        printResult(connection, cursor, query_Result_Text)

#prints results of query execution
def printResult(connection, cursor, query_Result_Text):
    print "Results Printing"
    if(cursor.description == None):
        connection.commit()
        query_Result_Text.insert(INSERT, cursor.statusmessage)
    else:
        tabcolnames = [description[0] for description in cursor.description]
        table = pylsytable(tabcolnames)
        rows = cursor.fetchall()
        tabcol_index = 0
        row_num = 0
        for i in rows:
            row_num += 1
            for each in i:
                print str(each) + '\t',
                table.append_data(tabcolnames[tabcol_index], str(each))
                tabcol_index += 1
            print
            tabcol_index = 0

            #printing large result set using pylsytable problem is solved
            if row_num == 100:  #a new table is created to print for every 100lines
                query_Result_Text.insert(INSERT, table)
                row_num = 0
                table = pylsytable(tabcolnames)

        if row_num != 0:
            query_Result_Text.insert(INSERT, table)
        connection.commit()
        query_Result_Text.config(state=DISABLED)

#This function initiates the query console process
def start(input_Query, result_Panel_Text):

    homeDirectory = os.environ['HOME']
    dynamicDirectory = "/dev/shm"

    #folder creation where materialized graphs get created
    if os.path.exists(homeDirectory + "/IRG_Stat_Graph") == False:
        os.mkdir(homeDirectory + "/IRG_Stat_Graph")
    #folder creation where dynamic graphs get created and manipulated
    if os.path.exists(dynamicDirectory + "/IRG_Tmp_Graph") == False:
        os.mkdir(dynamicDirectory + "/IRG_Tmp_Graph")

    #Here is connect to your PostgreSQL
    db = "heft"
    dbUser = "keshan"
    dbPort = 5432
    connection = psycopg2.connect(database=db, user=dbUser, port=dbPort)
    cursor = connection.cursor()

    result_Panel_Text.config(state=NORMAL)
    start_time = time.time()
    try:
        executeQuery(connection, cursor, input_Query, result_Panel_Text)
        print "Total query time is: ", (time.time() - start_time)
        os.system("rm -fr /dev/shm/IRG_Tmp_Graph/*")  #This clears graphs created on-the-fly
        queryParser.graphQueryWithResult.clear()  #clear the dictionary of parser's for result table names and graph sub-queries
    except psycopg2.ProgrammingError as reason:
        result_Panel_Text.insert(INSERT, str(reason))
        result_Panel_Text.config(state=DISABLED)
    except psycopg2.DataError as reason:
        result_Panel_Text.insert(INSERT, str(reason))
        result_Panel_Text.config(state=DISABLED)
    except psycopg2.DatabaseError as reason:
        result_Panel_Text.insert(INSERT, str(reason))
        result_Panel_Text.config(state=DISABLED)
    finally:
        cursor.close()
        connection.close()
