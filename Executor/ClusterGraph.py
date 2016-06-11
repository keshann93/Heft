#!/usr/bin/python
# -*- coding: UTF-8 -*-
import Graph
import graph_tool.all as gt


class ClusterGraph(Graph):
    # @ParamType aAlgo
    # @ParamType aGraph
    # @ParamType aClusComm
    # @ParamType aConn
    def commDete(self, aAlgo, aGraph, aClusComm, aConn):
        pass

    # @ParamType aClusComm
    # @ParamType aGraph
    # @ParamType aConn
    def connComp(self, aClusComm, aGraph, aConn):
        pass

    # @ParamType aClusComm
    # @ParamType aGraph
    # @ParamType aConn
    def stroConnComp(self, aClusComm, aGraph, aConn):
        pass

    def __init__(self):
        self.___clusterId = None
        self.___size = None
        self.___members = None

    def connCompute(self, graphTxtName, isDirected):
        f = open(graphTxtName)
        self.idIndexDict = dict()
        self.indexIdDict = dict()
        self.g = gt.Graph(directed= isDirected)
        index = 0
        edge_list = []
        for each in f:
            if each.startswith("#"):
                continue
            strPair = each.strip().split()
            sourceId = int(strPair[0])
            targetId = int(strPair[1])
            if sourceId not in self.idIndexDict:
                self.g.add_vertex()
                self.idIndexDict[sourceId] = index
                self.indexIdDict[index] = sourceId
                index += 1
            if targetId not in self.idIndexDict:
                self.g.add_vertex()
                self.idIndexDict[targetId] =  index
                self.indexIdDict[index] = targetId
                index += 1

            if ((sourceId, targetId) not in edge_list):
                if (isDirected == False):
                    edge_list.append((sourceId, targetId))
                    edge_list.append((targetId, sourceId))
                    self.g.add_edge(self.g.vertex(self.idIndexDict[sourceId]), self.g.vertex(self.idIndexDict[targetId]))
                else:
                    edge_list.append((sourceId, targetId))
                    self.g.add_edge(self.g.vertex(self.idIndexDict[sourceId]), self.g.vertex(self.idIndexDict[targetId]))
  