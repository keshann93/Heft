#!/usr/bin/python
# -*- coding: UTF-8 -*-
from abc import ABCMeta, abstractmethod
import GraphType

class Graph(object):
	__metaclass__ = ABCMeta
	# @ReturnType void
	@classmethod
	def addVertex(self):
		pass

	# @ReturnType void
	@classmethod
	def addEdge(self):
		pass

	# @ParamType aPath 
	# @ParamType aConn 
	@classmethod
	def processCommand(self, aPath, aConn):
		pass

	# @ParamType aPath 
	# @ParamType aDestrow 
	# @ParamType aSourow 
	# @ParamType aLen 
	# @ParamType aConn 
	@classmethod
	def createTable(self, aPath, aDestrow, aSourow, aLen, aConn):
		pass

	@classmethod
	def __init__(self):
		# @AttributeType Dictionary
		self.___sourceId = None
		# @AttributeType Dictionary
		self.___destinationId = None
		# @AttributeType Dictionary
		self.___edgeList = None
		# @AttributeType Boolean
		self.___graphType = None

		
		

