'''
WebServiceClient.py
Copyright (c) Grenoble-Universite - Vesta-system.
Use, duplication or distribution is subject to authorization.
Created on 14 sept. 2016
@author Sylvain Galmiche <sylvain.galmiche@vesta-system.com>
'''

import json
import urllib.parse
import urllib.request
import datetime
WEB_SERVICE_URL = "http://37.187.134.115/VestaEnergy/Application/service/"

def sendRequest(request) :
	#print ("[REQUEST]", request)
	#help(urllib3)
	url = WEB_SERVICE_URL + urllib.parse.quote(request,'/?=')
	#print ("[URL]", url)
#	print("type request", type(urllib.request.urlopen(url)))
	return urllib.request.urlopen(url).read().decode('UTF-8')

class WebServiceClient:

	def __init__(self, login, password):
		self._login = login
		self._password = password

	def getLogin(self):
		return self.login

	def login(self):
		request = "login.php?login=" + self._login + "&password=" + self._password
		self.token = sendRequest(request)

	def getBuildingList(self) :
		request = "getBuildingList.php?token=" + self.token
		r = sendRequest(request)
		buildingList = json.loads(r)
		return buildingList
	
	def getZoneList(self, buildindId) :
		request = "getZones.php?token=" + self.token + "&building=" + buildindId
		r = sendRequest(request)
		zoneList = json.loads(r)
		return zoneList

	def getSensorList(self, buildindId) :
		request = "getSensors.php?token=" + self.token + "&building=" + buildindId
		r = sendRequest(request)
		sensorList = json.loads(r)
		return sensorList
		
	def getSensorVariableHistory(self, buildindId, serviceName, variableName, start, end) :
		request = "getSensorHistory.php?token=" + self.token + "&building=" + buildindId + "&serviceName=" + serviceName + "&variableName=" + variableName + "&start=" + start + "&end=" + end
		r = sendRequest(request)
		variableHistory = json.loads(r)
		return variableHistory

	def getVariableDataHistory(self,BUILDING_ID, var_id, START, END):
		sensorList = self.getSensorList(BUILDING_ID)
		for sensor in sensorList:
			if sensor['id']==var_id:
				serviceName = sensor["serviceName"]
				variableName = sensor["variableName"]
		variableHistory = self.getSensorVariableHistory(BUILDING_ID, serviceName, variableName, START, END)
		if variableHistory == []:
			print('NO DATA AVAILABLE FOR ', var_id, 'from : ', START, 'to : ', END)
		else:
			dateVector = list()
			valuesVector = list()
			for measurement in variableHistory:
				date = measurement["date"]
				dateVector.append(date)
				value = measurement["value"]
				valuesVector.append(value)
				#print(name, datetime.datetime.fromtimestamp(date / 1000), "->", value)
				#print(name, datetime.datetime.fromtimestamp(date / 1000).strftime('%Y-%m-%d %H:%M:%S'), "->", value)
		return ([dateVector, valuesVector])

	def getVariableDataHistory(self,BUILDING_ID, var_id, START, END):
		sensorList = self.getSensorList(BUILDING_ID)
		for sensor in sensorList:
			if sensor['id']==var_id:
				serviceName = sensor["serviceName"]
				variableName = sensor["variableName"]
		variableHistory = self.getSensorVariableHistory(BUILDING_ID, serviceName, variableName, START, END)
		if variableHistory == []:
			print('NO DATA AVAILABLE FOR ', var_id, 'from : ', START, 'to : ', END)
		else:
			dateVector = list()
			valuesVector = list()
			for measurement in variableHistory:
				date = measurement["date"]
				dateVector.append(date)
				value = measurement["value"]
				valuesVector.append(value)
				#print(name, datetime.datetime.fromtimestamp(date / 1000), "->", value)
				#print(name, datetime.datetime.fromtimestamp(date / 1000).strftime('%Y-%m-%d %H:%M:%S'), "->", value)
		return ([dateVector, valuesVector])

	def getDictionaryDataHistory(self,BUILDING_ID, var_id, START, END):
		sensorList = self.getSensorList(BUILDING_ID)
		for sensor in sensorList:
			if sensor['id']==var_id:
				serviceName = sensor["serviceName"]
				variableName = sensor["variableName"]
		variableHistory = self.getSensorVariableHistory(BUILDING_ID, serviceName, variableName, START, END)
		if variableHistory == []:
			print('NO DATA AVAILABLE FOR ', var_id, 'from : ', START, 'to : ', END)
		else:
			valuesDict=dict()
			#dateVector = list()
			#valuesVector = list()
			for measurement in variableHistory:
				date = measurement["date"]
				#dateVector.append(date)
				value = measurement["value"]
				#valuesVector.append(value)
				valuesDict[date]=value
				#print(name, datetime.datetime.fromtimestamp(date / 1000), "->", value)
				#print(name, datetime.datetime.fromtimestamp(date / 1000).strftime('%Y-%m-%d %H:%M:%S'), "->", value)
		return (valuesDict)

	def logout(self) :
		url = WEB_SERVICE_URL + "logout.php?token=" + self.token