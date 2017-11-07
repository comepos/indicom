__author__ = 'Yanis Hadj Said'
"""
le back-end contient la base des types, le conteneur de données, la définition du créateur de base de données ainsi que la classe de manipulation

"""
import sqlite3
import csv
import numpy
import timemg
import os
import random
import string
import time
import tkinter as tk
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from webServiceClientVestaForPosA import WebServiceClient
import pandas as pds
from math import *
import calendar

def filter_outliersV0(variable_id: int, values: list):
    differences = list()
    for k in range(1, len(values)):
        # calcul différences
        differences.append(abs(values[k] - values[k - 1]))
    # ordonnacement
    differences.sort()
    differences.append(0)
    outlierDataDetected = None
    #print("main differences in", variable_id, differences[int(0.8 * len(values)): len(values) - 1])
    for k in range(int(0.8 * len(values)), len(values) - 1):
        if differences[k - 1] == 0:
            if numpy.std(values)>=3:
                values=len(values)*[0]
        elif abs((differences[k] - differences[k - 1]) / differences[k - 1]) >= 5:
            outlierDataDetected = differences[k]
            print("outlierDataDetected in", variable_id, "\n ----->differences[k+1]", differences[k+1],"\n ----->differences[k]", differences[k], "\n ----->differences[k-1]", differences[k - 1])
    print("\n -----> outlierData value", outlierDataDetected)
    if not not outlierDataDetected:
        print('$$$$$outlierDataDetected considered')
        for k in range(1, len(values)):
            dfr = abs(values[k] - values[k - 1])
            if values[k - 1] == 0:
                values[k]=0
            elif dfr >= 0.9 * outlierDataDetected:
                for j in range(k, len(values)):
                    if abs(values[j] - values[j - 1]) >= 0.9 * outlierDataDetected:
                        print("outlierData replaced values", k - j)
                        for i in range(k, j):
                            values[i] = values[i - 1]

def filter_outliers(label, variable_id: int,fullDateTime: list, data_values: list):
    #calcul des références de trie
    #Le filtre
    if len(fullDateTime) == 0 :
        pass
    else:
        fullDateTimeBegin=fullDateTime[0]
        fullDateTimeEnd=fullDateTime[len(fullDateTime)-1]
        sortedValues = sorted(data_values)
        referenceMean = numpy.mean(sortedValues[0:int(0.8 * len(sortedValues))])
        referenceStd = numpy.std(sortedValues[0:int(0.8 * len(sortedValues))])
        print('>>>>>------- Filtering function for :',label,'------<<<<< \n', 'referenceMean=', referenceMean, 'referenceStd=',
              referenceStd,'\n', 'min value', sortedValues[0], 'max value', sortedValues[int(0.8 * len(sortedValues))],' absolute max value', sortedValues[len(sortedValues)-1])

        #création du dataframe
        rightDateFormat = pds.to_datetime(fullDateTime, dayfirst=True)
        df = pds.DataFrame(data_values, index=rightDateFormat, columns=['valeurs'])

        df.index.names = ['Date']
        #df=df[((df.valeurs >= referenceMean - 1.96 * referenceStd) & (df.valeurs <= referenceMean + 1.96 * referenceStd))]
        df = df[((df.valeurs >= referenceMean - 2.8 * referenceStd) & (df.valeurs <= referenceMean + 2.8 * referenceStd))]

        # df['duplicated']=df.duplicated('index')
        df['index'] = df.index
        df=df.drop_duplicates('index')
        fullDateTime = list()
        data_values = list()
        for element in df.index:
            fullDateTime.append(element)
            data_values.append(float(df.at[element, 'valeurs']))

        return fullDateTime, data_values

def resample_data(data_epochtimesms: list, data_values: list, timequantum_duration_in_secondes: int, starting_epochtimems: int, ending_epochtimems: int):

    augmented_data_epochtimesms = [timemg.epochtimems_to_timequantum(starting_epochtimems, timequantum_duration_in_secondes)]
    augmented_data_values = [data_values[0]]
    for i in range(len(data_epochtimesms)):
        data_epochtimems = data_epochtimesms[i]
        for epochtimems in range(timemg.epochtimems_to_timequantum(augmented_data_epochtimesms[-1], timequantum_duration_in_secondes) + timequantum_duration_in_secondes * 1000, timemg.epochtimems_to_timequantum(data_epochtimems, timequantum_duration_in_secondes), timequantum_duration_in_secondes * 1000):
            augmented_data_epochtimesms.append(epochtimems)
            augmented_data_values.append(augmented_data_values[-1])
        if timemg.epochtimems_to_timequantum(data_epochtimems, timequantum_duration_in_secondes) > timemg.epochtimems_to_timequantum(augmented_data_epochtimesms[-1], timequantum_duration_in_secondes):
            augmented_data_epochtimesms.append(timemg.epochtimems_to_timequantum(data_epochtimems, timequantum_duration_in_secondes))
            augmented_data_values.append(augmented_data_values[-1])
        augmented_data_epochtimesms.append(data_epochtimems)
        augmented_data_values.append(data_values[i])
    for epochtimems in range(timemg.epochtimems_to_timequantum(augmented_data_epochtimesms[-1], timequantum_duration_in_secondes), timemg.epochtimems_to_timequantum(ending_epochtimems, timequantum_duration_in_secondes), timequantum_duration_in_secondes * 1000):
        augmented_data_epochtimesms.append(epochtimems + timequantum_duration_in_secondes * 1000)
        augmented_data_values.append(augmented_data_values[-1])

    sampled_data_epochtimes, sampled_data_values = [], []
    integrator = 0
    cumulateurTemps = 0
    virginIntegrator = 0
    previous_datasample_epochtimemsvect=list()
    for i in range(1, len(augmented_data_epochtimesms)):
        previous_epochtimems = augmented_data_epochtimesms[i - 1]
        previous_datasample_epochtimems = timemg.epochtimems_to_timequantum(augmented_data_epochtimesms[i - 1], timequantum_duration_in_secondes)
        previous_datasample_epochtimemsvect.append(previous_datasample_epochtimems)
        epochtimems = augmented_data_epochtimesms[i]
        datasample_epochtime = timemg.epochtimems_to_timequantum(augmented_data_epochtimesms[i], timequantum_duration_in_secondes)
        previous_value = augmented_data_values[i - 1]
        integrator += (epochtimems - previous_epochtimems) * previous_value
        cumulateurTemps+=epochtimems - previous_epochtimems
        #virginIntegrator += previous_value
        #print(cumulateurTemps/1000,': avant condition', timemg.epochtimems_to_datetime(previous_epochtimems),timemg.epochtimems_to_datetime(previous_datasample_epochtimems), 'virginIntegrator',integrator,'epochtimems',timemg.epochtimems_to_datetime(epochtimems))
        if datasample_epochtime > previous_datasample_epochtimems:
            #print(cumulateurTemps/1000,': milieu condition', timemg.epochtimems_to_datetime(previous_epochtimems), 'Integrator',integrator / (epochtimems - previous_datasample_epochtimems))
            if timemg.epochtimems_to_timequantum(starting_epochtimems, timequantum_duration_in_secondes) <= previous_datasample_epochtimems < timemg.epochtimems_to_timequantum(ending_epochtimems, timequantum_duration_in_secondes) and previous_datasample_epochtimems:
                #print(cumulateurTemps/1000,': après condition',timemg.epochtimems_to_datetime(previous_datasample_epochtimems),'Integrator',integrator / (timequantum_duration_in_secondes * 1000),)
                sampled_data_epochtimes.append(previous_datasample_epochtimems)
                sampled_data_values.append(integrator / (timequantum_duration_in_secondes * 1000))
                #sampled_data_values.append(integrator)
            integrator = 0
            cumulateurTemps = 0
    count=0
    for temps in sampled_data_epochtimes:
        #print('*****AAAAAAAAAAAAAAA*****', timemg.epochtimems_to_stringdate(temps),sampled_data_values[count])
        count=count+1
    return sampled_data_epochtimes, sampled_data_values

def count_data(data_epochtimesms: list, data_values: list, timequantum_duration_in_secondes: int, starting_epochtimems: int, ending_epochtimems: int):
    counters_dict = dict()
    for i in range(len(data_epochtimesms)):
        if data_values[i] != 0:
            datasample = timemg.epochtimems_to_timequantum(data_epochtimesms[i], timequantum_duration_in_secondes)
            if datasample in counters_dict:
                counters_dict[datasample] += 1
            else:
                counters_dict[datasample] = 1
    sampled_data_epochtimes = [datasample for datasample in range(timemg.epochtimems_to_timequantum(starting_epochtimems, timequantum_duration_in_secondes), timemg.epochtimems_to_timequantum(ending_epochtimems, timequantum_duration_in_secondes), timequantum_duration_in_secondes * 1000)]
    sampled_data_values = []
    for i in range(len(sampled_data_epochtimes)):
        if sampled_data_epochtimes[i] in counters_dict:
            sampled_data_values.append(counters_dict[sampled_data_epochtimes[i]])
        else:
            sampled_data_values.append(0)
    return sampled_data_epochtimes, sampled_data_values


class DataBaseCreator():
    """
    La classe qui permet la création d'une base de données, le parsage des sources de données et leur transcription dans la base
    """
    def __init__(self):
        self.variableCounter=0
    def createExtractionDatabase(self,databaseName: str, site: str, building: str):
        self.site=site
        self.building=building
        self.device="device"
        try:
            os.remove(databaseName)
        except OSError:
            pass
        connection = sqlite3.connect(databaseName)
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE variable (id INTEGER, name TEXT, site TEXT, building TEXT, zone TEXT, device TEXT,
            PRIMARY KEY (id))''')
        cursor.execute('''CREATE TABLE data (variableidref INTEGER, epochtimeinms INTEGER, value REAL, fulldate TEXT,
            PRIMARY KEY (variableidref,epochtimeinms),
            FOREIGN KEY (variableidref) REFERENCES variable(id))''')
        cursor.execute('''CREATE UNIQUE INDEX 'epochinmsidx' ON 'data' (variableidref ASC, epochtimeinms ASC)''')
        connection.commit()
        connection.close()

    def fillDatabaseFromPredisMhiCSV(self, databaseName: str, sourceFileName: str, variableName: str, fromdate: str, todate: str=None):
        self.variableName = variableName
        connection = sqlite3.connect(databaseName)
        cursor = connection.cursor()
        cursor.execute("INSERT INTO variable VALUES(%i,'%s','%s','%s','%s','%s')" % (self.variableCounter, variableName, self.site, self.building, self.zone, self.device))
        name = self.site + ':' + self.building + ':' + ':' + self.zone + ':' + self.device + ':' + variableName
        print(name)
        variabledataset = VariableDatasetFromAree(self.site, self.building, self.zone, self.device, self.variableName, sourceFileName, fromdate, todate)
        timesInMs = variabledataset.getEpochTimes()
        values = variabledataset.getValues()
        for i in range(len(timesInMs)):
            try:
                cursor.execute("INSERT INTO data VALUES(%i,%i,%f,'%s')" % (self.variableCounter, timesInMs[i], values[i], timemg.epochtimems_to_stringdate(timesInMs[i])))
                #if variableName != 'productionPV':
                    #print("\n valeurs chargées dans la BD",self.variableCounter,variableName, values[i],timemg.epochtimems_to_stringdate(timesInMs[i]))
            except:
                print('>> Sample %s already in database' % timemg.epochtimems_to_stringdate(timesInMs[i]))
        self.variableCounter += 1
        connection.commit()
        connection.close()

    def fillVariableInDatabaseFromPosAWebServiceDictionaryTech(self, databaseName: str, dbZone: str, dbVariableName: str, sensorIsToPickUp:str, fromdate: str, todate: str = None):
        LOGIN = 'yhadjsaid'
        PASSWORD = 'zupPLKWZ'
        webServiceClient = WebServiceClient(LOGIN, PASSWORD)
        webServiceClient.login()
        # get building list
        #buildingList = webServiceClient.getBuildingList()
        print("Building list :")
        self.BUILDING_ID = '0ea237f6-3764-498e-807a-de7c57b559f0'
        values =webServiceClient.getDictionaryDataHistory(self.BUILDING_ID, sensorIsToPickUp, fromdate, todate)
        dataFrame=pds.DataFrame(data=values, index=[1])
        print(dataFrame.describe())

        # logout
        webServiceClient.logout()

        connection = sqlite3.connect(databaseName)
        cursor = connection.cursor()
        cursor.execute("INSERT INTO variable VALUES(%i,'%s','%s','%s','%s','%s')" % (self.variableCounter, dbVariableName, self.site, self.building, dbZone, self.device))
        name = self.site + ':' + self.building + ':' + ':' + dbZone + ':' + self.device + ':' + dbVariableName
        print(name)

        for epochTime in values:
            try:
                cursor.execute("INSERT INTO data VALUES(%i,%i,%f,'%s')" % (self.variableCounter, epochTime, values[epochTime], timemg.epochtimems_to_stringdate(epochTime)))

            except:
                print('>> Sample %s already in database' % timemg.epochtimems_to_stringdate(epochTime))
        self.variableCounter += 1
        connection.commit()
        connection.close()
        return (self.variableCounter-1,dbVariableName)


    def fillVariableInDatabaseFromPosAWebService(self, databaseName: str, dbZone: str, dbVariableName: str,
                                                 sensorIsToPickUp: str, fromdate: str, todate: str = None):
        LOGIN = 'stephane.ploix'
        PASSWORD = 'malone38'
        webServiceClient = WebServiceClient(LOGIN, PASSWORD)
        webServiceClient.login()
        # get building list
        # buildingList = webServiceClient.getBuildingList()
        print("Building list :")
        self.BUILDING_ID = '0ea237f6-3764-498e-807a-de7c57b559f0'
        epochTime, values = webServiceClient.getVariableDataHistory(self.BUILDING_ID, sensorIsToPickUp, fromdate, todate)

        laps=list()
        var=list()
        for inx in range(0,len(epochTime)):
            laps.append((epochTime[inx] - epochTime[inx-1])/6000)
            var.append(values[inx] - values[inx-1])


        epochTimedataFrame = pds.DataFrame(data=laps)
        print("epochTimedataFrame------>",epochTimedataFrame.describe())
        valuesdataFrame = pds.DataFrame(data=var)
        print("valuesdataFrame------> \n",valuesdataFrame.describe())

        # logout
        webServiceClient.logout()

        connection = sqlite3.connect(databaseName)
        cursor = connection.cursor()
        cursor.execute("INSERT INTO variable VALUES(%i,'%s','%s','%s','%s','%s')" % (
        self.variableCounter, dbVariableName, self.site, self.building, dbZone, self.device))
        name = self.site + ':' + self.building + ':' + ':' + dbZone + ':' + self.device + ':' + dbVariableName
        print(name)

        for indx in range (0, len(values)):
            try:
                cursor.execute("INSERT INTO data VALUES(%i,%i,%f,'%s')" % (
                self.variableCounter, epochTime[indx], values[indx], timemg.epochtimems_to_stringdate(epochTime[indx])))

            except:
                print('>> Sample %s already in database' % timemg.epochtimems_to_stringdate(epochTime[indx]))
        self.variableCounter += 1
        connection.commit()
        connection.close()
        return (self.variableCounter - 1, dbVariableName)

    def fillVariableInDatabaseFromCSVFile(self, databaseName: str, dbZone: str, dbVariableName: str,variableSourceFileName: str):
        with open(variableSourceFileName, "r", encoding='latin-1') as csvfile:
            rowReader = csv.reader(csvfile, delimiter=';', quoting=csv.QUOTE_NONE)
            values=list()
            epochTime=list()
            for row in rowReader:
                try:
                    epochTime.append(timemg.stringdate_to_epochtimems(row[0]))
                    values.append(float(row[1].replace(',','.')))
                except:
                    print('escaping...')
                    pass

        connection = sqlite3.connect(databaseName)
        cursor = connection.cursor()
        cursor.execute("INSERT INTO variable VALUES(%i,'%s','%s','%s','%s','%s')" % (
            self.variableCounter, dbVariableName, self.site, self.building, dbZone, self.device))
        name = self.site + ':' + self.building + ':' + ':' + dbZone + ':' + self.device + ':' + dbVariableName
        print(name)

        for indx in range(0, len(values)):
            try:
                cursor.execute("INSERT INTO data VALUES(%i,%i,%f,'%s')" % (
                    self.variableCounter, epochTime[indx], values[indx],
                    timemg.epochtimems_to_stringdate(epochTime[indx])))

            except:
                print('>> Sample %s already in database' % timemg.epochtimems_to_stringdate(epochTime[indx]))
        connection.commit()
        connection.close()
        self.variableCounter += 1
        return (self.variableCounter - 1, dbVariableName)

    def fillVariableInDatabaseFromCSVFileForHanau(self, databaseName: str, dbZone: str, dbVariableName: str,variableSourceFileName: str):
        with open(variableSourceFileName, "r", encoding='latin-1') as csvfile:
            rowReader = csv.reader(csvfile, delimiter=';', quoting=csv.QUOTE_NONE)
            values=list()
            epochTime=list()
            for row in rowReader:
                try:
                    epochTime.append(timemg.separatedStringDate_to_epochtimems(row[0],row[1],row[2],row[3],row[4],row[5]))
                    values.append(float(row[6].replace(',','.'))/1000)
                except:
                    print('escaping...')
                    pass

        connection = sqlite3.connect(databaseName)
        cursor = connection.cursor()
        cursor.execute("INSERT INTO variable VALUES(%i,'%s','%s','%s','%s','%s')" % (
            self.variableCounter, dbVariableName, self.site, self.building, dbZone, self.device))
        name = self.site + ':' + self.building + ':' + ':' + dbZone + ':' + self.device + ':' + dbVariableName
        print(name)

        for indx in range(0, len(values)):
            try:
                cursor.execute("INSERT INTO data VALUES(%i,%i,%f,'%s')" % (
                    self.variableCounter, epochTime[indx], values[indx],
                    timemg.epochtimems_to_stringdate(epochTime[indx])))

            except:
                print('>> Sample %s already in database' % timemg.epochtimems_to_stringdate(epochTime[indx]))
        connection.commit()
        connection.close()
        self.variableCounter += 1
        return (self.variableCounter - 1, dbVariableName)

class VariableDatasetFromAree():

    def __init__(self, site: str, building: str, zone: str, device: str, variableName: str,sourceFileName: str, startdate: str, enddate: str=None):  # date format: '17/02/2015 00:00:00':
        self.site = site
        self.building = building
        self.zone = zone
        self.device = device
        self.variableName = variableName
        self.values = dict()
        self.epochtimes, self.values=self.csvFromAree_extract(self.variableName,sourceFileName)

        self.epochtimes.sort()

    def csvFromAree_extract(self, variableName: str,sourceFileName: str):
        with open(sourceFileName, "r", encoding='latin-1') as csvfile:
            rowReader = csv.reader(csvfile, delimiter=';', quoting=csv.QUOTE_NONE)
            filtredData = dict()
            epochTimeList=list()
            for row in rowReader:
                try:
                    epochTime = timemg.stringdate_to_epochtimems(row[0])
                    epochTimeList.append(epochTime)
                    filtredData[epochTime] = float(row[1])
                    #if 'DEM_40-TGBT' in sourceFileName:
                     #   print("§§§§§§§dataRead",filtredData[epochTime],timemg.epochtimems_to_stringdate(epochTime),row[0])
                except:
                    pass
                if '#' in row[0]:
                    variableNameFromTheFile = row[0].split('#')[1]
                    #print('variableName',variableNameFromTheFile)
        #if input("is the variableNameFromTheFile: "+variableNameFromTheFile+"  is cohérent with : "+variableName +"  please tell 'yes' or 'no'"):
        #print("************",filtredData)
        return epochTimeList, filtredData

    def getEpochTimes(self):
        return self.epochtimes

    def getValue(self, epochtime):
        return self.values[epochtime]

    def getValues(self, epochtimes=None):
        if epochtimes is None:
            epochtimes = self.epochtimes
        return [self.values[epochtime] for epochtime in epochtimes]

class VariableDataset():

    def __init__(self, site: str, building: str, zone: str, device: str, variableName: str, epochtimes, values, startdate: str, enddate: str=None):  # date format: '17/02/2015 00:00:00':
        self.site = site
        self.building = building
        self.zone = zone
        self.device = device
        self.variableName = variableName
        self.values = dict()
        self.epochtimes=epochtimes
        self.values=values

        self.epochtimes.sort()


    def getEpochTimes(self):
        return self.epochtimes

    def getValue(self, epochtime):
        return self.values[epochtime]

    def getValues(self, epochtimes=None):
        if epochtimes is None:
            epochtimes = self.epochtimes
        return [self.values[epochtime] for epochtime in epochtimes]

class DBvariable:

    def __init__(self):
        self.id = None
        self.name = None

    def get_id(self):
        return self.id

    def get_name(self):
        return self.name

    def __str__(self):
        raise NotImplementedError

class VestaFormatSensorVariable(DBvariable):

    def __init__(self, record: list):
        """
        internal representation of a variable in Vesta-System formalism
        :param record: a record from database
        """
        DBvariable.__init__(self)
        self.id, self.name, self.site, self.building, self.zone, self.device = record

    def __str__(self):
        """
        string representing the variable
        """
        return 'variable %i [%s]:%s in %s/%s/%s' % (self.id, self.name, self.device, self.zone, self.building, self.site)

class DataBaseHandler:
    """
    Cette classe établie le lien avec la base de données Sqlite. elle permet aussi la récupération des données rééchantillonnées et filtrés
    """

    def __init__(self, name: str, starting_stringdatetime: str=None, ending_stringdatetime: str=None):
        self.name = name
        self.connection = sqlite3.connect(name)
        cursor = self.connection.cursor()
        self.earliest_epochtimems = int(cursor.execute('SELECT min(epochtimeinms) FROM data').fetchone()[0])
        self.latest_epochtimems = int(cursor.execute('SELECT max(epochtimeinms) FROM data').fetchone()[0])

        if starting_stringdatetime is not None:
            self.starting_epochtimems = timemg.stringdate_to_epochtimems(starting_stringdatetime)
        else:
            self.starting_epochtimems = self.earliest_epochtimems
        if ending_stringdatetime is not None:
            self.ending_epochtimems = timemg.stringdate_to_epochtimems(ending_stringdatetime)
        else:
            self.ending_epochtimems = self.latest_epochtimems
        self.variables = self._get_variables()
        self.dataHorizonInMs= self.ending_epochtimems-self.starting_epochtimems
    def get_name(self):
        return self.name

    def _get_variables(self):
        cursor = self.connection.cursor()
        variables = list()
        for record in cursor.execute('SELECT id, name, site, building, zone, device FROM variable ORDER BY id ASC'):
            variables.append(VestaFormatSensorVariable(record))
        return variables

    def set_starting_stringdatetime(self, starting_stringdatetime: str):
        self.starting_epochtimems = timemg.stringdate_to_epochtimems(starting_stringdatetime)

    def set_ending_stringdatetime(self, ending_stringdatetime: str):
        self.ending_epochtimems = timemg.stringdate_to_epochtimems(ending_stringdatetime)

    def close(self):
        """
        close database
        """
        self.connection.close()

    def get_raw_measurements_old(self, variable_id: int, remove_outliers: bool=True, outlier_sensitivity: float=5, min_value: float=None, max_value: float=None):
        """
        return raw measurements from database in between constructor specified starting and ending times
        :param variable_id: identifier (int) of the variable to extract
        :param remove_outliers: True to remove outliers (default is True)
        :param outlier_sensitivity: outlier detection sensitivity (defaut is 5)
        :param max_value: maximum acceptable value (default is None i.e. no filtering)
        :return: raw measurements
        """

        cursor = self.connection.cursor()
        cursor.execute('SELECT epochtimeinms, value FROM data WHERE variableidref=="%i" AND epochtimeinms>=%i AND epochtimeinms<=%i ORDER BY epochtimeinms ASC' % (variable_id, self.starting_epochtimems, self.ending_epochtimems))
        epochtimesms, values = [], []
        for record in cursor.fetchall():
            epochtimesms.append(int(record[0]))
            values.append(record[1])
        if variable_id == 0:
            count = 0
            for temps in epochtimesms:
                #print('%%%%%%%%%%%%%gsmBefore outlier', timemg.epochtimems_to_datetime(temps), values[count])
                count = count + 1
        if remove_outliers:
            filter_outliers(variable_id, epochtimesms, values, outlier_sensitivity, min_value, max_value)
        if variable_id == 0:
            count = 0
            for temps in epochtimesms:
                #print('%%%%%%%%%%%%%gsmafter outlier', timemg.epochtimems_to_datetime(temps), values[count])
                count = count + 1
        return epochtimesms, values

    def get_raw_measurements(self, variable_id: int):
        """
        return raw measurements from database in between constructor specified starting and ending times
        :param variable_id: identifier (int) of the variable to extract
        :param remove_outliers: True to remove outliers (default is True)
        :param outlier_sensitivity: outlier detection sensitivity (defaut is 5)
        :param max_value: maximum acceptable value (default is None i.e. no filtering)
        :return: raw measurements
        """

        cursor = self.connection.cursor()
        cursor.execute('SELECT epochtimeinms, value FROM data WHERE variableidref=="%i" AND epochtimeinms>=%i AND epochtimeinms<=%i ORDER BY epochtimeinms ASC' % (variable_id, self.starting_epochtimems, self.ending_epochtimems))
        epochtimesms, values = [], []
        for record in cursor.fetchall():
            epochtimesms.append(int(record[0]))
            values.append(record[1])
        if variable_id == 0:
            count = 0
            for temps in epochtimesms:
                #print('%%%%%%%%%%%%%gsmBefore outlier', timemg.epochtimems_to_datetime(temps), values[count])
                count = count + 1
        if variable_id == 0:
            count = 0
            for temps in epochtimesms:
                #print('%%%%%%%%%%%%%gsmafter outlier', timemg.epochtimems_to_datetime(temps), values[count])
                count = count + 1
        return epochtimesms, values

    def get_raw_measurements_with_fullDate(self, variable_id: int):
        """
        return raw measurements from database in between constructor specified starting and ending times
        :param variable_id: identifier (int) of the variable to extract
        :param remove_outliers: True to remove outliers (default is True)
        :param outlier_sensitivity: outlier detection sensitivity (defaut is 5)
        :param max_value: maximum acceptable value (default is None i.e. no filtering)
        :return: raw measurements
        """

        cursor = self.connection.cursor()
        cursor.execute('SELECT fulldate, value FROM data WHERE variableidref=="%i" AND epochtimeinms>=%i AND epochtimeinms<=%i ORDER BY epochtimeinms ASC' % (variable_id, self.starting_epochtimems, self.ending_epochtimems))
        fullTimeDate, values = [], []
        for record in cursor.fetchall():
            fullTimeDate.append(record[0])
            values.append(record[1])

        return fullTimeDate, values

    def get_filtred_measurements(self, variable_id: int):
        fullDateTime, data_values=self.get_raw_measurements_with_fullDate(variable_id)
        #values[15]=153730
        filter_outliers(variable_id,fullDateTime, data_values)
        return fullDateTime, data_values

    def get_sampled_measurements(self, variable_id: int, timequantum_duration_in_secondes: int=1800, remove_outliers: bool=True, outlier_sensitivity: float=5, min_value: float=None, max_value: float=None):
        """
        return resample measurements from database in between constructor specified starting and ending times
        :param variable_id: identifier (int) of the variable to extract
        :param timequantum_duration_in_secondes: duration of the time quantum (sample period) in secondes (default is 1800s i.e. 30 min)
        :param remove_outliers: True to remove outliers (default is True)
        :param outlier_sensitivity: outlier detection sensitivity (defaut is 5)
        :param max_value: maximum acceptable value (default is None i.e. no filtering)
        :return: resampled measurements
        """
        data_epochtimesms, data_values = self.get_raw_measurements(variable_id)

        if len(data_epochtimesms) == 0:
            return None
        return resample_data(data_epochtimesms, data_values, timequantum_duration_in_secondes, self.starting_epochtimems, self.ending_epochtimems)

    def get_sampled_measurements_with_pandas(self, variable_id: int, period: str = 'H'):
        """
        H : pas horaire
        D : calendaire
        W : hebdomadaire
        M : mensuel
        """
        fullDateTime, data_values = self.get_raw_measurements_with_fullDate(variable_id)

        if len(fullDateTime) == 0:
            return None
        else:
            rightDateFormat=pds.to_datetime(fullDateTime)
            df=pds.DataFrame(data_values, index=rightDateFormat)
            df.index.names =['Date']
            df['index'] = df.index
            #df['duplicated']=df.duplicated('index')
            df1 = df.drop_duplicates('index')
            df1.set_index = df1['index']
            df1 = df1.drop('index', axis=1)

            newDF = df1.resample(period).pad()  # voir :
            # H : pas horaire
            # D : calendaire
            # W : hebdomadaire
            # M : menuel

            #  http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases
            #print("-------->resampler", type(resampler), "<---------\n")
            # pad() prend les points aux horaires choisies. Si pas existants garde l'instance d'avant (bloqueur d'ordre Zero)
            # asfreq()prend les points aux horaires choisies. Si pas existants interpolation linéaire (bloqueur d'ordre 1)
            # sum() prend la somme des valeurs dans l'heure et fait une interpolation linéaire pour les valeurs manquantes
            # bfill() prend la valeur de l'ittération (k+1) avec un bloqueur d'ordre zero en cas d'absence de données
            #print("-------->newDF", newDF, "<---------\n")
            #print('Sauvegarde dans le fichier output.csv''...')



            indx=list()
            val=list()
            for element in newDF.index:
                indx.append(timemg.pdsTimestamp_to_epochtimems(str(element)))
                val.append(float(newDF.at[element,0]))

        return indx, val, newDF

    def get_sampled_and_filtred_measurements_with_pandas(self, variable_id: int, label, period: str = 'H'):
        """
        H : pas horaire
        D : calendaire
        W : hebdomadaire
        M : mensuel
        :param label:
        """
        if period =='T':
            expectedNumberOfRows=floor(self.ending_epochtimems/(1000*60))-floor(self.starting_epochtimems/(1000*60))+1
        if period == '10T':
            expectedNumberOfRows = floor(self.ending_epochtimems / (1000 * 600)) - floor(
                self.starting_epochtimems / (1000 * 600))+1
        if period =='H':
            expectedNumberOfRows=floor(self.ending_epochtimems/(1000*3600))-floor(self.starting_epochtimems/(1000*3600))+1
        elif period =='D':
            expectedNumberOfRows = floor(self.ending_epochtimems / (1000 * 3600*24)) - floor(
                self.starting_epochtimems / (1000 * 3600*24))+1
        elif period == 'W':
            # a revoir l'histoire de l'incrementation
            expectedNumberOfRows = floor(self.ending_epochtimems / (1000 * 3600*24*7)) - floor(
                self.starting_epochtimems / (1000 * 3600*24*7))+2
        elif period == 'M':
            expectedNumberOfRows = floor(self.ending_epochtimems / (1000 * 3600*24*7*30)) - floor(
                self.starting_epochtimems / (1000 * 3600*24*7*30))+1
        elif period == '6M':
            expectedNumberOfRows = floor(self.ending_epochtimems / (1000 * 3600 * 24 * 7 * 30 * 6)) - floor(
                self.starting_epochtimems / (1000 * 3600 * 24 * 7 * 30 * 6))+1

        fullDateTime, data_values = self.get_raw_measurements_with_fullDate(variable_id)
        fullDateTime, data_values = filter_outliers(label, variable_id, fullDateTime, data_values)
        if len(fullDateTime) == 0:
            fullDateTime.append(timemg.epochtimems_to_stringdate(self.starting_epochtimems))
            fullDateTime.append(timemg.epochtimems_to_stringdate(self.ending_epochtimems))
            data_values=[0,0]

        rightDateFormat=pds.to_datetime(fullDateTime, dayfirst=True)

        print('data_values min and max', min(data_values), max(data_values))
        df=pds.DataFrame(data_values, index=rightDateFormat)
        df.index.names =['Date']
        df['index'] = df.index
        df1 = df.drop_duplicates('index')
        df1.set_index = df1['index']
        df1 = df1.drop('index', axis=1)
        newDF = df1.resample(period).pad()

        # voir :
        # H : pas horaire
        # D : calendaire
        # W : hebdomadaire
        # M : mensuel
        #  http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases
        #print("-------->resampler", type(resampler), "<---------\n")
        # pad() prend les points aux horaires choisies. Si pas existants garde l'instance d'avant (bloqueur d'ordre Zero)
        # asfreq()prend les points aux horaires choisies. Si pas existants interpolation linéaire (bloqueur d'ordre 1)
        # sum() prend la somme des valeurs dans l'heure et fait une interpolation linéaire pour les valeurs manquantes
        # bfill() prend la valeur de l'ittération (k+1) avec un bloqueur d'ordre zero en cas d'absence de données
        #print("-------->newDF", newDF, "<---------\n")
        #print('Sauvegarde dans le fichier output.csv''...')



        indx=list()
        val=list()
        #newDF.to_csv('check'+str(variable_id))
        for element in newDF.index:
            if not isnan(float(newDF.at[element,0])):
                indx.append(timemg.pdsTimestamp_to_epochtimems(str(element)))
                val.append(float(newDF.at[element, 0]))

        if len(indx) < expectedNumberOfRows:
            while len(indx) < expectedNumberOfRows:
                val.insert(0,val[0])
                indx.insert(0, (2*indx[0]-indx[1]))

        return indx, val

    def get_counter_measurements(self, variable_id: int, timequantum_duration_in_secondes: int=1800):
        """
        return the number of non-null measurements per time quantum (sample period)
        :param variable_id: identifier (int) of the variable to extract
        :param timequantum_duration_in_secondes: duration of the time quantum (sample period) in secondes (default is 1800s i.e. 30 min)
        :return: number of non-null measurements per time quantum (sample period)
        """
        data_epochtimesms, data_values = self.get_raw_measurements(variable_id, remove_outliers=False)
        return count_data(data_epochtimesms, data_values, timequantum_duration_in_secondes, self.starting_epochtimems, self.ending_epochtimems)

    def get_variables(self):
        """
        return the list of available variables
        :return: list of child of DBvariable
        """
        return self.variables

    def get_earliest_epochtimems(self):
        """
        return the smallest time in the database
        :return: epoch time in ms
        """
        return self.earliest_epochtimems

    def get_latest_epochtimems(self):
        """
        return the biggest time in the database
        :return: epoch time in ms
        """
        return self.latest_epochtimems

    def get_starting_epochtimems(self):
        """
        return the starting time for the extraction period
        :return: epoch time in ms
        """
        return self.starting_epochtimems

    def get_ending_epochtimems(self):
        """
        return the ending time for the extraction period
        :return: epoch time in ms
        """
        return self.ending_epochtimems

    def __str__(self):
        string = "database '%s' with data from %s to %s\n" % (self.name, timemg.epochtimems_to_stringdate(self.starting_epochtimems), timemg.epochtimems_to_stringdate(self.ending_epochtimems))
        for variable in self.variables:
            string += variable.__str__() + "\n"
        return string

class DataContainer:
    """
    Cette classe permet la récupération des variables stockées dans la base de données, la création de nouvelles variables de différents types et on tockage dans la base
    Cette classe contient aussi le plotter, méthode dessinant les courbes
    """

    def __init__(self, sample_time=60 * 60, starting_stringdatetime=None, ending_stringdatetime=None):
        self.sample_time = sample_time
        self.starting_stringdatetime = starting_stringdatetime
        self.ending_stringdatetime = ending_stringdatetime
        self.registered_databases = dict()
        self.data = dict()
        self.extracted_variables = list()
        self.data['stringtime'] = None
        self.extracted_variables.append('stringtime')
        self.data['epochtime'] = None
        self.extracted_variables.append('epochtime')
        self.data['datetime'] = None
        self.extracted_variables.append('datetime')
        self.clicsOnPlotter = 0
        self.folder = time.strftime("%m_%d-%H_%M_sampled_"+str(int(sample_time/3600))+"h")
        if not os.path.exists(self.folder):
            os.makedirs(self.folder)

    def get_extracted_variables(self):
        return self.extracted_variables

    def link_to_database(self, database: DataBaseHandler):
        self.database_name=database.get_name
        self.registered_databases[self.database_name] = database
        database.set_starting_stringdatetime(self.starting_stringdatetime)
        database.set_ending_stringdatetime(self.ending_stringdatetime)

    def add_sampled_variable(self, label: str, variable_id: int, remove_outliers: bool, outlier_sensitivity: float = 5,
                             min_value: float = None, max_value: float = None, record: bool = True):
        if self.sample_time >= 50 and self.sample_time <= 70:
            per = 'T'
        elif self.sample_time >= 500 and self.sample_time <= 700:
            per = '10T'
        elif self.sample_time >= 3000 and self.sample_time <= 4000:
            per = 'H'
        elif self.sample_time >= 80000 and self.sample_time <= 87000:
            per = 'D'
        elif self.sample_time >= 600000 and self.sample_time <= 700000:
            per = 'W'
        elif self.sample_time >= 2300000 and self.sample_time <= 2700000:
            per = 'M'
        elif self.sample_time >= 14000000 and self.sample_time <= 16000000:
            per = '6M'

        epochtimes_in_ms, values = self.registered_databases[
            self.database_name].get_sampled_and_filtred_measurements_with_pandas(variable_id,
                                                                                 label, per)

        if self.data['epochtime'] is None:
            self.data['epochtime'] = epochtimes_in_ms
            self.data['stringtime'] = [timemg.epochtimems_to_stringdate(epochTimeInMs) for epochTimeInMs in epochtimes_in_ms]
            self.data['datetime'] = [timemg.epochtimems_to_datetime(epochTimeInMs) for epochTimeInMs in epochtimes_in_ms]
        if label not in self.extracted_variables:
            self.data[label] = values
            if record:
                self.extracted_variables.append(label)
        else:
            print('variable %s already extracted' % label)

    def add_local_variable(self, label:str,values):
        if label not in self.extracted_variables:
            self.data[label] = values
            self.extracted_variables.append(label)

    def add_counter_variable(self, database_name, label: str, variable_id: int, record: bool=True):
        epochtimes_in_ms, values = self.registered_databases[database_name].get_counter_measurements(variable_id, timequantum_duration_in_secondes=self.sample_time)
        if self.data['epochtime'] is None:
            self.data['epochtime'] = epochtimes_in_ms
            self.data['stringtime'] = [timemg.epochtimems_to_stringdate(epochTimeInMs) for epochTimeInMs in epochtimes_in_ms]
            self.data['datetime'] = [timemg.epochtimems_to_datetime(epochTimeInMs) for epochTimeInMs in epochtimes_in_ms]
        if label not in self.extracted_variables:
            self.data[label] = values
            if record:
                self.extracted_variables.append(label)
        else:
            print('variable %s already extracted' % label)

    def add_external_variable(self, label: str, values: list):
        if label not in self.extracted_variables:
            self.data[label] = values
            self.extracted_variables.append(label)
        else:
            print('variable %s already extracted' % label)

    def add_csv_variable(self, csv_filename: str, label: str):
        if label not in self.extracted_variables:
            epochtimeinms = [timemg.stringdate_to_epochtimems(self.starting_stringdatetime)]
            values = [-1]
            with open(csv_filename, 'r', newline='') as csvfile:
                csv_reader = csv.reader(csvfile, dialect='excel')
                header = True
                for row in csv_reader:
                    if header:
                        header = False
                    else:
                        epochtimeinms.append(int(row[0]))
                        values.append(float(row[1]))
            epochtimeinms.append(epochtimeinms[-1] + 1)
            values.append(-1)
            sampled_data_epochtimes, sampled_data_values = resample_data(epochtimeinms, values, self.sample_time, timemg.stringdate_to_epochtimems(self.starting_stringdatetime), timemg.stringdate_to_epochtimems(self.ending_stringdatetime))
            self.data[label] = sampled_data_values
            self.extracted_variables.append(label)
        else:
            print('variable %s already existing' % label)

    def get_variable(self, label: str):
        return self.data[label]

    def get_epochtime(self):
        return self.data['epochtime']

    def get_datetime(self):
        return self.data['datetime']

    def get_number_of_variables(self):
        return len(self.extracted_variables)

    def get_number_of_samples(self):
        if self.data['epochtime'] is None:
            return 0
        else:
            return len(self.data['epochtime'])

    def get_starting_epochtimems(self):
        return timemg.stringdate_to_epochtimems(self.starting_stringdatetime)

    def get_ending_epochtimems(self):
        return timemg.stringdate_to_epochtimems(self.ending_stringdatetime)

    def save_as_csv(self, file_name: str):
        if file_name[-4:] != '.csv':
            file_name += '.csv'
        with open(file_name, 'w', newline='') as csvfile:
            csv_file = csv.writer(csvfile, dialect='excel')
            csv_file.writerow(self.extracted_variables)
            for i in range(len(self.data['epochtime'])):
                row = list()
                for var in self.extracted_variables:
                    row.append(self.data[var][i])
                csv_file.writerow(row)

    def _plot_selection(self, int_vars: list):
        #styles = ('-', '--', '-.', ':')
        styles = ('-')
        linewidths = (3.0, 2.5, 2.5, 1.5, 1.0, 0.5, 0.25)
        figure, axes = plt.subplots()
        text_legends = list()
        for i in range(len(int_vars)):
            if int_vars[i].get():
                style = styles[i % len(styles)]
                linewidth = linewidths[i // len(styles) % len(linewidths)]
                time_data = list(self.data['datetime'])
                value_data = list(self.data[self.extracted_variables[i + 3]])
                if len(time_data) > 1:
                    time_data.append(time_data[-1] + (time_data[-1] - time_data[-2]))
                    value_data.append(value_data[-1])
                print("&&&&&&&ploting dimension", len(time_data), len(value_data))
                axes.step(time_data, value_data, linewidth=linewidth, linestyle=style, where='post')

                #axes.set_xlim([time_data[0], time_data[-1]])
                text_legends.append(self.extracted_variables[i + 3])
                int_vars[i].set(0)
        axes.legend(text_legends, loc=0)
        figure.set_tight_layout(True)
        horizon=(timemg.stringdate_to_epochtimems(self.ending_stringdatetime) - timemg.stringdate_to_epochtimems(
            self.starting_stringdatetime))

        if horizon <= 24 * 3600 * 1000:
            axes.set_title('sample time %i seconds \n from %s to %s' % (
            int(self.sample_time), self.starting_stringdatetime, self.ending_stringdatetime))
        elif horizon <= 10 * 24 * 3600 * 1000 and horizon >= 2 * 24 * 3600 * 1000:
            axes.set_title('sample time %i seconds \n from %s to %s' % (
                int(self.sample_time), self.starting_stringdatetime, self.ending_stringdatetime))
        elif horizon <= 60 * 24 * 3600 * 1000 and horizon >= 10 * 24 * 3600 * 1000:
            axes.set_title('sample time %i seconds \n from %s to %s' % (
                int(self.sample_time), self.starting_stringdatetime, self.ending_stringdatetime))
        elif horizon >= 60 * 24 * 3600 * 1000:
            axes.set_title('sample time %i seconds \n from %s to %s' % (
                int(self.sample_time), self.starting_stringdatetime, self.ending_stringdatetime))

        locator=mdates.AutoDateLocator()
        formatter=mdates.AutoDateFormatter(locator)
        formatter.scaled[1/(24.*60.)]= '%d %b %H:%M'
        formatter.scaled[1./24.] = '%d %b %H:%M'

        axes.xaxis.set_major_locator(locator)
        axes.xaxis.set_major_formatter(formatter)


        axes.grid(True)
        #plt.savefig(self.folder+"/" +'PlotNumber'+str(self.clicsOnPlotter))
        self.clicsOnPlotter = self.clicsOnPlotter +1
        plt.show()

    def plot(self):
        tk_variables = list()
        tk_window = tk.Tk()
        tk_window.wm_title('variable plotter')
        tk.Button(tk_window, text='plot', command=lambda: self._plot_selection(tk_variables)).grid(row=0, column=0, sticky=tk.W + tk.E)
        frame = tk.Frame(tk_window).grid(row=1, column=0, sticky=tk.N + tk.S)
        vertical_scrollbar = tk.Scrollbar(frame, orient=tk.VERTICAL)
        vertical_scrollbar.grid(row=1, column=1, sticky=tk.N + tk.S)
        canvas = tk.Canvas(frame, width=400, yscrollcommand=vertical_scrollbar.set)
        tk_window.grid_rowconfigure(1, weight=1)
        canvas.grid(row=1, column=0, sticky='news')
        vertical_scrollbar.config(command=canvas.yview)
        checkboxes_frame = tk.Frame(canvas)
        checkboxes_frame.rowconfigure(1, weight=1)
        for i in range(3, len(self.extracted_variables)):
            tk_variable = tk.IntVar(0)
            tk_variables.append(tk_variable)
            tk.Checkbutton(checkboxes_frame, text=self.extracted_variables[i], variable=tk_variable, offvalue=0).grid(row=(i - 3), sticky=tk.W)
        canvas.create_window(0, 0, window=checkboxes_frame)
        checkboxes_frame.update_idletasks()
        canvas.config(scrollregion=canvas.bbox('all'))
        tk_window.geometry(str(tk_window.winfo_width()) + "x" + str(tk_window.winfo_screenheight()))
        tk_window.mainloop()

    def __str__(self):
        string = 'Data cover period from %s to %s with time period: %d seconds\nRegistered database:\n' % (self.starting_stringdatetime, self.ending_stringdatetime, self.sample_time)
        for database in self.registered_databases:
            string += '- %s \n' % database
        string += 'Available variables:\n'
        for variable_name in self.extracted_variables:
            string += '- %s \n' % variable_name
        return string

if __name__=="__main__":
    dbInstanceHandler = DataBaseHandler("PosA_folder/PosA-DB.sqlite3","14/10/2016 00:00:00","14/12/2016 00:00:00")
    dbInstanceHandler.get_sampled_and_filtred_measurements_with_pandas(25, label='only Id is available')


