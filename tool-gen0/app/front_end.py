__author__ = 'Yanis Hadj Said'

import matplotlib
matplotlib.use('TkAgg')

from back_end import *


class ProjectDataBase():

    def __init__(self, projectName, projectLocation):
        self.dbName = projectName+"-DB"
        self.db = DataBaseCreator()
        self.db.createExtractionDatabase(projectName+"-DB.sqlite3", projectLocation, projectName)
        self.dataContainer=dict()
        self.id_in_db = list()
        self.name_in_db = list()

    def dbFullfillerFromVetaWebServer(self, *args):
        "args : [sensor_id, variableName_in_db, timeBegin, timeEnd] with globalConsuption and globalProduction as mandatory variables for indicators computation"

        for arg in args:
            sensor_id =arg[0]
            variableName_in_db =arg[1]
            timeBegin =arg[2]
            timeEnd =arg[3]
            idindb,nameindb=self.db.fillVariableInDatabaseFromPosAWebService(self.dbName + ".sqlite3", "global", variableName_in_db,
                                                             sensor_id, timeBegin, timeEnd)
            self.id_in_db.append(idindb)
            self.name_in_db.append(nameindb)

    def dbFullfillerFromCSV(self, *args):
        "args : [variableName_in_db, CSVName] "

        for arg in args:
            variableName_in_db = arg[0]
            CSVName = arg[1]
            idindb, nameindb = self.db.fillVariableInDatabaseFromCSVFile(self.dbName + ".sqlite3", "global",variableName_in_db,CSVName)
            self.id_in_db.append(idindb)
            self.name_in_db.append(nameindb)

    def dbFullfillerFromCSVForHanau(self, *args):
        "args : [variableName_in_db, CSVName] "

        for arg in args:
            variableName_in_db = arg[0]
            CSVName = arg[1]
            idindb, nameindb = self.db.fillVariableInDatabaseFromCSVFileForHanau(self.dbName + ".sqlite3", "global",
                                                                         variableName_in_db, CSVName)
            self.id_in_db.append(idindb)
            self.name_in_db.append(nameindb)


class NormalizedIndicatorComputationAndPloting():
    """
    calcul des indicateurs dans un dataContainer instancié à cet effet. L'instantication du dataContainer impose le pas d'échantillonnage des données.
    Cette méthode ne permet que le calcul des indicateurs à des pas de temps différents.

    """
    def __init__(self,projectName, globalCalculationStep):
        self.globalCalculationStep=globalCalculationStep
        self.dataFrame= pds.DataFrame()
        self.dataContainer=None
        self.dbName=projectName+"-DB"
        self.dbHandler = DataBaseHandler(self.dbName + ".sqlite3")
        self.period=600

    def dataContainersCreation(self,starting_datetime_for_period, ending_datetime_for_period):
        """
        créé l'entrepot python des variables, avec valeurs et attribus pour les mettre à disposition des calculs d'indicateurs. Cet entrepot est lié via la méthode link_to_dataBase
        à la base de données d'ou les variables doivent être chargées et elle le sont par la suite via la méthode dataContainerLoading
        :param starting_datetime_for_period:
        :param ending_datetime_for_period:
        :return:
        """

        self.starting_datetime_for_period=starting_datetime_for_period
        self.ending_datetime_for_period=ending_datetime_for_period
        self.dataContainer= DataContainer(sample_time=self.period, starting_stringdatetime=self.starting_datetime_for_period,
                                               ending_stringdatetime= self.ending_datetime_for_period)
        ##A faire ...
        #self.pandasDataContainer=PandasDataContainer(sample_time=self.period, starting_stringdatetime=self.starting_datetime_for_period,
        #                                       ending_stringdatetime= self.ending_datetime_for_period)

        self.dataContainer.link_to_database(self.dbHandler)
        self._dataContainersLoading()

    def _dataContainersLoading(self):
        # db introspection
        vestaFormatVariables=self.dbHandler.get_variables()
        for variable in vestaFormatVariables:
            self.dataContainer.add_sampled_variable(variable.get_name(), variable.get_id(), remove_outliers=True)

    def integratecumulativeData(self, var_name_in_db):
        """
        méthode qui permet le calcul des integrales délimités par le pas d'échantillonnage pour une variable définie en paramètre

        """

        totalCumule = self.dataContainer.get_variable(var_name_in_db)
        instantane = list()
        count = 0
        instantane.append(0)
        for conso in totalCumule:

            if count + 1 < len(totalCumule):
                instantane.append(totalCumule[count + 1] - totalCumule[count])
                count = count + 1
                self.dataContainer.add_local_variable(var_name_in_db+'PerPeriod', instantane)

    def computeEnergyDifferential(self, name : str, computationStep=None):
        if computationStep != None:
            methodComputationStep=computationStep
        else:
            methodComputationStep=self.globalCalculationStep

        timeVector = self.dataContainer.get_epochtime()
        vestaFormatVariables = self.dbHandler.get_variables()
        for var_name_in_db in vestaFormatVariables:
            variableValuesVector = self.dataContainer.get_variable(var_name_in_db.get_name())
            integraleVectorPerResampleStep = list()
            derivativeVectorPerResampleStep = list()

            increment = 0
            derivativeValue = 0
            counter=0
            for i in range(len(timeVector)-1):
                valueconso= variableValuesVector[i]
                stringTime=timemg.epochtimems_to_stringdate(timeVector[i])
                if (timeVector[i] / 1000) % methodComputationStep != 0:
                        increment = increment + 1
                        derivativeValue = derivativeValue + variableValuesVector[i+1]-variableValuesVector[i]
                elif (timeVector[i] / 1000) % methodComputationStep == 0:
                    for g in range(counter):
                        integraleVectorPerResampleStep.append(derivativeValue)
                        # formule à vérifier
                        derivativeVectorPerResampleStep.append(derivativeValue*increment*(self.period/methodComputationStep))
                    counter = 0
                    increment = 1
                    derivativeValue = variableValuesVector[i+1]-variableValuesVector[i]
                counter=counter+1
            if counter != 0:
                if increment != 0:
                    for g in range(counter):
                        integraleVectorPerResampleStep.append(derivativeValue)
                        # formule à vérifier
                        derivativeVectorPerResampleStep.append(derivativeValue * increment * (self.period / methodComputationStep))
                if increment == 0:
                    for g in range(counter):
                        integraleVectorPerResampleStep.append(derivativeValue)
                        # formule à vérifier
                        derivativeVectorPerResampleStep.append(derivativeValue * increment * (self.period / methodComputationStep))
            integraleVectorPerResampleStep.append(0)
            derivativeVectorPerResampleStep.append(0)
            self.dataContainer.add_local_variable(var_name_in_db.get_name() + 'differencial'+name, integraleVectorPerResampleStep)
            self.dataContainer.add_local_variable(var_name_in_db.get_name() + 'derivativeValues'+name, derivativeVectorPerResampleStep)

    def integrateAllVariables(self):
        """
        méthode qui permet le calcul des integrales délimités par le pas d'échantillonnage pour toutes les variables

        """
        vestaFormatVariables = self.dbHandler.get_variables()
        for var_name_in_db in vestaFormatVariables:
            totalCumule = self.dataContainer.get_variable(var_name_in_db.get_name())
            instantane = list()
            count = 0
            instantane.append(0)
            for conso in totalCumule:
                if count + 1 < len(totalCumule):
                    instantane.append(totalCumule[count + 1] - totalCumule[count])
                    count = count + 1
            self.dataContainer.add_local_variable(var_name_in_db.get_name() + 'PerPeriod', instantane)

    def computeNormalizedAutoconsumption(self, name, computationStep=None):
        if computationStep != None:
            methodComputationStep = computationStep
        else:
            methodComputationStep =self.globalCalculationStep
        timeVector=self.dataContainer.get_epochtime()
        variables = self.dataContainer.get_extracted_variables()
        for variable in variables:
            if 'globalConsuptionPerPeriod' in variable:
                print('globalConsuptionPerPeriod found')
                globalConsuptionPerPeriod=self.dataContainer.get_variable(variable)
            if "globalProductionPerPeriod" in variable:
                print('globalProductionPerPeriod found')
                globalProductionPerPeriod=self.dataContainer.get_variable(variable)

        autoconsumption = list()
        autoconsumptionCurentValue = 0
        increment = 0
        counter=0
        for i in range(len(globalProductionPerPeriod)):

            #valueconso=globalConsuptionPerPeriod[i]
            #valueprod=globalProductionPerPeriod[i]
            #stringTime=timemg.epochtimems_to_stringdate(timeVector[i])

            if (timeVector[i]/1000) % methodComputationStep !=0:
                if globalProductionPerPeriod[i] !=0 and globalConsuptionPerPeriod[i] <= globalProductionPerPeriod[i]:
                    increment = increment + 1
                    autoconsumptionCurentValue = autoconsumptionCurentValue+(globalConsuptionPerPeriod[i] / globalProductionPerPeriod[i])
                elif globalProductionPerPeriod[i] !=0 and globalConsuptionPerPeriod[i] >= globalProductionPerPeriod[i]:
                    increment = increment + 1
                    autoconsumptionCurentValue = autoconsumptionCurentValue+(1)
                else:
                    pass
                counter = counter + 1
            elif (timeVector[i] / 1000) % methodComputationStep == 0:
                if increment != 0:
                    for g in range(counter):
                        autoconsumption.append(autoconsumptionCurentValue / increment)
                if increment == 0:
                    for g in range(counter):
                        autoconsumption.append(0)
                autoconsumptionCurentValue = 0
                increment = 0
                counter = 0
                if globalProductionPerPeriod[i] != 0 and globalConsuptionPerPeriod[i] <= globalProductionPerPeriod[i]:
                    increment = increment + 1
                    autoconsumptionCurentValue = autoconsumptionCurentValue + (
                        globalConsuptionPerPeriod[i] / globalProductionPerPeriod[i])
                elif globalProductionPerPeriod[i] != 0 and globalConsuptionPerPeriod[i] >= globalProductionPerPeriod[i]:
                    increment = increment + 1
                    autoconsumptionCurentValue = autoconsumptionCurentValue + (1)
                counter = counter + 1
            else:
                print("cas non considéré",counter, increment, timeVector[i], timeVector[i]/1000 % methodComputationStep)
        if counter !=0:
            if increment !=0:
                for g in range(counter):
                    autoconsumption.append(autoconsumptionCurentValue / increment)
            if increment == 0:
                for g in range(counter):
                    autoconsumption.append(0)
        self.dataContainer.add_local_variable('NormalizedAutoconsumptionPerPeriod'+name, autoconsumption)

    def computeNormalizedAutoproduction(self, name, computationStep=None):
        if computationStep != None:
            methodComputationStep=computationStep
        else:
            methodComputationStep = self.globalCalculationStep
        timeVector=self.dataContainer.get_epochtime()
        variables = self.dataContainer.get_extracted_variables()
        for variable in variables:
            if 'globalConsuptionPerPeriod' in variable:
                print('globalConsuptionPerPeriod found')
                globalConsuptionPerPeriod=self.dataContainer.get_variable(variable)
            if "globalProductionPerPeriod" in variable:
                print('globalProductionPerPeriod found')
                globalProductionPerPeriod=self.dataContainer.get_variable(variable)

        autoproduction = list()
        autoproductionCurentValue = 0
        increment = 0
        counter=0
        for i in range(len(globalProductionPerPeriod)):
            #valueconso=globalConsuptionPerPeriod[i]
            #valueprod=globalProductionPerPeriod[i]
            #stringTime=timemg.epochtimems_to_stringdate(timeVector[i])
            if (timeVector[i]/1000) % methodComputationStep !=0:
                if globalConsuptionPerPeriod[i] !=0 and globalProductionPerPeriod[i] <= globalConsuptionPerPeriod[i]:
                    increment = increment + 1
                    autoproductionCurentValue = autoproductionCurentValue+(globalProductionPerPeriod[i]/globalConsuptionPerPeriod[i])
                elif globalConsuptionPerPeriod[i] !=0 and globalConsuptionPerPeriod[i] <= globalProductionPerPeriod[i]:
                    increment = increment + 1
                    autoproductionCurentValue = autoproductionCurentValue+(1)
                else:
                    pass
                counter = counter + 1
            elif (timeVector[i] / 1000) % methodComputationStep == 0:
                if increment != 0:
                    for g in range(counter):
                        autoproduction.append(autoproductionCurentValue / increment)
                if increment == 0:
                    for g in range(counter):
                        autoproduction.append(0)
                autoproductionCurentValue = 0
                increment = 0
                counter = 0
                if globalConsuptionPerPeriod[i] != 0 and globalProductionPerPeriod[i] <= globalConsuptionPerPeriod[i]:
                    increment = increment + 1
                    autoproductionCurentValue = autoproductionCurentValue + (
                    globalProductionPerPeriod[i] / globalConsuptionPerPeriod[i])
                elif globalProductionPerPeriod[i] != 0 and globalConsuptionPerPeriod[i] <= globalProductionPerPeriod[i]:
                    increment = increment + 1
                    autoproductionCurentValue = autoproductionCurentValue + (1)
                else:
                    pass
                counter = counter + 1
            else:
                print("cas non considéré",counter, increment, timeVector[i], timeVector[i]/1000 % methodComputationStep)
        if counter !=0:
            if increment !=0:
                for g in range(counter):
                    autoproduction.append(autoproductionCurentValue / increment)
            if increment == 0:
                for g in range(counter):
                    autoproduction.append(0)
        self.dataContainer.add_local_variable('NormalizedAutoproductionPerPeriod'+name, autoproduction)

    def computeNormalizedCouverture(self,name, computationStep=None):
        if computationStep != None:
            methodComputationStep=computationStep
        else:
            methodComputationStep = self.globalCalculationStep

        timeVector=self.dataContainer.get_epochtime()
        variables = self.dataContainer.get_extracted_variables()
        for variable in variables:
            if 'globalConsuptionPerPeriod' in variable:
                print('globalConsuptionPerPeriod found')
                globalConsuptionPerPeriod=self.dataContainer.get_variable(variable)
            if "globalProductionPerPeriod" in variable:
                print('globalProductionPerPeriod found')
                globalProductionPerPeriod=self.dataContainer.get_variable(variable)

        autoproduction = list()
        autoproductionCurentValue = 0
        increment = 0
        counter=0
        for i in range(len(globalProductionPerPeriod)):
            #valueconso=globalConsuptionPerPeriod[i]
            #valueprod=globalProductionPerPeriod[i]
            #stringTime=timemg.epochtimems_to_stringdate(timeVector[i])
            if (timeVector[i]/1000) % methodComputationStep !=0:
                if globalConsuptionPerPeriod[i] !=0 and globalProductionPerPeriod[i] <= globalConsuptionPerPeriod[i]:
                    increment = increment + 1
                    autoproductionCurentValue = autoproductionCurentValue+(globalProductionPerPeriod[i]/globalConsuptionPerPeriod[i])
                else:
                    pass
                counter = counter + 1
            elif (timeVector[i] / 1000) % methodComputationStep == 0:
                if increment != 0:
                    for g in range(counter):
                        autoproduction.append(autoproductionCurentValue / increment)
                if increment == 0:
                    for g in range(counter):
                        autoproduction.append(0)
                autoproductionCurentValue = 0
                increment = 0
                counter = 0
                if globalConsuptionPerPeriod[i] != 0 and globalProductionPerPeriod[i] <= globalConsuptionPerPeriod[i]:
                    increment = increment + 1
                    autoproductionCurentValue = autoproductionCurentValue + (
                    globalProductionPerPeriod[i] / globalConsuptionPerPeriod[i])
                else:
                    pass
                counter = counter + 1
            else:
                print("cas non considéré",counter, increment, timeVector[i], timeVector[i]/1000 % methodComputationStep)
        if counter !=0:
            if increment !=0:
                for g in range(counter):
                    autoproduction.append(autoproductionCurentValue / increment)
            if increment == 0:
                for g in range(counter):
                    autoproduction.append(0)
        self.dataContainer.add_local_variable('NormalizedCouverturePerPeriod'+name, autoproduction)

    def computeCouverture(self, name, computationStep=None):
        if computationStep != None:
            methodComputationStep = computationStep
        else:
            methodComputationStep = self.globalCalculationStep

        timeVector = self.dataContainer.get_epochtime()
        variables = self.dataContainer.get_extracted_variables()
        for variable in variables:
            if 'globalConsuptionPerPeriod' in variable:
                print('globalConsuptionPerPeriod found')
                globalConsuptionPerPeriod = self.dataContainer.get_variable(variable)
            if "globalProductionPerPeriod" in variable:
                print('globalProductionPerPeriod found')
                globalProductionPerPeriod = self.dataContainer.get_variable(variable)

        couverture = list()
        productionCurentValue = 0
        consumptionCurentValue = 0
        couverturePerPeriod = 0
        counter = 0
        for i in range(len(globalProductionPerPeriod)):
            # valueconso=globalConsuptionPerPeriod[i]
            # valueprod=globalProductionPerPeriod[i]
            # stringTime=timemg.epochtimems_to_stringdate(timeVector[i])
            if (timeVector[i] / 1000) % methodComputationStep != 0:
                    productionCurentValue = productionCurentValue + globalProductionPerPeriod[i]
                    consumptionCurentValue= consumptionCurentValue + globalConsuptionPerPeriod[i]
                    counter = counter + 1

            elif (timeVector[i] / 1000) % methodComputationStep == 0:
                productionCurentValue = productionCurentValue + globalProductionPerPeriod[i]
                consumptionCurentValue = consumptionCurentValue + globalConsuptionPerPeriod[i]
                counter = counter + 1
                if consumptionCurentValue != 0:
                    couverturePerPeriod=productionCurentValue/consumptionCurentValue
                    for g in range(counter):
                        couverture.append(couverturePerPeriod)
                elif consumptionCurentValue == 0:
                    for g in range(counter):
                        couverture.append(None)
                couverturePerPeriod = 0
                productionCurentValue = 0
                consumptionCurentValue = 0
                counter = 0
                print("longueurs intermédiaires", len(couverture), i)

        if counter != 0 :
            if consumptionCurentValue !=0 :
                couverturePerPeriod = productionCurentValue / consumptionCurentValue
                for g in range(counter):
                    couverture.append(couverturePerPeriod)
            if consumptionCurentValue == 0:
                for g in range(counter):
                    couverture.append(None)
        print("longueurs....", len(couverture), len(timeVector))
        self.dataContainer.add_local_variable('couverturePerPeriod' + name, couverture)

    def computeAutoproduction(self):
        variables = self.dataContainer.get_extracted_variables()
        for variable in variables:

            if 'globalConsuptionPerPeriod' in variable:
                globalConsuptionPerPeriod = self.dataContainer.get_variable(variable)
            if "globalProductionPerPeriod" in variable:
                globalProductionPerPeriod = self.dataContainer.get_variable(variable)
        # calcul autoconsumption instantanée
        autonomie = list()
        for i in range(len(globalProductionPerPeriod)):
            if globalConsuptionPerPeriod[i] == 0:
                autonomie.append(1)
            elif globalConsuptionPerPeriod[i] >= globalProductionPerPeriod[i]:
                autonomie.append(globalProductionPerPeriod[i] / globalConsuptionPerPeriod[i])
            elif globalConsuptionPerPeriod[i] <= globalProductionPerPeriod[i]:
                autonomie.append(1)
            else:
                autonomie.append(0)
        self.dataContainer.add_local_variable('AutoproductionPerPeriod', autonomie)

    def oldcomputeCouverture(self):
        variables = self.dataContainer.get_extracted_variables()
        for variable in variables:
            if 'globalConsuptionPerPeriod' in variable:
                globalConsuptionPerPeriod = self.dataContainer.get_variable(variable)
            if "globalProductionPerPeriod" in variable:
                globalProductionPerPeriod = self.dataContainer.get_variable(variable)
        couverture = list()
        for i in range(len(globalProductionPerPeriod)):
            if globalConsuptionPerPeriod[i] == 0:
                couverture.append(1)
            else:
                couverture.append(globalProductionPerPeriod[i] / globalConsuptionPerPeriod[i])
        self.dataContainer.add_local_variable('couverturePerPeriod', couverture)

    def computeCommonAutoconsumption(self,period):
        self.subPeriod=period*1000
        variables = self.dataContainer.get_extracted_variables()
        for variable in variables:

            if 'globalConsuptionPerPeriod' in variable:
                globalConsuptionPerPeriod = self.dataContainer.get_variable(variable)
            if "globalProductionPerPeriod" in variable:
                globalProductionPerPeriod = self.dataContainer.get_variable(variable)
        # calcul autoconsumption instantanée

        commonAutoconsumption = list()
        epochtime=self.dataContainer.get_epochtime()

        for i in range(len(globalProductionPerPeriod)):
            if  globalProductionPerPeriod[i]==0 :
                commonAutoconsumption.append(0)
            elif globalConsuptionPerPeriod[i] >= globalProductionPerPeriod[i]:
                commonAutoconsumption.append(globalProductionPerPeriod[i] / globalConsuptionPerPeriod[i])
            elif globalConsuptionPerPeriod[i] <= globalProductionPerPeriod[i] or globalConsuptionPerPeriod[i] == 0 :
                commonAutoconsumption.append(1)

        finalComonAutoconsumption=len(commonAutoconsumption)*[0]
        counter=0
        mean=0
        timeTick=epochtime[0]
        cumulatedautoconsumption = 0
        j=0


        for i in range(len(epochtime)):
            if epochtime[i] > timeTick + self.subPeriod:
                for k in range(j,i):
                    finalComonAutoconsumption[k]=mean
                timeTick = timeTick + self.subPeriod
                cumulatedautoconsumption = 0
                counter = 0
                j=i

            if epochtime[i] >=timeTick and epochtime[i]<=timeTick+self.subPeriod:
                counter = counter + 1
                if commonAutoconsumption[i] != 0:
                    cumulatedautoconsumption=cumulatedautoconsumption+commonAutoconsumption[i]
                    mean=cumulatedautoconsumption/counter
        self.dataContainer.add_local_variable('finalComonAutoconsumptionPerPeriod', finalComonAutoconsumption)

    def computeCommonAutoproduction(self,period):
        self.subPeriod=period*1000
        variables = self.dataContainer.get_extracted_variables()
        for variable in variables:

            if 'globalConsuptionPerPeriod' in variable:
                globalConsuptionPerPeriod = self.dataContainer.get_variable(variable)
            if "globalProductionPerPeriod" in variable:
                globalProductionPerPeriod = self.dataContainer.get_variable(variable)
        # calcul autoconsumption instantanée
        commonAutoproduction = list()
        epochtime=self.dataContainer.get_epochtime()

        for i in range(len(globalProductionPerPeriod)):
            if globalConsuptionPerPeriod[i] == 0:
                commonAutoproduction.append(1)
            elif globalConsuptionPerPeriod[i] >= globalProductionPerPeriod[i]:
                commonAutoproduction.append(globalProductionPerPeriod[i] / globalConsuptionPerPeriod[i])
            elif globalConsuptionPerPeriod[i] <= globalProductionPerPeriod[i]:
                commonAutoproduction.append(1)
            else:
                commonAutoproduction.append(0)

        finalComonAutoproduction=len(commonAutoproduction)*[0]
        counter=0
        mean=0
        timeTick=epochtime[0]
        cumulatedautoconsumption = 0
        j=0


        for i in range(len(epochtime)):
            if epochtime[i] > timeTick + self.subPeriod:
                for k in range(j,i):
                    finalComonAutoproduction[k]=mean
                timeTick = timeTick + self.subPeriod
                cumulatedautoconsumption = 0
                counter = 0
                j=i


            if epochtime[i] >=timeTick and epochtime[i]<=timeTick+self.subPeriod:
                counter = counter + 1
                if commonAutoproduction[i] != 0:

                    cumulatedautoconsumption=cumulatedautoconsumption+commonAutoproduction[i]
                    mean=cumulatedautoconsumption/counter

        self.dataContainer.add_local_variable('finalCommonAutoproductionPerPeriod', finalComonAutoproduction)

    def computeDiffProdConso(self,period):
        self.subPeriod=period*1000
        variables = self.dataContainer.get_extracted_variables()
        for variable in variables:

            if 'globalConsuptionPerPeriod' in variable:
                globalConsuptionPerPeriod = self.dataContainer.get_variable(variable)
            if "globalProductionPerPeriod" in variable:
                globalProductionPerPeriod = self.dataContainer.get_variable(variable)
        # calcul autoconsumption instantanée

        diffProdConso = list()
        epochtime=self.dataContainer.get_epochtime()

        for i in range(len(epochtime)):
            diffProdConso.append(globalProductionPerPeriod[i] - globalConsuptionPerPeriod[i])

        self.dataContainer.add_local_variable('diffProdConsoPerPeriod', diffProdConso)

    def computeDiffProdConsointegration(self,period):
        self.subPeriod=period*1000
        variables = self.dataContainer.get_extracted_variables()
        for variable in variables:
            if 'globalConsuptionPerPeriod' in variable:
                globalConsuptionPerPeriod = self.dataContainer.get_variable(variable)
            if "globalProductionPerPeriod" in variable:
                globalProductionPerPeriod = self.dataContainer.get_variable(variable)
        # calcul autoconsumption instantanée

        epochtime=self.dataContainer.get_epochtime()

        integ=list()
        for i in range(len(epochtime)):
            if i ==0:
                integ.append(globalProductionPerPeriod[i] - globalConsuptionPerPeriod[i])
            else:
                integ.append(integ[i-1]+globalProductionPerPeriod[i]- globalConsuptionPerPeriod[i])
        #print('----------\n', integ)
        self.dataContainer.add_local_variable('integPerPeriod', integ)

    def plot(self):
        self.dataContainer.plot()
#if __name__ =='__main__':

