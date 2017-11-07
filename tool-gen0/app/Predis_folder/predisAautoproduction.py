"""
Nous utilisons dans ce module la classe AccurateIndicatorComputationAndPloting pour faire du pas de calcul variable
Ce module construit l'ensemble des indicateurs souhaités sur l'IHM.
"""
from front_end import *
print('loading...')

indicator = NormalizedIndicatorComputationAndPloting("Predis", 3600*24)


indicator.dataContainersCreation('01/07/2016 10:00:00', '01/04/2017 12:00:00')

indicator.integrateAllVariables()
print('computing...')


indicator.computeNormalizedAutoproduction('daily', 3600*24)
indicator.computeNormalizedAutoproduction('weekly', 3600*24*7)
indicator.computeNormalizedAutoproduction('monthly',3600*24*30)
indicator.computeNormalizedAutoproduction('yearly', 3600 * 24 * 30 * 12)

print('ploting...')
indicator.plot()
print('Done')
