from front_end import *
print('loading...')

indicator = NormalizedIndicatorComputationAndPloting("Predis", 3600*24)

indicator.dataContainersCreation('01/02/2017 10:00:00', '01/02/2017 18:00:00')
indicator.integrateAllVariables()
indicator.computeEnergyDifferential(3600)
indicator.computeNormalizedAutoconsumption('minimal', 3600)
print('computing...')

print('ploting...')
indicator.plot()
print('Done')
