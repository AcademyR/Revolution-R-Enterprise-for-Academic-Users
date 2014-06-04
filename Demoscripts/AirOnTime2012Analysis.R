###############################################################################
#
# AirOnTime2012Analysis.R
#
# A script containing a sampling of analysis functions using the
# 2012 airline on-time arrival data
#
# The same script should be able to run full 1987-2012 airline data
# This script does not generate any graphics
#
###############################################################################

###############################################################################
#  Set up compute context 
###############################################################################

rxSetComputeContext("local")

###############################################################################
#  Set up data source 
###############################################################################

airXdfFile <- "C:/Revolution/data/AirOnTime2012.xdf"   
airData <- RxXdfData(airXdfFile)
rxGetInfo(airData, TRUE)

################################################################################
#  Basic analysis functions
################################################################################

################################################################################
# rxSummary
################################################################################
# Compute summary statistics for Arrival Delay by DayOfWeek
summaryObj <- rxSummary(ArrDelay~DayOfWeek, data = airData)
summaryObj

################################################################################
# rxQuantile
################################################################################
# Compute the quartiles of the Arrival Delay column in the data set.
# Quartiles are the default setting for the rxQuantile function.
quarts <- rxQuantile("ArrDelay", airData)
quarts

################################################################################
# rxCube
################################################################################
# Compute counts for each minute of ArrDelayMinutes

delayCounts <- rxCube(~F(ArrDelayMinutes), data = airData, removeZeroCounts = TRUE)
# Extract data frame from results and look at first rows
delayCountsDF <- rxResultsDF(delayCounts)
head(delayCountsDF, n = 10L)

################################################################################
# rxCrossTabs
################################################################################
# All independent variables must be factors for rxCube and rxCrossTabs: "DepDel15".
# Use 'F(x)' to declare that a continuous variable x is to be treated as a factor.

crossTabs <- rxCrossTabs(formula = ArrDel15 ~ F(DepDel15):DayOfWeek, 
                         data = airData, means = TRUE)
crossTabs     # Presumably the results in each column do not sum to 1 due to missing data (?)

################################################################################
# rxCovCor
################################################################################
# Remark -- From the RSRUG:
# While rxCovCor is the primary tool for computing covariance, correlation, and other 
# cross-product matrices, you will seldom call it directly. Instead, it is usually simpler to use 
# one of the following convenience functions:  rxCov, rxCor, rxSSCP.

covForm <- ~ DepDelayMinutes + ArrDelayMinutes + AirTime
cov <- rxCovCor(formula = covForm, data = airData, type = "Cov")
cor <- rxCovCor(formula = covForm, data = airData, type = "Cor")
cov   # covariance matrix
cor   # correlation matrix


################################################################################
# rxLinMod
################################################################################
linModObj <- rxLinMod(ArrDelay~ DayOfWeek + F(CRSDepTime) + Distance, 
    data = airData)
summary(linModObj)

################################################################################
# rxLogit
################################################################################
logitObj <- rxLogit(ArrDel15~DayOfWeek + F(CRSDepTime) + Distance, data = airData)
summary(logitObj)

################################################################################
# rxLorenz
################################################################################

# Compute Lorenz data using probability weights
lorenzOut <- rxLorenz(orderVarName = "ArrDelayMinutes", data = airData)
lorenzOut

################################################################################
# rxGlm
################################################################################
glmObj <- rxGlm(ArrDelayMinutes~DayOfWeek + F(CRSDepTime) + Distance, 
    data = airData, family = rxTweedie(var.power = 1.15))
summary(glmObj)


################################################################################
# rxKmeans
################################################################################
kmeansObj <- rxKmeans( ~ArrDelay + Distance, data = airData,
		 numClusters = 10)
kmeansObj

################################################################################
# rxDTree
################################################################################
form <- "ArrDel15 ~ Month + Origin + Dest + DepTime + DepDelayGroups + DistanceGroup"

dtree <- rxDTree(form, data = airData, maxDepth = 5, 
                 cp = 1.0, maxSurrogate = 0, overwrite = TRUE)
dtree

################################################################################
# rxDForest
################################################################################
# Use the same formula as in the tree case:
rxGetInfo(airData, TRUE)
dforest <- rxDForest(form, data = airData, nTree = 10, removeMissings = TRUE,
                     maxDepth = 5, cp = 0.5, maxSurrogate = 0, overwrite = TRUE)
dforest


