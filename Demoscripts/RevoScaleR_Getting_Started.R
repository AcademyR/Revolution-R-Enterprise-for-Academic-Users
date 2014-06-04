##############################################################################
# RevoScaleR Getting Started Guide Examples
##############################################################################

#############################################################################
# IMPORTANT: If you have not downloaded the 'big' sample data, 
# set useBigData to FALSE
# If you have not downloaded the big airline data (or don't want to use it)
# set useBigAirData to FALSE

# Uncomment, set the flags, and specify location of directory where you 
# downloaded the data (if used)

# useBigData <- TRUE
# useBigAirData <- TRUE
# bigDataDir <- "C:/Revolution/Data"

##############################################################################
# General setup/cleanup: Run this code before proceeding
##############################################################################
# Specify location of pre-installed sample data
sampleDataDir <- rxGetOption("sampleDataDir")

if (useBigData)
{
    if (useBigAirData)
    {
        airDataName <- file.path(bigDataDir, "AirOnTime87to12/AirOnTime87to12")	
    }
    else
    {
        airDataName <- file.path(bigDataDir, "AirOnTime7Pct")
    }

    mortCsvDataName <- file.path(bigDataDir, "mortDefault", "mortDefault")
    mortXdfFileName <- "mortDefault.xdf"
}else
{
    mortCsvDataName <- file.path(sampleDataDir, "mortDefaultSmall")
    mortXdfFileName <- "mortDefaultSmall.xdf"
}
# Remove any files to be created in this demo if they already exists
if (file.exists(mortXdfFileName)) file.remove(mortXdfFileName)
if (file.exists("ADS.xdf")) file.remove("ADS.xdf")
if (file.exists("ADS1.xdf")) file.remove("ADS1.xdf") 
if (file.exists("ADS2.xdf")) file.remove("ADS2.xdf") 

if (file.exists("myMortData.xdf")) file.remove("myMortData.xdf")
if (file.exists("myMortData2.xdf")) file.remove("myMortData2.xdf")
##############################################################################
# Example Code from RevoScaleR Getting Started Guide
##############################################################################


  
##########################################################
# CHAPTER 4: A Tutorial Introduction to RevoScaleR
##########################################################
  
sampleDataDir <- rxGetOption("sampleDataDir")
getwd()
  
# Importing the data
inputFile <- file.path(sampleDataDir, "AirlineDemoSmall.csv")

airDS <- rxImport(inData = inputFile, outFile = "ADS.xdf", 
	missingValueString = "M", stringsAsFactors = TRUE)
colInfo <- list(DayOfWeek = list(type = "factor",
    levels = c("Monday", "Tuesday", "Wednesday", "Thursday", 
    "Friday", "Saturday", "Sunday")))

airDS <- rxImport(inData = inputFile, outFile = "ADS.xdf", 
	missingValueString = "M", colInfo  = colInfo, overwrite = TRUE)
  
# Examining the new data file
nrow(airDS)
ncol(airDS)
head(airDS)

rxGetVarInfo(airDS)
 
# Read 10 rows into a data frame
myData <- rxReadXdf(airDS, numRows=10, startRow=100000)
myData
levels(myData$DayOfWeek)
# Summarizing Your Data 
adsSummary <- rxSummary(~ArrDelay+CRSDepTime+DayOfWeek, data = airDS)
adsSummary <- summary( airDS )
  
adsSummary
  
# Computing summary information by category
rxSummary(~ArrDelay:DayOfWeek, data = airDS)
  
# Drawing histograms for each variable
rxHistogram(~ArrDelay, data = airDS)
rxHistogram(~CRSDepTime, data = airDS)
rxHistogram(~DayOfWeek, data = airDS)
  
# Extracting a subsample into a data frame
myData <- rxDataStep(inData = airDS, 
rowSelection = ArrDelay > 240 & ArrDelay <= 300, 
	varsToKeep = c("ArrDelay", "DayOfWeek"))
rxHistogram(~ArrDelay, data = myData)
  
# Fitting a simple model
arrDelayLm1 <- rxLinMod(ArrDelay ~ DayOfWeek, data = airDS)
summary(arrDelayLm1)
  
# Using the cube argument
arrDelayLm2 <- rxLinMod(ArrDelay ~ DayOfWeek, data = airDS, 
cube = TRUE)
summary(arrDelayLm2)
countsDF <- rxResultsDF(arrDelayLm2, type = "counts")
countsDF
rxLinePlot(ArrDelay~DayOfWeek, data = countsDF,
    main = "Average Arrival Delay by Day of Week")

  
# A linear model with multiple independent variables
arrDelayLm3 <- rxLinMod(ArrDelay ~ DayOfWeek:F(CRSDepTime), 
data = airDS, cube = TRUE)
arrDelayDT <- rxResultsDF(arrDelayLm3, type = "counts")
head(arrDelayDT, 15)
rxLinePlot( ArrDelay~CRSDepTime|DayOfWeek, data = arrDelayDT,
    title = "Average Arrival Delay by Day of Week by Departure Hour")
  
# Creating a subset of the data set and computing a Crosstab
airLateDS <- rxDataStep(inData = airDS, outFile = "ADS1.xdf", 
    varsToDrop = c("CRSDepTime"), 
    rowSelection = ArrDelay > 15)
ncol(airLateDS)
nrow(airLateDS)
myTab <- rxCrossTabs(ArrDelay~DayOfWeek, data = airLateDS)
summary(myTab, output = "means")
  
# Creating a new data set with variable transformations
airExtraDS <- rxDataStep(inData = airDS, outFile="ADS2.xdf", 
	transforms=list( 
		Late = ArrDelay > 15, 
		DepHour = as.integer(CRSDepTime),
		Night = DepHour >= 20 | DepHour <= 5))
                
rxGetInfo(airExtraDS, getVarInfo=TRUE, numRows=5)
  
# Running a logistic regression using the new data
logitObj <- rxLogit(Late~DepHour + Night, data = airExtraDS)
summary(logitObj)
  
# Computing predicted values
predictDS <- rxPredict(modelObject = logitObj, data = airExtraDS,
outData = airExtraDS)
rxGetInfo(predictDS, getVarInfo=TRUE, numRows=5)
  
##########################################################
# CHAPTER 6: Analayzing a Large Data Set with RevoScaleR
##########################################################
  

if (useBigData){
bigAirDS <- RxXdfData( airDataName )
print(
rxGetInfo(bigAirDS, getVarInfo=TRUE)
)

# Reading a chunk of Data
testDF <- rxReadXdf(file = bigAirDS, 
varsToKeep = c("ArrDelay","DepDelay", "DayOfWeek"), 
startRow = 100000, numRows = 1000)
print(
summary(testDF)
)
lmObj <- lm(ArrDelay~DayOfWeek, data = testDF)
print(
summary(lmObj)
)
 
# Estimating a Linear Model with a Huge Data Set
system.time(
delayArr <- rxLinMod(ArrDelay ~ DayOfWeek, data = bigAirDS, 
    cube = TRUE, blocksPerRead = 30)
)
print(
summary(delayArr)
)
 
# Estimate a model for departure delay
 
delayDep <- rxLinMod(DepDelay ~ DayOfWeek, data = bigAirDS, 
cube = TRUE, blocksPerRead = 30)	
  
# Compare the results and plot
# 
cubeResults <- rxResultsDF(delayArr)
cubeResults$DepDelay <- rxResultsDF(delayDep)$DepDelay
rxLinePlot( ArrDelay + DepDelay ~ DayOfWeek, data = cubeResults, 
    title = 'Average Arrival and Departure Delay by Day of Week') 
  
# Turning off progress reports
delayDep <- rxLinMod(DepDelay ~ DayOfWeek, data = bigAirDS, 
cube = TRUE, blocksPerRead = 30, reportProgress = 0)
  
# Handling larger linear models
delayCarrier <- rxLinMod(ArrDelay ~ UniqueCarrier, 
data = bigAirDS, cube = TRUE, blocksPerRead = 30)
dcCoef <- sort(coef(delayCarrier))
print(
head(dcCoef, 10)
) 		
print(
tail(dcCoef, 10)
)
print(
sprintf("United's additional delay compared with Hawaiian: %f",
    dcCoef["UniqueCarrier=UA"]-dcCoef["UniqueCarrier=HA"])
)
  
# Estimating linear models with many independent variables 
delayCarrierLoc <- rxLinMod(ArrDelay ~ UniqueCarrier + Origin+Dest,
    data = bigAirDS, cube = TRUE, blocksPerRead = 30)

dclCoef <- coef(delayCarrierLoc)
print(
sprintf(
"United's additional delay accounting for dep and arr location: %f", 
   dclCoef["UniqueCarrier=UA"]- dclCoef["UniqueCarrier=HA"])
) 
print(
paste("Number of coefficients estimated: ", length(!is.na(dclCoef)))
)
  
# Accounting for hour of day
delayCarrierLocHour <- rxLinMod(ArrDelay ~ 
    	UniqueCarrier + Origin + Dest + F(CRSDepTime), 
data = bigAirDS, cube = TRUE, blocksPerRead = 30)
dclhCoef <- coef(delayCarrierLocHour)
  
# Summarizing the results
print(
sprintf("United's additional delay compared with Hawaiian: %f",
    dcCoef["UniqueCarrier=UA"]-dcCoef["UniqueCarrier=HA"])
)
print(
paste("Number of coefficients estimated: ", length(!is.na(dcCoef)))
)
print(
sprintf(
   "United's additional delay accounting for dep and arr location: %f",
   dclCoef["UniqueCarrier=UA"]- dclCoef["UniqueCarrier=HA"])
)
print(
paste("Number of coefficients estimated: ", length(!is.na(dclCoef)))
)
print(
sprintf(
   "United's additional delay accounting for location and time: %f",    
   dclhCoef["UniqueCarrier=UA"]-dclhCoef["UniqueCarrier=HA"])
)
print(
paste("Number of coefficients estimated: ", length(!is.na(dclhCoef)))
)
  
# Predicting airline delay
expectedDelay <- function( carrier = "AA", origin = "SEA", 
    dest = "SFO", deptime = "9")
{
    coeffNames <- c( 
    sprintf("UniqueCarrier=%s", carrier), 
    sprintf("Origin=%s", origin), 
    sprintf("Dest=%s", dest),
    sprintf("F_CRSDepTime=%s", deptime))
    return (sum(dclhCoef[coeffNames]))
}
  
# Computing predictions for selected flights
# Go to JFK (New York) from Seattle at 5 in the afternoon on United
print(
expectedDelay("AA", "SEA", "JFK", "17")
)
# Go to Newark from Seattle at 5 in the afternoon on United
print(
expectedDelay("UA", "SEA", "EWR", "17")
)
# Or go to Honolulu from Seattle at 7 am on Hawaiian
print(
expectedDelay("HA", "SEA", "HNL", "7")
)
} # End of useBigDataBlock

##########################################################
# CHAPTER 7: An Example Using the 2000 U.S. Census
##########################################################

sampleDataDir <- rxGetOption("sampleDataDir")
dataFile <- file.path( sampleDataDir, "CensusWorkers")
rxGetInfo(dataFile, getVarInfo=TRUE, numRows=3)
  
# Plotting the weighted counts of males and females by age 
ageSex <- rxCube(~F(age):sex, data=dataFile, pweights="perwt")
ageSexDF <- rxResultsDF(ageSex)
rxLinePlot(Counts~age, groups=sex, data=ageSexDF, 
 	title="2000 U.S. Workers by Age and Sex")
  
# Looking at wage income by age and sex
wageAgeSexLm  <- rxLinMod(incwage~F(age):sex, data=dataFile, 
pweights="perwt", cube=TRUE)
wageAgeSex <- rxResultsDF(wageAgeSexLm)
colnames(wageAgeSex) <- c("Age","Sex","WageIncome","Counts" )
rxLinePlot(WageIncome~Age, data=wageAgeSex, groups=Sex, 
title="Wage Income by Age and Sex")
  
# Looking at weeks worked by age and sex
workAgeSexLm <- rxLinMod(wkswork1 ~ F(age):sex, data=dataFile, 
pweights="perwt", cube=TRUE)
workAgeSex <- rxResultsDF(workAgeSexLm)
colnames(workAgeSex) <- c("Age","Sex","WeeksWorked","Counts" )
rxLinePlot( WeeksWorked~Age, groups=Sex, data=workAgeSex, 
main="Weeks Worked by Age and Sex")
  
# Looking at wage income by age, sex, and state
wageAgeSexStateLm  <- rxLinMod(incwage~F(age):sex:state, data=dataFile, 
pweights="perwt", cube=TRUE)
wageAgeSexState <- rxResultsDF(wageAgeSexStateLm)
colnames(wageAgeSexState) <-
c("Age","Sex","State", "WageIncome","Counts" )
rxLinePlot(WageIncome~Age|State, groups=Sex, data=wageAgeSexState, 
layout=c(3,1), main="Wage Income by Age and Sex")

  
# Subsetting an .xdf file into a data frame
ctDataFrame <- rxDataStep (inData=dataFile,
	rowSelection = (state == "Connecticut") & (age <= 50))
nrow(ctDataFrame)
head(ctDataFrame, 5)
summary(ctDataFrame)


##########################################################
# Chapter 8: An Example Analyzing Loan Defaults
##########################################################

sampleDataDir <- rxGetOption("sampleDataDir")

  
# Importing a set of files in a loop using append
append <- "none"
for (i in 2000:2009)
{
    importFile <- paste(mortCsvDataName, i, ".csv", sep="")
    mortDS <- rxImport(importFile, mortXdfFileName, 
        append=append)
    append <- "rows"
}
mortDS <- RxXdfData( mortXdfFileName )
rxGetInfo(mortDS, numRows=5)
  
# Computing summary statistics 
rxSummary(~., data = mortDS, blocksPerRead = 2)
  
# Computing a logistic regression
logitObj <- rxLogit(default~F(year) + creditScore + 
       yearsEmploy + ccDebt,
    	data = mortDS, blocksPerRead = 2, 
      reportProgress = 1)
summary(logitObj)
  
# Computing a Logistic Regression with many parameters
if (useBigData){
system.time(
logitObj <- rxLogit(default  ~ F(houseAge) + F(year)+  
    creditScore + yearsEmploy + ccDebt,
    data = mortDS, blocksPerRead = 2, reportProgress = 1))
print(
summary(logitObj)
)
  
# Extracting the coefficients and plotting them
cc <- coef(logitObj)
df <- data.frame(Coefficient=cc[2:41], HouseAge=0:39)
rxLinePlot(Coefficient~HouseAge,data=df, type="p")
  
# Computing the Probability of Default 
creditScore <- c(300, 700)
yearsEmploy <- c( 2, 8)
ccDebt <- c(5000, 10000)
year <- c(2008, 2009)
houseAge <- c(5, 20)
predictDF <- data.frame(
	creditScore = rep(creditScore, times = 16),
	yearsEmploy = rep(rep(yearsEmploy, each = 2), times = 8),
	ccDebt      = rep(rep(ccDebt, each = 4), times = 4),
	year        = rep(rep(year, each = 8), times = 2),
	houseAge    = rep(houseAge, each = 16))
predictDF <- rxPredict(modelObject = logitObj, data = predictDF, 
    outData = predictDF)
print(
predictDF[order(predictDF$default_Pred, decreasing = TRUE),]
)
} # End of useBigData

##########################################################
# Chapter 9: Writing Your Own Chunking Algorithms
##########################################################

chunkTable <- function(fileName, varsToKeep = NULL, 
     blocksPerRead = 1 )
{
	ProcessChunkAndUpdate <- function( dataList)
	{	
		# Process Data 
		chunkTable <- table(as.data.frame(dataList))
		
		# Update Results
	      tableSum <- chunkTable + .rxGet("tableSum") 
		.rxSet("tableSum", tableSum)
		
		cat("Chunk number: ",.rxChunkNum," tableSum = ", 
                tableSum, "\n") 	
		return( NULL )
	}
	
	updatedObjects <- 
		rxDataStep( inData = fileName, 
                  varsToKeep = varsToKeep,
			blocksPerRead = blocksPerRead, 
			transformObjects = list(tableSum = 0),
			transformFunc = ProcessChunkAndUpdate,
 			returnTransformObjects = TRUE,
			reportProgress = 0)
		
	return(updatedObjects$tableSum)
}
  
if (useBigData){
 
fileName <- file.path(bigDataDir, "AirOnTime7Pct.xdf")
print(
rxGetInfo(fileName, numRows=2)
)
chunkTable(fileName=fileName, varsToKeep="DayOfWeek")
  
} # End of useBigData
 
 

