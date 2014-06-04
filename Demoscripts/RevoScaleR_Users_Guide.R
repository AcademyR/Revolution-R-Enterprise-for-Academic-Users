##############################################################################
# RevoScaleR User's Guide Examples
##############################################################################

#############################################################################
# IMPORTANT: The 'big' example data must be downloaded from the Revolution
# Analytics website before running this script.  The location should be
# specified below.
# 
# The 'robust' and 'biglm' packages should also be installed to run all examples.
#
# Additional Data Sets Used:
# http://www.infochimps.com/datasets/nyse-daily-1970-2010-open-close-high-low-and-volume/downloads/15407
# Click Download 124 MG CSV ZIP Compressed
# Unzip in the bigDataDir specified below, and rename the created
# infochimps subdirectory to NYSE_daily_prices.
# Set the bHasNYSE flag to TRUE below to have tests using this data run.

# http://archive.ics.uci.edu/ml/machine-learning-databases/adult/
# Download adult.data as adult.data.txt and adult.test as adult.test.txt
# Place in bigDataDir specified below
# Set the bHasAdultData flag to TRUE below to have tests using this data run.


##############################################################################
# General setup/cleanup: Run this code before proceeding
##############################################################################

# Uncomment and modify the following lines for your use
bHasNYSE <- TRUE
bHasAdultData <- TRUE
bigDataDir <- "C:/Revolution/Data"

#### ANY FILES IN THE outputDataPath WILL BE DELETED !! ###########
outputDataPath <- "C:/Revolution/Data/OutData"

if (file.exists(outputDataPath))
{
    unlink(outputDataPath, recursive = TRUE)
} 

dir.create(outputDataPath)
rxOptions(outDataPath = outputDataPath, dataPath = outputDataPath)

bigAirData <- file.path(bigDataDir, "AirOnTime87to12/AirOnTime87to12.xdf")	

sampleAirData <- file.path(bigDataDir, "AirOnTime7Pct.xdf")

bigCensusData <- file.path(bigDataDir, "CensusUS5Pct2000.xdf")
	
mortCsvDataName <- file.path(bigDataDir, "mortDefault", "mortDefault")

if (bHasNYSE)
{
	nyseCsvFiles <- file.path(bigDataDir, "NYSE_daily_prices","NYSE",
	"NYSE_daily_prices_")
}
if (bHasAdultData)
{
	adultDataFile <- file.path(bigDataDir, "adult.data.txt")
	adultTestFile <- file.path(bigDataDir, "adult.test.txt")
}


##############################################################################
# Example Code from RevoScaleR User's Guide
##############################################################################


######################################################## 
# Chapter 1: Introduction
Ch1Start <- Sys.time()

inFile <- file.path(rxGetOption("sampleDataDir"), "AirlineDemoSmall.csv")
airData <- rxImport(inData=inFile, outFile = "airExample.xdf", 
	stringsAsFactors = TRUE, missingValueString = "M", rowsPerRead = 200000)
rxGetInfo(airData, getVarInfo = TRUE)
rxHistogram(~ArrDelay|DayOfWeek,  data = airData)
rxSummary(~ ArrDelay, data = airData )
airData <- rxDataStep(inData = airData, outFile = "airExample.xdf", 
    transforms=list(VeryLate = (ArrDelay > 120 | is.na(ArrDelay))), 
    overwrite = TRUE)
logitResults <- rxLogit(VeryLate ~ DayOfWeek, data = airData )
summary(logitResults)
logitResults2 <- rxLogit(VeryLate ~ DayOfWeek - 1, data = airData )
summary(logitResults2)

#  Writing Your Own Analyses for Large Data Sets

ProcessAndUpdateData <- function( data )
{
    # Process Data
    notMissing <- !is.na(data$ArrDelay)
    morning <- data$CRSDepTime >= 6 & data$CRSDepTime < 12 & notMissing
    afternoon <- data$CRSDepTime >= 12 & data$CRSDepTime < 17 & notMissing
    evening <- data$CRSDepTime >= 17 & data$CRSDepTime < 23 & notMissing
    mornArr <- sum(data$ArrDelay[morning], na.rm = TRUE)      
    mornCounts <- sum(morning, na.rm = TRUE) 
    afterArr <- sum(data$ArrDelay[afternoon], na.rm = TRUE) 
    afterCounts <- sum(afternoon, na.rm = TRUE)
    evenArr <- sum(data$ArrDelay[evening], na.rm = TRUE) 
    evenCounts <- sum(evening, na.rm = TRUE)
    
   
 # Update Results
   .rxSet("toMornArr", mornArr + .rxGet("toMornArr"))
   .rxSet("toMornCounts", mornCounts + .rxGet("toMornCounts"))
   .rxSet("toAfterArr", afterArr + .rxGet("toAfterArr"))
   .rxSet("toAfterCounts", afterCounts + .rxGet("toAfterCounts"))
   .rxSet("toEvenArr", evenArr + .rxGet("toEvenArr"))
   .rxSet("toEvenCounts", evenCounts + .rxGet("toEvenCounts"))

    return( NULL )
}
totalRes <- rxDataStep( inData = airData , returnTransformObjects = TRUE,
    transformObjects = 
        list(toMornArr = 0, toAfterArr = 0, toEvenArr = 0, 
        toMornCounts = 0, toAfterCounts = 0, toEvenCounts = 0),
    transformFunc = ProcessAndUpdateData,
    transformVars = c("ArrDelay", "CRSDepTime"))
FinalizeResults <- function(totalRes)
{
    return(data.frame(
        AveMorningDelay = totalRes$toMornArr / totalRes$toMornCounts,
        AveAfternoonDelay = totalRes$toAfterArr / totalRes$toAfterCounts,
        AveEveningDelay = totalRes$toEvenArr / totalRes$toEvenCounts))
}
FinalizeResults(totalRes)
rxSummary(~ArrDelay, data = "airExample.xdf", 
rowSelection = CRSDepTime >= 6 & CRSDepTime < 12 & !is.na(ArrDelay))

#  Sample Data for Use with RevoScaleR

list.files(system.file("SampleData", package = "RevoScaleR"))

######################################################## 
# Chapter 2: Importing Data
Ch2Start <- Sys.time()

#  Importing Delimited Text Data
readPath <- rxGetOption("sampleDataDir")
infile <- file.path(readPath, "claims.txt")
claimsDF <- rxImport(infile)
rxGetInfo(claimsDF, getVarInfo = TRUE)
names(claimsDF)
claimsDS <- rxImport(inData = infile, outFile = "claims.xdf")
rxGetInfo( claimsDS, getVarInfo = TRUE)
names( claimsDS )
inFile <- file.path(rxGetOption("sampleDataDir"), "AirlineDemoSmall.csv")
airData <- rxImport(inData = inFile, outFile="airExample.xdf", 
	stringsAsFactors = TRUE, missingValueString = "M", 
	rowsPerRead = 200000, overwrite = TRUE)

#  Importing Fixed Format Data

inFile <- file.path(readPath, "claims.sts")
claimsFF <- rxImport(inData = inFile, outFile = "claimsFF.xdf")
rxGetInfo(claimsFF, getVarInfo=TRUE)	
claimsFF2 <- rxImport(inFile, outFile = "claimsFF2.xdf", stringsAsFactors=TRUE)
rxGetInfo(claimsFF2, getVarInfo=TRUE)
inFileNS <- file.path(readPath, "claims.dat")
outFileNS <- "claimsNS.xdf"	
colInfo=list("rownum" = list(start = 1, width = 3, type = "integer"),
             "age" = list(start = 4, width = 5, type = "factor"),
             "car.age" = list(start = 9, width = 3, type = "factor"),
             "type" = list(start = 12, width = 1, type = "factor"),
             "cost" = list(start = 13, width = 6, type = "numeric"),
             "number" = list(start = 19, width = 3, type = "numeric"))
claimsNS <- rxImport(inFileNS, outFile = outFileNS, colInfo = colInfo)
rxGetInfo(claimsNS, getVarInfo=TRUE)

#  Importing SAS Data

inFileSAS <- file.path(rxGetOption("sampleDataDir"), "claims.sas7bdat")
xdfFileSAS <- "claimsSAS.xdf"
claimsSAS <- rxImport(inData = inFileSAS, outFile = xdfFileSAS)
rxGetInfo(claimsSAS, getVarInfo=TRUE)

#  Importing SPSS Data

inFileSpss <- file.path(rxGetOption("sampleDataDir"), "claims.sav")
xdfFileSpss <- "claimsSpss.xdf"
claimsSpss <- rxImport(inData = inFileSpss, outFile = xdfFileSpss)
rxGetInfo(claimsSpss, getVarInfo=TRUE)

#  Specifying Variable Data Types

inFile <- file.path(rxGetOption("sampleDataDir"), "claims.sts")
rxImport(infile, outFile = "claimsSAF.xdf", stringsAsFactors = TRUE)
rxGetInfo("claimsSAF.xdf", getVarInfo = TRUE)

outfileColClass <- "claimsCCNum.xdf"
rxImport(infile, outFile = outfileColClass, colClasses=c(number = "integer"))
rxGetInfo("claimsCCNum.xdf", getVarInfo = TRUE)
 
outfileCAOrdered <- "claimsCAOrdered.xdf"
colInfoList <- list("car.age"= list(type = "factor", levels = c("0-3", 
    "4-7", "8-9", "10+")))
rxImport(infile, outFile = outfileCAOrdered, colInfo = colInfoList)
rxGetInfo("claimsCAOrdered.xdf", getVarInfo = TRUE)
   
outfileCAOrdered2 <- "claimsCAOrdered2.xdf"
colInfoList <- list("car.age" = list(type = "factor", levels = c("0-3", 
    "4-7", "8-9", "10+")))
claimsOrdered <- rxImport(infile, outfileCAOrdered2, 
	colClasses = c(number = "integer"), 
    	colInfo = colInfoList, stringsAsFactors = TRUE)
rxGetInfo(claimsOrdered, getVarInfo = TRUE) 

#  Specifying Additional Variable Information

inFileAddVars <- file.path(rxGetOption("sampleDataDir"), "claims.txt")
outfileTypeRelabeled <- "claimsTypeRelabeled.xdf"
colInfoList <- list("type" = list(type = "factor", levels = c("A", 
    "B", "C", "D"), newLevels=c("Subcompact", "Compact", "Mid-size", 
    "Full-size"), description="Body Type"))
claimsNew <- rxImport(inFileAddVars, outFile = outfileTypeRelabeled, 
	colInfo = colInfoList)
rxGetInfo(claimsNew, getVarInfo = TRUE) 

#  Appending to an Existing File

inFile <- file.path(rxGetOption("sampleDataDir"), "claims.txt")
colInfoList <- list("car.age" = list(type = "factor", levels = c("0-3", 
    "4-7", "8-9", "10+")))
outfileCAOrdered2 <- "claimsCAOrdered2.xdf"

claimsAppend <- rxImport(inFile, outFile = outfileCAOrdered2, 
	colClasses = c(number = "integer"),
     colInfo = colInfoList, stringsAsFactors = TRUE, append = "rows")
rxGetInfo(claimsAppend, getVarInfo=TRUE) 

 
#  Transforming Data on Import

inFile <- file.path(rxGetOption("sampleDataDir"), "claims.txt")
outfile <- "claimsXform.xdf"
claimsDS <-rxImport(infile, outFile = outfile, 
	transforms=list(logcost=log(cost)))
rxGetInfo(claimsDS, getVarInfo=TRUE)

#  Splitting Data Files
rxSetComputeContext("local")
splitFiles <- rxSplit(bigCensusData, numOutFiles = 5, splitBy = "blocks", 
	varsToKeep = c("age",  "incearn", "incwelfr", "educrec", "metro", "perwt")) 
names(splitFiles)
nodePaths <- paste("compute", 10:13, sep="")
baseNames <- file.path(outputDataPath, nodePaths, "DistCensusData")
splitFiles2 <- rxSplit(bigCensusData, splitBy = "blocks", 
	outFilesBase = baseNames, 
	varsToKeep = c("age", "incearn", "incwelfr", "educrec", "metro", "perwt")) 
names(splitFiles2)
splitFiles3 <- rxSplit(bigCensusData, splitBy = "blocks", 
	outFileSuffixes=paste("-", 1:5, sep=""),
	varsToKeep = c("age", "incearn", "incwelfr", "educrec", "metro", "perwt")) 
names(splitFiles3)
set.seed(257)
splitFiles4 <- rxSplit(inData = bigCensusData, 
	outFilesBase="censusData", 
	splitByFactor="testSplitVar", 
	varsToKeep = c("age", "incearn", "incwelfr", "educrec", "metro", "perwt"), 
	transforms=list(testSplitVar = factor( 
		sample(0:1,	size=.rxNumRows, replace=TRUE, prob=c(.10, .9)), 
		levels=0:1, labels = c("Test", "Train"))))
names(splitFiles4)
rxSummary(~age, data = splitFiles4[[1]], reportProgress = 0)
rxSummary(~age, data = splitFiles4[[2]], reportProgress = 0)

#  Reading Data from an .xdf File into a Data Frame

inFile <- file.path(rxGetOption("sampleDataDir"), "CensusWorkers.xdf")
myCensusDF <- rxDataStep(inData=inFile, 
  rowSelection = state == "Washington" & age > 40,
  varsToKeep = c("age", "perwt", "sex"))
myCensusSample <- rxDataStep(inData=inFile, 
    rowSelection= (seq(from=.rxStartRow,length.out=.rxNumRows) %% 10) == 0 )
myCensusDF2 <- rxDataStep(inData=inFile, 
  varsToKeep = c("age", "perwt", "sex"),
  transforms=list(ageGroup = cut(age, seq(from=20, to=70, by=10))))

#  Converting .xdf Files to Text

rxXdfToText("claims.xdf", "claims.csv")
rxXdfToText("claims.xdf", "claims.txt", sep=" ")
rxXdfToText("claims.xdf", "claims.txt", sep=" ", varsToDrop="number", 
            overwrite=TRUE)


######################################################## 
# Chapter 3: Data Sources
Ch3Start <- Sys.time()

# Methods for Looking at Data Sources

readPath <- rxGetOption("sampleDataDir")
infile <- file.path(readPath, "claims.txt")
claimsDS <- rxImport(inData = infile, outFile = "claims.xdf", 
	overwrite = TRUE)
dim(claimsDS)
colnames(claimsDS)
dimnames(claimsDS)
length(claimsDS)
head(claimsDS)
tail(claimsDS)

# Using Data Sources

inFileSAS <- file.path(rxGetOption("sampleDataDir"), "claims.sas7bdat") 
sourceDataSAS <- RxSasData(inFileSAS, stringsAsFactors=TRUE)
names(sourceDataSAS)
rxLinMod(cost ~ age + car_age, data = sourceDataSAS)

inFileSpss <- file.path(rxGetOption("sampleDataDir"), "claims.sav") 
sourceDataSpss <- RxSpssData(inFileSpss, stringsAsFactors=TRUE)
rxLinMod(cost ~ age + car_age, data=sourceDataSpss)

# Working with an Xdf Data Source

claimsPath <-  file.path(rxGetOption("sampleDataDir"), "claims.xdf")
claimsDs <- RxXdfData(claimsPath)
rxOpen(claimsDs)
claims <- rxReadNext(claimsDs)
rxClose(claimsDs)

# Using an Xdf Data Source with biglm

require(biglm)
biglmxdf <- function(dataSource, formula)
{	
	moreData <- TRUE
	df <- rxReadNext(dataSource)
	biglmRes <- biglm(formula, df)	
	while (moreData)
	{
		df <- rxReadNext(dataSource)	
		if (length(df) != 0)
		{
			biglmRes <- update(biglmRes, df)						
		}
		else
        {
			moreData <- FALSE
        }
	}							
	return(biglmRes)			
}

dataSource <- RxXdfData(bigAirData, 
	varsToKeep = c("DayOfWeek", "DepDelay","ArrDelay"), blocksPerRead = 15)
rxOpen(dataSource)
system.time(bigLmRes <- biglmxdf(dataSource, ArrDelay~DayOfWeek))
rxClose(dataSource)
summary(bigLmRes)

######################################################## 
# Chapter 4: Transforming and Subsetting Data
Ch4Start <- Sys.time()


# Create a data frame
set.seed(59)
myData <- data.frame(
x = rnorm(100), 
y = runif(100), 
z = rep(1:20, times = 5))

# Subset observations and variables
myNewData <- rxDataStep( inData = myData,
	rowSelection = y > .5,
	varsToKeep = c("y", "z"))

# Get information about the new data frame
rxGetInfo(data = myNewData, getVarInfo = TRUE)

readPath <- rxGetOption("sampleDataDir")
censusWorkers <- file.path(readPath, "CensusWorkers.xdf")
partWorkers <- rxDataStep(inData = censusWorkers, rowSelection = wkswork1 < 30,
              varsToKeep = c("age", "sex", "wkswork1", "incwage", "perwt"))
rxGetInfo(partWorkers, getVarInfo = TRUE)
partWorkersDS <- rxDataStep(inData = censusWorkers, 
		outFile = "partWorkers.xdf", 
        rowSelection = wkswork1 < 30,
        varsToDrop = c("state"), overwrite = TRUE)
rxGetVarInfo( partWorkersDS )

#  Transforming Data with rxDataStep

expData <- data.frame(
   BuyDate = c("2011/10/1", "2011/10/1", "2011/10/1", 
    "2011/10/2", "2011/10/2", "2011/10/2", "2011/10/2", 
    "2011/10/3", "2011/10/4", "2011/10/4"),
   Food =      c( 32, 102,  34,  5,   0, 175,  15, 76, 23, 14),
   Wine =      c( 0,  212,   0,  0, 425,  22,   0, 12,  0, 56),
   Garden =    c( 0,   46,   0,  0,   0,  45, 223,  0,  0, 0),
   House = c( 22,  72,  56,  3,   0,  0,    0, 37, 48, 23),
   Sex =   factor(c("F", "F", "M", "M", "M", "F", "F", "F", "M", "F")),
   Age =          c( 20,  51,  32,  16,  61, 42,  35, 99, 29, 55),
   stringsAsFactors = FALSE)
			newExpData = rxDataStep( inData = expData, 
    transforms = list(
        Total      = Food + Wine + Garden + House,
        AveCat     = Total/4,
        Age        = ifelse(Age == 99, NA, Age),
        UnderAge   = Age < 21,
        Day        = (as.POSIXlt(BuyDate))$wday, 
        Day        = factor(Day, levels = 0:6, 
                        labels = c("Su","M","Tu", "W", "Th","F", "Sa")),
        SpendCat   = cut(Total, breaks=c(0, 75, 250, 10000), 
				   labels=c("low", "medium", "high"), right=FALSE),
        FoodWine = ifelse( Food > 50, TRUE, FALSE),
        FoodWine = ifelse( Wine > 50, TRUE, FoodWine),
        BuyDate  = NULL))
newExpData
# Create a data frame with missing values
set.seed(59)
myData1 <- data.frame(x = rnorm(100), y = runif(100))

xmiss <- seq.int(from = 5, to = 100, by = 5)
ymiss <- seq.int(from = 2, to = 100, by = 5)
myData1$x[xmiss] <- NA
myData1$y[ymiss] <- NA
rxGetInfo(myData1, numRows = 5)
# Compute the summary statistics and extract
# the means in a named vector
sumStats <- rxResultsDF(rxSummary(~., myData1))
sumStats
meanVals <- sumStats$Mean
names(meanVals) <- row.names(sumStats)
# Use rxDataStep to replace missings with imputed mean values
myData2 <- rxDataStep(inData = myData1, transforms = list(
    x = ifelse(is.na(x), meanVals["x"], x),
    y = ifelse(is.na(y), meanVals["y"], y)),
    transformObjects = list(meanVals = meanVals))
rxGetInfo(myData2, numRows = 5)
rxDataStep (inData = censusWorkers, outFile = "newCensusWorkers",
varsToDrop = c("state"), transforms = list(
	ageFactor = cut(age, breaks=seq(from = 20, to = 70, by = 5), 
    right = FALSE)))
rxGetInfo("newCensusWorkers", getVarInfo = TRUE)
educExp <- c(Connecticut=1795.57, Washington=1170.46, Indiana = 1289.66)
censusWorkers <- file.path(rxGetOption("sampleDataDir"), "CensusWorkers.xdf")
rxDataStep(inData = censusWorkers, outFile = "censusWorkersWithEduc",
	transforms = list(
         stateEducExpPC = educExp[match(state, names(educExp))] ), 
	transformObjects= list(educExp=educExp))
rxGetInfo("censusWorkersWithEduc.xdf",getVarInfo=TRUE)

#  Using the Data Step to Create an .xdf File from a Data Frame

set.seed(39)
myData <- data.frame(x1 = rnorm(10000), x2 = runif(10000))

rxDataStep(inData = myData, outFile = "testFile.xdf",
    rowSelection = x2 > .1,
    transforms = list( x3 = x1 + x2 ),
    rowsPerRead = 5000 )
rxGetInfo("testFile.xdf")


#  Re-Blocking an .xdf File

fileName <- file.path(rxGetOption("sampleDataDir"), "CensusWorkers.xdf")
rxGetInfo(fileName, getBlockSizes = TRUE)

newFile <- "censusWorkersEvenBlocks.xdf"
rxDataStep(inData = fileName, outFile = newFile, rowsPerRead = 60000)
rxGetInfo(newFile, getBlockSizes = TRUE)	

#  Modifying Variable Information

newVarInfo <- list(
    incwage = list(newName = "WageIncome"),
    state   = list(newName = "State", description = "State of Residence"),
    stateEducExpPC = list(description = "State Per Capita Educ Exp"))     
fileName <- "censusWorkersWithEduc.xdf"
rxSetVarInfo(varInfo = newVarInfo, data = fileName)
rxGetVarInfo( fileName )


#  Sorting Data

censusWorkers <- file.path(rxGetOption("sampleDataDir"), "CensusWorkers.xdf")
outXDF <- "censusWorkersSorted.xdf"
rxSort(inData = censusWorkers, outFile = outXDF, sortByVars=c("age",
	"incwage"), decreasing=c(FALSE, TRUE))
rxGetInfo(outXDF, numRows=10)
set.seed(17)
users <- sample(c("Aiden", "Ella", "Jayden", "Ava", "Max", "Grace", "Riley", 
	              "Lolita", "Liam", "Emma", "Ethan", "Elizabeth", "Jack", 
	              "Genevieve", "Avery", "Aurora", "Dylan", "Isabella",
	              "Caleb", "Bella"), 100, replace=TRUE)
state <- sample(c("Washington", "California", "Texas", "North Carolina", 
	              "New York", "Massachusetts"), 100, replace=TRUE)
transAmt <- round(runif(100)*100, digits=3)
df <- data.frame(users=users, state=state, transAmt=transAmt)

rxSort(df, sortByVars=c("users", "state"), removeDupKeys=TRUE, 
    dupFreqVar = "DUP_COUNT")	
sampleDataDir <- rxGetOption("sampleDataDir")
airDemo <- file.path(sampleDataDir, "AirlineDemoSmall.xdf")
airDedup <- file.path(outputDataPath, "rxAirDedup.xdf")
rxSort(inData = airDemo, outFile = airDedup, 
	sortByVars =  c("DayOfWeek", "CRSDepTime", "ArrDelay"),
  	removeDupKeys = TRUE, dupFreqVar = "FreqWt")
rxGetInfo(airDedup)

linModObj <- rxLinMod(ArrDelay~CRSDepTime + DayOfWeek, data = airDedup, 
	fweights = "FreqWt")
summary(linModObj)


rxLinMod(formula = ArrDelay ~ CRSDepTime + DayOfWeek, data = airDedup, 
    fweights = "FreqWt")
linModObjBig <- rxLinMod(ArrDelay~CRSDepTime + DayOfWeek, data = airDemo)
summary(linModObjBig)


#  The rxQuantile Function and the Five-Number Summary

readPath <- rxGetOption("sampleDataDir")
AirlinePath <- file.path(readPath, "AirlineDemoSmall.xdf")
rxQuantile("ArrDelay", AirlinePath)


#  Merging Data

acct <- c(0538, 0538, 0538, 0763, 1534)
billee <- c("Rich C", "Rich C", "Rich C", "Tom D", "Kath P")
patient <- c(1, 2, 3, 1, 1)
acctDF<- data.frame( acct=acct, billee= billee, patient=patient)
acct <- c(0538, 0538, 0538, 0538, 0763, 0763, 0763)
patient <- c(3, 2, 2, 3, 1, 1, 2)
type <- c("OffVisit", "AdultPro", "OffVisit", "2SurfCom", "OffVisit", "AdultPro", "OffVisit")
procedureDF <- data.frame(acct=acct, patient=patient, type=type)
rxMerge(inData1 = acctDF, inData2 = procedureDF, type = "inner", 
	matchVars=c("acct", "patient"))

rxMerge(inData1 = acctDF, inData2 = procedureDF, type = "left", 
	matchVars=c("acct", "patient"))

rxMerge(inData1 = acctDF, inData2 = procedureDF, type = "right", 
	matchVars=c("acct", "patient"))
rxMerge(inData1 = acctDF, inData2 = procedureDF, type = "full", 
	matchVars=c("acct", "patient"))


myData1 <- data.frame( x1 = 1:3, y1 = c("a", "b", "c"), z1 = c("x", "y", "z"))
myData2 <- data.frame( x2 = 101:103, y2 = c("d", "e", "f"), 
          z2 = c("u", "v", "w"))
rxMerge(inData1 = myData1, inData2 = myData2, type = "oneToOne")

names(myData2) <- c("x1", "x2", "x3")
rxMerge(inData1 = myData1, inData2 = myData2, type = "union")

claimsXdf <- file.path(rxGetOption("sampleDataDir"), "claims.xdf")

rxMerge(inData1 = claimsXdf, inData2 = claimsXdf, outFile = "claimsTwice.xdf", 
	type = "union")
censusWorkers <- file.path(rxGetOption("sampleDataDir"), "CensusWorkers.xdf")
rxGetVarInfo(censusWorkers, varsToKeep = "state")
educExp <- data.frame(state=c("Connecticut", "Washington", "Indiana"),
	EducExp = c(1795.57,1170.46,1289.66 ))

rxMerge(inData1 = censusWorkers, inData2 = educExp, 
	outFile="censusWorkersEd.xdf", matchVars = "state", overwrite=TRUE)
rxGetVarInfo("censusWorkersEd.xdf")

#  Recoding Factors

set.seed(100)
sex <- factor(sample(c("M","F"), size = 10, replace = TRUE), 
              levels = c("M", "F"))
DF <- data.frame(sex = sex, score = rnorm(10))
DF[["sex"]]
newDF <- rxFactors(inData = DF, overwrite = TRUE,
          factorInfo = list(Gender = list(newLevels = c(Female = "F", 
                                              Male = "M"), 
                                          varName = "sex")))
newDF$Gender 	
# Combining factor levels
  
surveyDF <- rxDataStep(inData = 
	file.path(rxGetOption("sampleDataDir"),"CustomerSurvey.xdf"))
sl <- levels(surveyDF[[1]])
quarterList <-  list(newLevels = list(
                         "Largely Satisfied" = sl[1:2],
                         "Neither Satisfied Nor Dissatisfied" = sl[3:5],
                         "Largely Dissatisfied" = sl[6:7]))
surveyDF <- rxFactors(inData = surveyDF,
            factorInfo <- list(
                   Q1 = quarterList,
                   Q2 = quarterList,
                   Q3 = quarterList,
                   Q4 = quarterList))
surveyDF[["Q1"]]	
 

######################################################## 
# Chapter 6: Data Summaries
#  Using Variable Information
Ch6Start <- Sys.time()

  
readPath <- rxGetOption("sampleDataDir")
censusWorkers <- file.path(readPath, "CensusWorkers.xdf")
censusWorkerInfo <- rxGetVarInfo(censusWorkers)
names(censusWorkerInfo)
names(censusWorkerInfo$age)
censusWorkerInfo$age$high
censusWorkerInfo$age$low	
outputDir <- rxGetOption("outDataPath")
tempCensusWorkers <- file.path(outputDir, "tempCensusWorkers.xdf")
file.copy(from = censusWorkers, to = tempCensusWorkers)
censusWorkerInfo$age$low <- 35
censusWorkerInfo$age$high <- 50
rxSetVarInfo(censusWorkerInfo, tempCensusWorkers)
rxSummary(~F(age), data = tempCensusWorkers)
censusWorkerInfo$age$low <- 20
censusWorkerInfo$age$high <- 65
rxSetVarInfo(censusWorkerInfo, "tempCensusWorkers.xdf")
# Formulas in rxSummary
  
readPath <- rxGetOption("sampleDataDir")
censusWorkers <- file.path(readPath, "CensusWorkers.xdf")
rxSummary(~ age + incwage + perwt + sex + wkswork1, data = censusWorkers)
rxSummary(~ ArrDelay:DayOfWeek, data = file.path(readPath, 
    "AirlineDemoSmall.xdf")) 
rxSummary(~ incwage:F(age), data = censusWorkers)
rxSummary(~ sex:state, pweights = "perwt", data = censusWorkers)
rxSummary(~ sex:F(age, low = 30, high = 39), data = censusWorkers,
      pweights="perwt", rowSelection = age >= 30 & age < 40)
# Writing By-Group Summary Statistics to an .xdf File
  
readPath <- rxGetOption("sampleDataDir")
censusWorkers <- file.path(readPath, "CensusWorkers.xdf")
rxSummary(~ incwage:F(age):sex + wkswork1:F(age):sex, data = censusWorkers,
	byGroupOutFile = "ByAge.xdf", 
	summaryStats = c("Mean", "StdDev", "SumOfWeights"), 
	pweights = "perwt", overwrite = TRUE)

rxGetInfo("ByAge.xdf", numRows = 5)

rxLinePlot(incwage_Mean~F_age, groups = sex, data = "ByAge.xdf")

#Transforming data in rxSummary

rxSummary(~ log(incwage), data = censusWorkers)
  
######################################################## 
# Chapter 7: Crosstabs
Ch7Start <- Sys.time()

  
UCBADF <- as.data.frame(UCBAdmissions)
z <- rxCube(Freq ~ Gender:Admit, data = UCBADF)
z2 <- rxCube(Freq ~ Gender:Admit:Dept, data = UCBADF)
z2

#  Letting the data speak: Example 1

censusWorkers <- file.path(readPath, "CensusWorkers.xdf")
censusWorkersCube <- rxCube(incwage ~ F(age), data=censusWorkers)
censusWorkersCubeDF <- rxResultsDF(censusWorkersCube)
rxLinePlot(incwage ~ age, data=censusWorkersCubeDF, 
    title="Relationship of Income and Age")
  
#  Transforming Data
  
library(rpart)
rxCube(~ Kyphosis:Age, data = kyphosis, 
    transforms=list(Age = cut(Age, breaks=c(0, 12, 60, 119, 
    180, 220), labels=c("<1", "1-4", "5-9", "10-15", ">15"))))
rxCube(~ Kyphosis:F(Number), data = kyphosis)

rxCube(~ Kyphosis:Start, data = kyphosis, 
    transforms=list(Start = cut(Start, breaks=c(0, 7.5, 19.5), 
    labels=c("cervical", "thoracic"))))
  
#  Cross-Tabulation with rxCrossTabs
  
z3 <- rxCrossTabs(Freq ~ Gender:Admit:Dept, data = UCBADF)
z3
summary(z3)
  
#  A Large Data Example
  
arrDelayXT <- rxCrossTabs(ArrDelay ~ UniqueCarrier:DayOfWeek, 
    data = bigAirData, blocksPerRead = 30)
print(arrDelayXT)
  
#  Tests of Independence on Cross-Tabulated Data
  
arrDelayXTab <- rxCrossTabs(ArrDel15~ UniqueCarrier:DayOfWeek, 
    data = bigAirData, blocksPerRead = 30, returnXtabs=TRUE)
rxChiSquaredTest(arrDelayXTab)
catcherr <- tryCatch(rxFisherTest(arrDelayXTab),error=function(e) e)
identical(catcherr$message,"FEXACT error 40.\nOut of workspace.")
catcherr
UCBADF <- as.data.frame(UCBAdmissions)
admissCTabs <- rxCrossTabs(Freq ~ Gender:Admit, data = UCBADF, 
    returnXtabs=TRUE)
rxFisherTest(admissCTabs)
rxChiSquaredTest(admissCTabs)	
admissCTabs2 <- rxCrossTabs(Freq ~ Gender:Admit:Dept, data = UCBADF,
    returnXtabs=TRUE)
rxChiSquaredTest(admissCTabs2)
rxFisherTest(admissCTabs2)
rxKendallCor(admissCTabs2)
  
#  Odds Ratios and Risk Ratios
  
admissCTabs
rxOddsRatio(admissCTabs)
rxRiskRatio(admissCTabs)
  
######################################################## 
# Chapter 8: Fitting Linear Models
Ch8Start <- Sys.time()

  
readPath <- rxGetOption("sampleDataDir")
airlineDemoSmall <- file.path(readPath, "AirlineDemoSmall.xdf")
rxLinMod(ArrDelay ~ DayOfWeek, data = airlineDemoSmall)
arrDelayLm1 <- rxLinMod(ArrDelay ~ DayOfWeek, cube = TRUE, 
    data = airlineDemoSmall)
  
#  Obtaining a Summary of a Model
   
summary(arrDelayLm1)
  
#  Using Probability Weights
  
rxLinMod(incwage ~ F(age), pweights = "perwt", data = censusWorkers)
  
#  Using Frequency Weights
  
fourthgraders <- file.path(rxGetOption("sampleDataDir"), 
    "fourthgraders.xdf")
fourthgradersLm <- rxLinMod(height ~ eyecolor, data = fourthgraders,
    fweights="reps")
  
#  Using rxLinMod with R Data Frames
  
rxLinMod(dist ~ speed, data = cars)
  
#  Using the Cube Option for Conditional Predictions
  
xfac1 <- factor(c(1,1,1,1,2,2,2,2,3,3,3,3), labels=c("One1", "Two1", "Three1"))
xfac2 <- factor(c(1,1,1,1,1,1,2,2,2,3,3,3), labels=c("One2", "Two2", "Three2"))
set.seed(100)
y <- as.integer(xfac1) + as.integer(xfac2)* 2 + rnorm(12)
myData <- data.frame(y, xfac1, xfac2)
myLinMod <- rxLinMod(y ~ xfac1, data = myData, cube = TRUE)
myLinMod
myLinMod$countDF
myLinMod1 <- rxLinMod(y~xfac1 + xfac2, data = myData, 
    cube = TRUE, cubePredictions = TRUE)
myLinMod1
myLinMod1$countDF
myCoef <- coef(myLinMod1)
avexfac2c <- .5*myCoef[4] + .25*myCoef[5]
condMean1 <- myCoef[1] + avexfac2c
condMean1
  
#  Fitted Values, Residuals, and Prediction
  
trainingDataFile <- "AirlineData06to07.xdf"
targetInfile <- "AirlineData08.xdf"
rxDataStep(sampleAirData, trainingDataFile, rowSelection = Year == 1999 |
	Year == 2000 | Year == 2001 | Year == 2002 | Year == 2003 |
	Year == 2004 | Year == 2005 | Year == 2006 | Year == 2007)
rxDataStep(sampleAirData, targetInfile, rowSelection = Year == 2008)	
arrDelayLm2 <- rxLinMod(ArrDelay ~ DayOfWeek + UniqueCarrier + Dest, 
    data = trainingDataFile)
rxPredict(arrDelayLm2, data = targetInfile, outData = targetInfile)
rxGetInfo(targetInfile,  numRows = 5)
  
# Standard Errors, Confidence Intervals, and Prediction Intervals
  
neaInc <- c(-94, -57, -29, 135, 143, 151, 245, 355, 392, 473, 486, 535, 571, 
    580, 620, 690)
fatGain <- c( 4.2, 3.0, 3.7, 2.7, 3.2, 3.6, 2.4, 1.3, 3.8, 1.7, 1.6, 2.2, 1.0,
    0.4, 2.3, 1.1)
ips132df <- data.frame(neaInc = neaInc, fatGain=fatGain)
ips132lm <- rxLinMod(fatGain ~ neaInc, data=ips132df, covCoef=TRUE)
ips132lmPred <- rxPredict(ips132lm, data=ips132df, computeStdErrors=TRUE,
    interval="confidence", writeModelVars = TRUE)
ips132lmPred$fatGain_StdErr
rxLinePlot(fatGain + fatGain_Pred + fatGain_Upper + fatGain_Lower ~ neaInc,
	data = ips132lmPred, type = "b", 
	lineStyle = c("blank", "solid", "dotted", "dotted"),
	lineColor = c(NA, "red", "black", "black"),
	symbolStyle = c("solid circle", "blank", "blank", "blank"),
	title = "Data, Predictions, and Confidence Bounds",
	xTitle = "Increase in Non-Exercise Activity",
	yTitle = "Increse in Fat", legend = FALSE)
ips132lmPred2 <- rxPredict(ips132lm, data=ips132df, computeStdErrors=TRUE,
    interval="prediction", writeModelVars = TRUE)
rxLinePlot(fatGain + fatGain_Pred + fatGain_Upper + fatGain_Lower ~ neaInc,
	data = ips132lmPred2, type = "b", 
	lineStyle = c("blank", "solid", "dotted", "dotted"),
	lineColor = c(NA, "red", "black", "black"),
	symbolStyle = c("solid circle", "blank", "blank", "blank"),
	title = "Prediction Intervals",
	xTitle = "Increase in Non-Exercise Activity",
	yTitle = "Increse in Fat", legend = FALSE)
arrDelayLmVC <- rxLinMod(ArrDelay ~ DayOfWeek + UniqueCarrier + Dest, 
    data = trainingDataFile, covCoef=TRUE)
rxPredict(arrDelayLmVC, data = targetInfile, outData = targetInfile,	
    computeStdErrors=TRUE, interval = "confidence", overwrite=TRUE)
rxGetInfo(targetInfile, numRows=10)
  
#  Stepwise Linear Regression
  
rxGetVarInfo(trainingDataFile)
initialModel <- rxLinMod(ArrDelay ~ DayOfWeek + CRSDepTime,
    data = trainingDataFile)
initialModel
airlineStepModel <- rxLinMod(ArrDelay ~ DayOfWeek + CRSDepTime,
	data = trainingDataFile,
	variableSelection = rxStepControl(method="stepwise", 
		scope = ~ DayOfWeek + CRSDepTime + CRSElapsedTime + 
			Distance + TaxiIn + TaxiOut ))
  
#	Specifying Model Scope
  
form <- Sepal.Length ~ Sepal.Width + Petal.Length
scope <- list(
    lower = ~ Sepal.Width,
    upper = ~ Sepal.Width + Petal.Length + Petal.Width * Species)
   
varsel <- rxStepControl(method = "stepwise", scope = scope)
rxlm.step <- rxLinMod(form, data = iris, variableSelection = varsel,
    verbose = 1, dropMain = FALSE, coefLabelStyle = "R")
#  
# Specifying selection criterion
#  
airlineStepModelSigLevel <- rxLinMod(ArrDelay ~ DayOfWeek + CRSDepTime,
	data = trainingDataFile, variableSelection = 
		rxStepControl( method = "stepwise", scope = ~ DayOfWeek + 
			CRSDepTime + CRSElapsedTime + Distance + TaxiIn + TaxiOut, 
			stepCriterion = "SigLevel" ))
airlineStepModelSigLevel.10 <- rxLinMod(ArrDelay ~ DayOfWeek + CRSDepTime,
	data = trainingDataFile, variableSelection = 
		rxStepControl( method = "stepwise", scope = ~ DayOfWeek + 
			CRSDepTime + CRSElapsedTime + Distance + TaxiIn + TaxiOut, 
			stepCriterion = "SigLevel",
			maxSigLevelToAdd=.10, minSigLevelToDrop=.10))
  
  
#  Fixed-Effects Models
  
library(MASS)
Petrol <- petrol
Petrol[,2:5] <- scale(Petrol[,2:5], scale=F)
rxLinMod(Y ~ No + EP, data=Petrol,cube=TRUE)

#  Least Squares Dummy Variable (LSDV) Models
#   A Quick Review of Interacting Factors

set.seed(50)
income <- rep(c(1000,1500,2500,4000), each=5) + 100*rnorm(20)
race <- rep(c("Black","White"), each=10)
sex <- rep(c("Female", "Male"), each=5)
sex <- c(sex,sex)
myData <- data.frame(income, race, sex)
rxSummary(income~race:sex, data=myData)
rxCube(income~race:sex, data=myData)
summary(rxLinMod(income~race:sex, cube=TRUE, data=myData))
lm(income~race:sex, data=myData)
lm(income~race*sex, data = myData)	
rxLinMod(income~race*sex, data = myData, dropFirst = TRUE, dropMain = FALSE)
race <- rep(c("Black","Caucasian"), each=10)
sex <- rep(c("Woman", "Man"), each=5)
sex <- c(sex,sex)
myData1 <- data.frame(income, race, sex)
rxLinMod(income~race*sex, data=myData, cube=TRUE)
  
#   Using Dummy Variables in rxLinMod
  
censusWorkers <- file.path(rxGetOption("sampleDataDir"), "CensusWorkers.xdf")
rxLinMod(incwage~sex, data=censusWorkers, pweights="perwt", cube=TRUE)
linMod1 <- rxLinMod(incwage~age, data=censusWorkers, pweights="perwt")
summary(linMod1)
age <- c(20,65)
coefLinMod1 <- coef(linMod1)
incwage_Pred <- coefLinMod1[1] + age*coefLinMod1[2]
plotData1 <- data.frame(age, incwage_Pred)
rxLinePlot(incwage_Pred~age, data=plotData1)
linMod2 <- rxLinMod(incwage~sex+age, data = censusWorkers, pweights = "perwt", 
cube=TRUE)
summary(linMod2)
age <- c(20,65,20,65)
sex <- factor(rep(c(1, 2), each=2), labels=c("Male", "Female"))
perwt <- rep(1, times=4)
incwage <- rep(0, times=4)
plotData2 <- data.frame(age, sex, perwt, incwage)
plotData2p <- rxPredict(linMod2, data=plotData2, outData=plotData2)
rxLinePlot(incwage_Pred~age, groups=sex, data=plotData2p)
linMod3 <- rxLinMod(incwage~sex+sex:age, data = censusWorkers, 
	pweights = "perwt", cube=TRUE)
summary(linMod3)
plotData3p <- rxPredict(linMod3, data=plotData2, outData=plotData2)
rxLinePlot(incwage_Pred~age, groups=sex, data=plotData3p)
linMod4 <- rxLinMod(incwage~sex:F(age), data=censusWorkers, pweights="perwt", 
cube=TRUE)
plotData4 <- linMod4$countDF
# Convert the age factor variable back to an integer
plotData4$age <- as.integer(levels(plotData4$F.age.))[plotData4$F.age.]
rxLinePlot(incwage~age, groups=sex, data=plotData4)
  
#  Intercept-Only Models
  
airlineDF <- rxDataStep(inData = 
	file.path(rxGetOption("sampleDataDir"), "AirlineDemoSmall.xdf"))
lm(ArrDelay ~ 1, data = airlineDF)
rxSummary(~ ArrDelay, data = airlineDF)
  
######################################################## 
# Chapter 9: Fitting Logistic Regression Models
Ch9Start <- Sys.time()

  
library(rpart)
rxLogit(Kyphosis ~ Age + Start + Number, data = kyphosis)
glm(Kyphosis ~ Age + Start + Number, family = binomial, data = kyphosis)

initModel <- rxLogit(Kyphosis ~ Age, data=kyphosis)
initModel
KyphStepModel <-  rxLogit(Kyphosis ~ Age,
	data = kyphosis,
	variableSelection = rxStepControl(method="stepwise", 
		scope = ~ Age + Start + Number ))
KyphStepModel
  
#  Logistic Regression Prediction
  
trainingDataFileName <- "mortDefaultTraining"
mortCsv2009 <- paste(mortCsvDataName, "2009.csv", sep = "")
targetDataFileName <- "mortDefault2009.xdf"
ageLevels <- as.character(c(0:40))		
yearLevels <- as.character(c(2000:2009))	
colInfo <- list(list(name = "houseAge", type = "factor", 
    levels = ageLevels), list(name = "year", type = "factor", 
    levels = yearLevels))
append= FALSE
for (i in 2000:2008)
{
    importFile <- paste(mortCsvDataName, i, ".csv", sep = "")
    rxImport(inData = importFile, outFile = trainingDataFileName, 
	colInfo = colInfo, append = append)
    append = TRUE								
}


rxImport(inData = mortCsv2009, outFile = targetDataFileName, 
   colInfo = colInfo)
logitObj <- rxLogit(default ~ year + creditScore + yearsEmploy + ccDebt,
	data = trainingDataFileName, blocksPerRead = 2, verbose = 1, 
	reportProgress=2)
rxPredict(logitObj, data = targetDataFileName, 
	outData = targetDataFileName, computeResiduals = TRUE)
rxGetInfo(targetDataFileName, numRows = 30)

  
#  Prediction Standard Errors and Confidence Intervals
  
logitObj2 <- rxLogit(default ~ year + creditScore + yearsEmploy + ccDebt,
	data = trainingDataFileName, blocksPerRead = 2, verbose = 1, 
	reportProgress=2, covCoef=TRUE)
rxPredict(logitObj2, data = targetDataFileName, 
	outData = targetDataFileName, computeStdErr = TRUE, 
	interval = "confidence", overwrite=TRUE)
rxGetInfo(targetDataFileName, numRows=10)	
  
######################################################## 
# Chapter 10: Fitting Generalized Linear Models
  #  A Simple Example Using the Poisson Family
Ch10Start <- Sys.time()

  
if ("robust" %in% .packages()){	
data(breslow.dat, package = "robust")

rxGetInfo(breslow.dat, getVarInfo = TRUE)
rxHistogram( ~sumY, numBreaks = 25, data = breslow.dat) 

myGlm <- rxGlm(sumY~ Base + Age + Trt, dropFirst = TRUE, 
    data = breslow.dat, family = poisson())
summary(myGlm)

exp(coef(myGlm))
myGlm$deviance/myGlm$df[2]
myGlm1 <- rxGlm(sumY ~ Base + Age + Trt, dropFirst = TRUE, 
    data = breslow.dat, family = quasipoisson())

summary(myGlm1)
} # End of if for robust package

  
#  An Example Using the Gamma Family
  
claimsXdf <- file.path(rxGetOption("sampleDataDir"),"claims.xdf")

claimsGlm <- rxGlm(cost ~ age + car.age + type, family = Gamma,
 			    dropFirst = TRUE, data = claimsXdf)
summary(claimsGlm)

propinFile <- "CensusPropertyIns.xdf"

propinDS <- rxDataStep(inData = bigCensusData, outFile = propinFile,
    rowSelection =  (related == 'Head/Householder') & (age > 20) & (age < 90),
    varsToKeep = c("propinsr", "age", "sex", "region", "perwt"), 
    blocksPerRead = 10, overwrite = TRUE)
rxGetInfo(propinDS)
rxSummary(~region, data = propinDS)
regionLevels <- list( "New England" = "New England Division",
    "Middle Atlantic" = "Middle Atlantic Division",
    "East North Central" = "East North Central Div.",
    "West North Central" = "West North Central Div.",
    "South Atlantic" = "South Atlantic Division",
    "East South Central" = "East South Central Div.",
    "West South Central" = "West South Central Div.",
    "Mountain" ="Mountain Division", 
    "Pacific" ="Pacific Division") 

rxFactors(inData = propinDS, outFile = propinDS, 
    factorInfo = list(region = list(newLevels = regionLevels, 
	   otherLevel = "Other")),
    overwrite = TRUE)
rxHistogram(~propinsr, data = propinDS, pweights = "perwt")

propinGlm <- rxGlm(propinsr~sex + F(age) + region, 
    pweights = "perwt", data = propinDS, 
    family = rxTweedie(var.power = 1.5), dropFirst = TRUE)
summary(propinGlm)

# Get the region factor levels
varInfo <- rxGetVarInfo(propinDS)
regionLabels <- varInfo$region$levels

# Create a prediction data set for region 5, all ages, both sexes
region <- factor(rep(5, times=138), levels = 1:10, labels = regionLabels)
age <- c(21:89, 21:89)
sex <- factor(c(rep(1, times=69), rep(2, times=69)), 
	levels = 1:2, 
	labels = c("Male", "Female"))
predData <- data.frame(age, sex, region)

# Create a prediction data set for region 2, all ages, both sexes
predData2 <- predData  
predData2$region <-factor(rep(2, times=138), levels = 1:10, 
	labels = varInfo$region$levels)

# Combine data sets and compute predictions
predData <- rbind(predData, predData2)
outData <- rxPredict(propinGlm,  data = predData)

predData$predicted <- outData$propinsr_Pred
rxLinePlot( predicted ~age|region+sex, data = predData,
  title = "Predicted Annual Property Insurance Costs",
  xTitle = "Age of Head of Household",
  yTitle = "Predicted Costs")
claimsXdf <- file.path(rxGetOption("sampleDataDir"),"claims.xdf")
claimsGlm <- rxGlm(cost ~ age + car.age + type, family = Gamma,
 			    dropFirst = TRUE, data = claimsXdf)
summary(claimsGlm)
claimsGlmStep <- rxGlm(cost ~ age, family = Gamma, dropFirst=TRUE,
					data=claimsXdf, variableSelection =
					rxStepControl(scope = ~ age + car.age + type ))
summary(claimsGlmStep)
 
  
######################################################## 
# Chapter 11: Estimating Decision Tree Models
#  A Simple Classification Tree
Ch11Start <- Sys.time()

  
data("kyphosis", package="rpart")
kyphTree <- rxDTree(Kyphosis ~ Age + Start + Number, data = kyphosis, 
	cp=0.01)
kyphTree
  
#  A Simple Regression Tree
  
mtcarTree <- rxDTree(mpg ~ disp, data=mtcars)
mtcarTree
  
#  A Larger Regression Tree Model
  
censusWorkers <- file.path(rxGetOption("sampleDataDir"),
    "CensusWorkers.xdf")
rxGetInfo(censusWorkers, getVarInfo=TRUE)
incomeTree <- rxDTree(incwage ~ age + sex + wkswork1, pweights = "perwt", 
    maxDepth = 3, minBucket = 30000, data = censusWorkers)
incomeTree

  
#  Large Data Tree Models
  
airlineTree <- rxDTree(ArrDel15 ~ CRSDepTime + DayOfWeek, data = sampleAirData,
    blocksPerRead = 30, maxDepth = 5, cp = 1e-5)
airlineTree
airlineTree$cptable
airlineTree4 <- prune.rxDTree(airlineTree, cp=1e-4)
airlineTree4
airlineTree4 <- prune(airlineTree, cp=1e-4)
plotcp(rxAddInheritance(airlineTree)) 
airlineTreePruned <- prune.rxDTree(airlineTree, cp=2.5e-4)
airlineTreePruned
  
#  Prediction
  
if (bHasAdultData){

newNames <- c("age", "workclass", "fnlwgt", "education", 
	"education_num", "marital_status", "occupation", "relationship", 
	"race", "sex", "capital_gain", "capital_loss", "hours_per_week", 
	"native_country", "income")
adultTrain <- rxImport(adultDataFile, stringsAsFactors = TRUE)
names(adultTrain) <- newNames
adultTest <- rxImport(adultTestFile, rowsToSkip = 1, 
    stringsAsFactors=TRUE)
names(adultTest) <- newNames
adultTree <- rxDTree(income ~ age + sex + hours_per_week, pweights = "fnlwgt", 
    data = adultTrain)
adultPred <- rxPredict(adultTree, data = adultTest, type="vector")
sum(adultPred == as.integer(adultTest$income))/length(adultTest$income)
} # End of bHasAdultData
  
#  Plotting Trees
  
plot(rxAddInheritance(airlineTreePruned))
text(rxAddInheritance(airlineTreePruned))


######################################################## 
# Chapter 12: Estimating Decision Tree Models
#  A Simple Classification Tree
Ch12Start <- Sys.time()

##
## Omit Ch11 Examples for now since they take too long
##
# data("kyphosis", package="rpart")
# kyphForest <- rxDForest(Kyphosis ~ Age + Start + Number, seed = 10,
	# data = kyphosis, cp=0.01, nTree=500, mTry=3)
# kyphForest
# dfPreds <- rxPredict(kyphForest, data=kyphosis)
# sum(as.character(dfPreds[,1]) ==
    # as.character(kyphosis$Kyphosis))/81
# stackForest <- rxDForest(stack.loss ~ Air.Flow + Water.Temp + Acid.Conc.,
	# data=stackloss, nTree=200, mTry=2)
# stackForest
# censusWorkers <- file.path(rxGetOption("sampleDataDir"),
    # "CensusWorkers.xdf")
# rxGetInfo(censusWorkers, getVarInfo=TRUE)
# incForest <- rxDForest(incwage ~ age + sex + wkswork1, pweights = "perwt", 
    # maxDepth = 3, minBucket = 30000, mTry=2, nTree=200, data = censusWorkers)
# incForest

# airlineForest <- rxDForest(ArrDel15 ~ CRSDepTime + DayOfWeek, 
    # data = sampleAirData, blocksPerRead = 30, maxDepth = 5, 
    # nTree=20, mTry=2, method="class", seed = 8)
# airlineForest
# airlineForest$forest
# airlineForest2 <- rxDForest(ArrDel15 ~ CRSDepTime + DayOfWeek, 
    # data = sampleAirData, blocksPerRead = 30, maxDepth = 5, seed = 8,
    # nTree=20, mTry=2, method="class", parms=list(loss=c(0,4,1,0)))

Ch11Stop <- Sys.time()

  
######################################################## 
# Chapter 13: Estimating Correlation and Variance/Covariance Matrices
#  Computing a Correlation Matrix for Use in Factor Analysis
Ch13Start <- Sys.time()

  

rxSummary(~phone + speakeng + wkswork1 + incwelfr + incss + educrec + metro +
	ownershd + marst + lingisol + nfams + yrsusa1 + movedin + racwht + age,
 	data = bigCensusData, blocksPerRead = 5, pweights = "perwt", 
	rowSelection = age > 20)	
censusCor <- rxCor(formula=~poverty + noPhone + noEnglish  + onSocialSecurity + 
	onWelfare + working + incearn + noHighSchool + inCity + renter + 
	noSpouse + langIsolated + multFamilies + newArrival + recentMove + 
	white + sei + older, 
	data = bigCensusData, pweights= "perwt", blocksPerRead = 5, 
	rowSelection = age > 20,
	transforms= list(
		noPhone = phone == "No, no phone available",
		noEnglish = speakeng == "Does not speak English",
		working = wkswork1 > 20,
		onWelfare = incwelfr > 0,
		onSocialSecurity = incss > 0,
 		noHighSchool = 
			!(educrec %in% 
			c("Grade 12", "1 to 3 years of college", "4+ years of college")),
		inCity = metro == "In metro area, central city",
		renter = ownershd %in% c("No cash rent", "With cash rent"),
		noSpouse = marst != "Married, spouse present",
		langIsolated = lingisol == "Linguistically isolated",
		multFamilies = nfams > 2,
		newArrival = yrsusa2 == "0-5 years",
		recentMove = movedin == "This year or last year",
		white = racwht == "Yes",
		older = age > 64	
 		))
censusFa <- factanal(covmat = censusCor, factors=2)
print(censusFa, digits=2, cutoff = .2, sort= TRUE)
censusFa <- factanal(covmat = censusCor, factors=3)
print(censusFa, digits=2, cutoff = .2, sort= TRUE)

  
#  Computing A Covariance Matrix for Principal Components Analysis
  
irisLog <- as.data.frame(lapply(iris[,1:4], log))
irisSpecies <- iris[,5]
irisCov <- rxCov(~ Sepal.Length + Sepal.Width + Petal.Length + Petal.Width,
    data=irisLog)
irisPca <- princomp(covmat=irisCov, cor=TRUE)
summary(irisPca)
plot(irisPca)
loadings(irisPca)
irisCor <- rxCor(~ Sepal.Length + Sepal.Width + Petal.Length + Petal.Width, 
    data=irisLog)
irisPca2 <- princomp(covmat=irisCor)
summary(irisPca2)
loadings(irisPca2)
plot(irisPca2)
  
#  A Large Data Principal Components Analysis

if (bHasNYSE){  
nyseXdf <- "NYSE_daily_prices.xdf"
append <- "none"
for (i in LETTERS)
{
    importFile <- paste(nyseCsvFiles, i, ".csv", sep="")
    rxImport(importFile, nyseXdf, append=append)
    append <- "rows"
}
stockCor <- rxCor(~ stock_price_open + stock_price_high + 
	stock_price_low + stock_price_close + 
	stock_price_adj_close, data="NYSE_daily_prices.xdf")
stockPca <- princomp(covmat=stockCor)
summary(stockPca)
loadings(stockPca)
plot(stockPca)
} # End of bHasNYSE

 
  
######################################################## 
# Chapter 14: Clustering
#  K-means Clustering
Ch14Start <- Sys.time()
#   Clustering the Airline Data  
rxDataStep(inData = sampleAirData, outFile = "AirlineDataClusterVars.xdf",
  varsToKeep=c("DayOfWeek", "ArrDelay", "CRSDepTime", "DepDelay"))
kclusts1 <- rxKmeans(formula= ~ArrDelay + CRSDepTime, 
	data = "AirlineDataClusterVars.xdf",
	seed = 10,
	outFile = "airlineDataClusterVars.xdf", numClusters=5)
kclusts1
rxGetInfo("AirlineDataClusterVars.xdf", getVarInfo=TRUE)
  
#   Using the Cluster Membership Information
  
clust1Lm <- rxLinMod(ArrDelay ~ DayOfWeek, "AirlineDataClusterVars.xdf",
	rowSelection = .rxCluster == 1 )
clust5Lm <- rxLinMod(ArrDelay ~ DayOfWeek, "AirlineDataClusterVars.xdf", 
	rowSelection = .rxCluster == 5)
summary(clust1Lm)
summary(clust5Lm)
  
######################################################## 
# Chapter 15: Converting RevoScaleR Model Objects for
#  Use in PMML
########################################################
Ch15Start <- Sys.time()
# About 150 million observations
rxLinModObj <- rxLinMod(ArrDelay~Year + DayOfWeek, data = bigAirData, 
    blocksPerRead = 10)
  
######################################################## 
# Chapter 16: Transform Functions
#  Creating Variables
Ch16Start <- Sys.time()
  
ageTransform <- function(dataList)
{
    dataList$ageFactor <- cut(dataList$age, breaks=seq(from = 20, to = 70, 
                              by = 5), right = FALSE)
    return(dataList)
}
testData <- rxReadXdf(file = censusWorkers, startRow = 100, numRows = 10, 
returnDataFrame = FALSE, varsToKeep = c("age"))

as.data.frame(ageTransform(testData))
rxDataStep(inData = censusWorkers, outFile = "newCensusWorkers",
    varsToDrop = c("state"), transformFunc = ageTransform,
    transformVars=c("age"), overwrite=TRUE)	
rxGetInfo("newCensusWorkers", getVarInfo = TRUE)
  
#  Using Additional Objects or Data in a Transform Function
  
educExpense <- c(Connecticut=1795.57, Washington=1170.46, Indiana = 1289.66)
transformFunc <- function(dataList)
{
      # Match each individual’s state and add the variable for educ. exp.
	dataList$stateEducExpPC = educExp[match(dataList$state, names(educExp))]
	return(dataList)
}
censusWorkers <- file.path(rxGetOption("sampleDataDir"), "CensusWorkers.xdf")
linModObj <- rxLinMod(incwage~sex + age + stateEducExpPC, 
	data = censusWorkers, pweights = "perwt",
	transformFun = transformFunc, transformVars = "state", 
	transformObjects = list(educExp = educExpense))
summary(linModObj)
  
#  Creating a Row Selection Variable
  
createRandomSample <- function(data)
{
    data$.rxRowSelection <- as.logical(rbinom(length(data[[1]]), 1, .10))
	return(data)
}
censusWorkers <- file.path(rxGetOption("sampleDataDir"), "CensusWorkers.xdf")
df <- rxXdfToDataFrame(file = censusWorkers, transformFunc = createRandomSample, 
	transformVars = "age")
rxGetInfo(df)
head(df)
df <- rxDataStep(inData = censusWorkers, 
	rowSelection = as.logical(rbinom(.rxNumRows, 1, .10)) == TRUE)
  
#  An Example Computing Moving Averages
  
makeMoveAveTransFunc <- function(numDays = 10, varName="", newVarName="")
{
    function (dataList)
    {
        numRowsToRead <- 0
        varForMoveAve <- 0
        # If no variable is named, use the first one sent 
        # to the transformFunc
        if (is.null(varName) || nchar(varName) == 0)
        {
            varForMoveAve <- names(dataList)[1]
        } else {
            varForMoveAve <- varName
        }   
     
        # Get the number of rows in the current chunk
        numRowsInChunk <- length(dataList[[varForMoveAve]])
    
        # .rxStartRow is the starting row number of the
        # chunk of data currently being processed
	  # Read in previous data if we are not starting at row 1
	 if (.rxStartRow > 1)
	 {
	     # Compute the number of lagged rows we'd like
            numRowsToRead <- numDays - 1
	     # Check to see if enough data is available
	     if (numRowsToRead >= .rxStartRow)
	     {
	        numRowsToRead <- .rxStartRow - 1
	     }
            # Compute the starting row of previous data to read
	     startRow <- .rxStartRow - numRowsToRead
		# Read previous rows from the .xdf file
	     previousRowsDataList <- RevoScaleR::rxReadXdf(
                file=.rxReadFileName, 
		   varsToKeep=names(dataList), 
                startRow=startRow, numRows=numRowsToRead,
 		   returnDataFrame=FALSE)
            # Concatenate the previous rows with the existing rows 
		   dataList[[varForMoveAve]] <- 
                c(previousRowsDataList[[varForMoveAve]], 
                    dataList[[varForMoveAve]])
	 }
	 # Create variable for simple moving average
        # It will be added as the last variable in the data list
        newVarIdx <- length(dataList) + 1

        # Initialize with NA's
	  dataList[[newVarIdx]] <- rep(as.numeric(NA), times=numRowsInChunk)

	  for (i in (numRowsToRead+1):(numRowsInChunk + numRowsToRead))
	  {	
	      j <- i - numRowsToRead
	      lowIdx <- i - numDays + 1
	      if (lowIdx > 0 && ((lowIdx == 1) || 
                (j > 1 && (is.na(dataList[[newVarIdx]][j-1])))))
		{
                # If it's the first computation or the previous value 
                # is missing, take the mean of all the relevant lagged data
		     dataList[[newVarIdx]][j] <- 
                     mean(dataList[[varForMoveAve]][lowIdx:i])
		 } else if (lowIdx > 1)
		 {
                # Add and subtract from the last computation
			dataList[[newVarIdx]][j] <- dataList[[newVarIdx]][j-1] -
                        dataList[[varForMoveAve]][lowIdx-1]/numDays +
			    dataList[[varForMoveAve]][i]/numDays
		 }
         }
        # Remove the extra rows we read in from the original variable
	   dataList[[varForMoveAve]] <- 
            
           dataList[[varForMoveAve]][(numRowsToRead + 1):(numRowsToRead + 
               numRowsInChunk)]
        
        # Name the new variable
        if (is.null(newVarName) || (nchar(newVarName) == 0))
        {
            # Use a default name if no name specified
	       names(dataList)[newVarIdx] <- 
                  paste(varForMoveAve, "SMA", numDays, sep=".")
        } else {
            names(dataList)[newVarIdx] <- newVarName
        }
       return(dataList)
    }
}
DJIAdaily <- file.path(rxGetOption("sampleDataDir"), "DJIAdaily.xdf")
rxLinePlot(Adj.Close+Adj.Close.SMA.360~YearFrac, data=DJIAdaily,  
    transformFunc=makeMoveAveTransFunc(numDays=360),
    transformVars=c("Adj.Close"))
  
######################################################## 
# Chapter 17: Visualizing Huge Data Sets: An Example from the U.S. Census
#  Examining the Data
Ch17Start <- Sys.time()

  
rxGetInfo(bigCensusData)
ageSex <- rxCube(~F(age):sex, pweights = "perwt", data = bigCensusData, 
blocksPerRead = 15)
factoi <- function(x)
{
	as.integer(levels(x))[x]
}
getSexRatio <- function(ageSex)
{
    ageSexDF <- as.data.frame(ageSex)
    sexRatioDF <- subset(ageSexDF, sex == 'Male')
    names(sexRatioDF)[names(sexRatioDF) == 'Counts'] <- 'Males' 
    sexRatioDF$sex <- NULL
    females <- subset(ageSexDF, sex == 'Female')
    sexRatioDF$Females <- females$Counts
    sexRatioDF$age <- factoi(sexRatioDF$F_age)
    sexRatioDF <- subset(sexRatioDF, Females > 0 & Males > 0 & age <=90)
    sexRatioDF$SexRatio <- sexRatioDF$Males/sexRatioDF$Females
    return(sexRatioDF)
}
sexRatioDF <- getSexRatio(ageSex)
rxLinePlot(SexRatio~age, data = sexRatioDF, 
    xlab = "Age", ylab = "Sex Ratio", 
    main = "Figure 1: Sex Ratio by Age, U.S. 2000 5% Census")


ageSex <- rxCube(~F(age):sex:region, pweights = "perwt", data = bigCensusData, 
blocksPerRead = 15)
sexRatioDF <- getSexRatio(ageSex)
rxLinePlot(SexRatio~age|region, data = sexRatioDF, 
	xlab = "Age", ylab = "Sex Ratio", 
    	main = "Figure 2: Sex Ratio by Age and Region, U.S. 2000 5% Census")
ageSex <- rxCube(~F(age):sex:racwht, pweights = "perwt", data = bigCensusData,
	blocksPerRead = 15)
sexRatioDF <- getSexRatio(ageSex)
rxLinePlot(SexRatio~age, groups = racwht, data = sexRatioDF, 
	xlab = "Age", ylab = "Sex Ratio", 
main = "Figure 3: Sex Ratio by Age, Conditioned on 'Is Race White?', U.S. 2000 5% Census")
ageSex <- rxCube(~F(age):sex:married, pweights = "perwt", data = bigCensusData,
    transforms = list(married = factor(marst == 'Married, spouse present',
    	levels = c(FALSE, TRUE), labels = c("No Spouse", "Spouse Present"))),
    blocksPerRead = 15)
sexRatioDF <- getSexRatio(ageSex)
rxLinePlot(SexRatio~age, groups = married, data = sexRatioDF, 
	xlab="Age", ylab = "Sex Ratio", 
main="Figure 4: Sex Ratio by Age, Living/Not Living with Spouse, U.S. 2000 5% Census")

  
#  Extending the Analysis
  
spouseAgeTransform <- function(data)
{
	# Use internal variables
	censusUS2000 <- .rxReadFileName
   	startRow <- .rxStartRow
    
	# Calculate basic information about input data chunk
	numRows <- length(data$sploc)
	endRow <- startRow + numRows - 1

	# Create a new variable. A spouse is present if the spouse locator
 	# (relative position of spouse in data) is positive
	data$hasSpouse <- data$sploc > 0

	# Create variables for spouse information
	spouseVars <- c("age", "incwage", "sex")
	data$spouseAge <- rep.int(NA_integer_, numRows)
	data$spouseIncwage <- rep.int(NA_integer_, numRows)
 	data$sameSex <- rep.int(NA, numRows)

	# Create temporary row numbers for this block
	rowNum <- seq_len(numRows)
	# Find the temporary row number for the spouse
	spouseRow <- rep.int(NA_integer_, numRows)
	if (any(data$hasSpouse))
	{
        spouseRow[data$hasSpouse] <-
		    rowNum[data$hasSpouse] +
		    data$sploc[data$hasSpouse] - data$pernum[data$hasSpouse]
	}

	##################################################################
	# Handle possibility that spouse is in previous or next chunk
	# Create a variable indicating if the spouse is in the previous,
    	# current, or next chunk
	blockBreaks <- c(-.Machine$integer.max, 0, numRows, .Machine$integer.max)
	blockLabels <- c("previous", "current", "next")
	spouseFlag <- cut(spouseRow, breaks = blockBreaks, labels = blockLabels)
	blockCounts <- tabulate(spouseFlag, nbins = 3)
	names(blockCounts) <- blockLabels

	# At least one spouse in previous chunk
	if (blockCounts[["previous"]] > 0)
	{
		# Go back to the original data set and read the
		# required rows in the previous chunk
		needPreviousRows <- 1 - min(spouseRow, na.rm = TRUE)
		previousData <- rxReadXdf(censusUS2000, 
			startRow = startRow - needPreviousRows,
			numRows = needPreviousRows, varsToKeep = spouseVars, 
			returnDataFrame = FALSE, reportProgress = 0)

		# Get the spouse locations
		whichPrevious <- which(spouseFlag == "previous")
		spouseRowPrev <- spouseRow[whichPrevious] + needPreviousRows

		# Set the spouse information for everyone with a spouse
		# in the previous chunk
		data$spouseAge[whichPrevious] <- previousData$age[spouseRowPrev]
		data$spouseIncwage[whichPrevious] <- previousData$incwage[spouseRowPrev]
		data$sameSex[whichPrevious] <-
		    data$sex[whichPrevious] == previousData$sex[spouseRowPrev]
	}

	# At least one spouse in current chunk
	if (blockCounts[["current"]] > 0)
	{
		# Get the spouse locations
	    whichCurrent <- which(spouseFlag == "current")
		spouseRowCurr <- spouseRow[whichCurrent]

		# Set the spouse information for everyone with a spouse
		# in the current chunk
		data$spouseAge[whichCurrent] <- data$age[spouseRowCurr]
		data$spouseIncwage[whichCurrent] <- data$incwage[spouseRowCurr]
		data$sameSex[whichCurrent] <- 
            data$sex[whichCurrent] == data$sex[spouseRowCurr]
	}

	# At least one spouse in next chunk
	if (blockCounts[["next"]] > 0)
	{
		# Go back to the original data set and read the
		# required rows in the next chunk
		needNextRows <- max(spouseRow, na.rm=TRUE) - numRows
		nextData <- rxReadXdf(censusUS2000, startRow = endRow+1,
			numRows = needNextRows, varsToKeep = spouseVars,
			returnDataFrame = FALSE, reportProgress = 0)

		# Get the spouse locations
		whichNext <- which(spouseFlag == "next")
		spouseRowNext <- spouseRow[whichNext] - numRows

		# Set the spouse information for everyone with a spouse
		# in the next block
		data$spouseAge[whichNext] <- nextData$age[spouseRowNext]
		data$spouseIncwage[whichNext] <- nextData$incwage[spouseRowNext]
		data$sameSex[whichNext] <-
		    data$sex[whichNext] == nextData$sex[spouseRowNext]
	}

	# Now caculate age difference
	data$ageDiff <- data$age - data$spouseAge
	data
}
varsToKeep=c("age", "region", "incwage", "racwht", "nchild", "perwt", "sploc",
	"pernum", "sex")
testDF <- rxReadXdf(bigCensusData, numRows = 6, startRow=9, 
	varsToKeep = varsToKeep, returnDataFrame=FALSE)
.rxStartRow <- 9
.rxReadFileName <- bigCensusData
newTestDF <- as.data.frame(spouseAgeTransform(testDF))
.rxStartRow <- 8
testDF2 <- rxReadXdf(bigCensusData, numRows = 8, startRow=8, 
	varsToKeep = varsToKeep, returnDataFrame=FALSE)
newTestDF2 <- as.data.frame(spouseAgeTransform(testDF2))
newTestDF[,c("age", "incwage", "sploc", "hasSpouse" ,"spouseAge", "ageDiff")]
newTestDF2[,c("age", "incwage", "sploc", "hasSpouse" ,"spouseAge", "ageDiff")]

spouseCensusXdf <- "spouseCensus2000"
rxDataStep(inData = bigCensusData, outFile=spouseCensusXdf, 
	varsToKeep=c("age", "region", "incwage", "racwht", "nchild", "perwt"),
	transformFunc = spouseAgeTransform,
	transformVars = c("age", "incwage","sploc", "pernum", "sex"),
	rowSelection = sex == 'Male' & hasSpouse == 1 & sameSex == FALSE & 
		age <= 90, 
	blocksPerRead = 15, overwrite=TRUE)
ageDiffData <- rxCube(ageDiff~F(age) , pweights="perwt", data = spouseCensusXdf, 
	returnDataFrame = TRUE, blocksPerRead = 15)
ageDiffData$ownAge <- factoi(ageDiffData$F_age)
rxLinePlot(ageDiff~ownAge,  data = ageDiffData,
	xlab="Age of Husband", ylab = "Age Difference (Husband-Wife)", 
main="Figure 5: Age Difference of Spouses Living Together, U.S. 2000 5% Census")

aa <- rxCube(~F(age):F(spouseAge), pweights = "perwt", data = spouseCensusXdf, 
	returnDataFrame = TRUE, blocksPerRead = 7)
# Convert factors to integers
aa$age <- factoi(aa$F_age)
aa$spouseAge <- factoi(aa$F_spouseAge)

# Do a level plot showing the counts for husbands aged 40 to 60
ageCompareSubset <- subset(aa, age >= 40 & age <= 60 & spouseAge >= 30 & spouseAge <= 65)
levelplot(Counts~age*spouseAge, data=ageCompareSubset,
	xlab="Age of Husband", ylab = "Age of Wife",
main="Figure 6: Counts by Age (40-60) and Spouse Age, U.S. 2000 5% Census")
ageCompareSubset <- subset(aa, age >= 60 & age <= 80 & spouseAge >= 50 & spouseAge <= 75)
levelplot(Counts~age*spouseAge, data = ageCompareSubset,
	xlab = "Age of Husband", ylab = "Age of Wife",
	main ="Figure 7: Counts by Age(60-80)and Spouse Age , U.S. 2000 5% Census")

ageCompareSubset <- subset(aa, age > 61 & age < 68 & spouseAge > 50 & 
	spouseAge < 85)
rxLinePlot(Counts~spouseAge, groups = age, data = ageCompareSubset,
	xlab = "Age of Wife", ylab = "Counts",  
	lineColor = c("Blue4", "Blue2", "Blue1", "Red2", "Red3", "Red4"),
main = "Figure 8: Ages of Wives (> 45) for Husband's Ages 62 to 67, U.S. 2000 5% Census")

Ch17End <- Sys.time()

print(Ch2Start - Ch1Start)
print(Ch3Start - Ch2Start)
print(Ch4Start - Ch3Start)
print(Ch6Start - Ch4Start)
print(Ch7Start - Ch6Start)
print(Ch8Start - Ch7Start)
print(Ch9Start - Ch8Start)
print(Ch10Start - Ch9Start)
print(Ch11Start - Ch10Start)
print(Ch12Start - Ch11Start)
print(Ch13Start - Ch12Start)
print(Ch14Start - Ch13Start)
print(Ch15Start - Ch14Start)
print(Ch16Start - Ch15Start)
print(Ch17Start - Ch16Start)
print(Ch17End - Ch17Start)
print(Ch17End - Ch1Start)

 

