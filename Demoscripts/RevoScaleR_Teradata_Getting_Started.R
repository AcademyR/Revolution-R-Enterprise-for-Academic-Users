##############################################################################
# RevoScaleR RxTeradata Getting Started Guide Examples
##############################################################################

##############################################################################
# General setup/cleanup: Run this code before proceeding
##############################################################################

# To follow the examples in this guide, you will need the following 
# comma-delimited text data files  loaded into your Teradata database: 
#  ccFraud.csv as RevoTestDB.ccFraud10
#  ccFraudScore.csv as RevoTestDB.ccFraudScore10
# These data files are available from Revolution FTP site. 
# You can use the 'fastload' Teradata command to load the data sets into your 
# data base. 

# Modify and uncomment the following code for your connection to Teradata,
# and other information about your Teradata setup:

# tdConnString <- "DRIVER=Teradata;DBCNAME=machineNameOrIP
# DATABASE=RevoTestDB;UID=myUserID;PWD=myPassword;"

#tdShareDir = paste("c:\\AllShare\\", Sys.getenv("USERNAME"), sep="")
#tdRemoteShareDir = "/tmp/revoJobs"
#tdRevoPath = "/usr/lib64/Revo-7.1/R-3.0.2/lib64/R"

# Make sure that the local share dir exists:
if (!file.exists(tdShareDir)) dir.create(tdShareDir, recursive = TRUE)

# If your DATABASE is not named RevoTestDB, you will need
# to modify the SQL queries in the following code to your
# DATABASE name



##########################################################
# Using a Teradata Data Source and Compute Context
##########################################################  
 
  
# Creating an RxTeradata Data Source
  
tdQuery <- "SELECT * FROM RevoTestDB.ccFraud10"	
teradataDS <- RxTeradata(connectionString = tdConnString, 
sqlQuery = tdQuery, rowsPerRead = 50000)
  
# Extracting basic information about your data
  
rxGetVarInfo(data = teradataDS)
  
# Specifying column information in your data source
  
stateAbb <- c("AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC",
    "DE", "FL", "GA", "HI","IA", "ID", "IL", "IN", "KS", "KY", "LA",
    "MA", "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NB", "NC", "ND",
    "NH", "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI","SC",
    "SD", "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY")


ccColInfo <- list(		
    gender = list(
	  type = "factor", 
        levels = c("1", "2"),
   	  newLevels = c("Male", "Female")),		
    cardholder = list(
	    type = "factor", 
	    levels = c("1", "2"),	
          newLevels = c("Principal", "Secondary")),
	state = list(
          type = "factor", 
          levels = as.character(1:51),
	    newLevels = stateAbb)
	)
teradataDS <- RxTeradata(connectionString = tdConnString, 
    sqlQuery = tdQuery, colInfo = ccColInfo, rowsPerRead = 50000)

rxGetVarInfo(data = teradataDS)

tdWait <- TRUE
tdConsoleOutput <- FALSE
tdCompute <- RxInTeradata(
    connectionString = tdConnString, 
    shareDir = tdShareDir,
    remoteShareDir = tdRemoteShareDir,
    revoPath = tdRevoPath,
    wait = tdWait,
    consoleOutput = tdConsoleOutput)
rxGetNodeInfo(tdCompute)

##########################################################
# High-Performance In-Database Analtycis in Teradata
##########################################################  
  
# Compute summary statistics in Teradata
  
# Set the compute context to compute in Teradata
rxSetComputeContext(tdCompute)
rxSummary(formula = ~gender + balance + numTrans + numIntlTrans +
creditLine, data = teradataDS)
# Set the compute context to compute locally
rxSetComputeContext ("local") 
  
# Refining the RxTeradata data source
  
ccColInfo <- list(		
    gender = list(
	  type = "factor", 
        levels = c("1", "2"),
   	  newLevels = c("Male", "Female")),		
    cardholder = list(
	    type = "factor", 
	    levels = c("1", "2"),	
          newLevels = c("Principal", "Secondary")),
	state = list(
          type = "factor", 
          levels = as.character(1:51),
	    newLevels = stateAbb),
      balance = list(low = 0, high = 41485),
      numTrans = list(low = 0, high = 100),
      numIntlTrans = list(low = 0, high = 60),
      creditLine = list(low = 1, high = 75)
	)
teradataDS <- RxTeradata(connectionString = tdConnString, 
    sqlQuery = tdQuery, colInfo = ccColInfo, rowsPerRead = 50000)
  
# Visualizing your data using rxHistogram and rxCube
  
rxSetComputeContext(tdCompute)

rxHistogram(~creditLine|gender, data = teradataDS, 
histType = "Percent")
cube1 <- rxCube(fraudRisk~F(numTrans):F(numIntlTrans), 
data = teradataDS)
cubePlot <- rxResultsDF(cube1)
levelplot(fraudRisk~numTrans*numIntlTrans, data = cubePlot)
  
# Analyzing your data with rxLinMod
  
linModObj <- rxLinMod(balance ~ gender + creditLine, 
data = teradataDS)

summary(linModObj)

  
# Analyzing your data with rxLogit
  
logitObj <- rxLogit(fraudRisk ~ state + gender + cardholder + balance + 
    numTrans + numIntlTrans + creditLine, data = teradataDS, 
    dropFirst = TRUE)
summary(logitObj)
  
# Scoring a Data Set
  
tdQuery <- "SELECT * FROM RevoTestDB.ccFraudScore10"
teradataScoreDS <- RxTeradata(connectionString = tdConnString, 
    sqlQuery = tdQuery, colInfo = ccColInfo, rowsPerRead = 50000)


teradataOutDS <- RxTeradata(table = "RevoTestDB.ccScoreOutput", 
    connectionString = tdConnString, rowsPerRead = 50000 )
   
rxSetComputeContext(tdCompute)
if (rxTeradataTableExists("RevoTestDB.ccScoreOutput"))
    rxTeradataDropTable("RevoTestDB.ccScoreOutput")
   
rxPredict(modelObject = logitObj, 
	data = teradataScoreDS,
	outData = teradataOutDS,
	predVarNames = "ccFraudLogitScore",
	type = "link",
	writeModelVars = TRUE,
	overwrite = TRUE)
tdMinMax <- RxOdbcData(sqlQuery = paste(
      "SELECT MIN(ccFraudLogitScore),",
 	"MAX(ccFraudLogitScore) FROM RevoTestDB.ccScoreOutput"),
	connectionString = tdConnString)
minMaxVals <- rxImport(tdMinMax)
minMaxVals <- as.vector(unlist(minMaxVals))

teradataScoreDS <- RxTeradata(sqlQuery = 
	"Select ccFraudLogitScore FROM RevoTestDB.ccScoreOutput", 
      connectionString = tdConnString, rowsPerRead = 50000,
 	colInfo = list(ccFraudLogitScore = list(
            low = floor(minMaxVals[1]), 
		high = ceiling(minMaxVals[2]))))

rxSetComputeContext(tdCompute)
rxHistogram(~ccFraudLogitScore, data = teradataScoreDS)		
  
##########################################################
# Using rxDataStep and rxImport
##########################################################  
  
teradataScoreDS <- RxTeradata(
    sqlQuery =  "Select * FROM RevoTestDB.ccScoreOutput", 
    connectionString = tdConnString, rowsPerRead = 50000 )

teradataOutDS2 <- RxTeradata(table = "RevoTestDB.ccScoreOutput2",
	connectionString = tdConnString, rowsPerRead = 50000)

rxSetComputeContext(tdCompute)
if (rxTeradataTableExists("RevoTestDB.ccScoreOutput2"))
    rxTeradataDropTable("RevoTestDB.ccScoreOutput2")

rxDataStep(inData = teradataScoreDS, outFile = teradataOutDS2, 
	transforms = list(ccFraudProb = inv.logit(ccFraudLogitScore)), 
	transformPackages = "boot", overwrite = TRUE)
rxGetVarInfo(teradataOutDS2)

 
# Using rxImport to Extract a Subsample
 
rxSetComputeContext("local")

teradataProbDS <- RxTeradata(
	sqlQuery = paste(
	"Select * FROM RevoTestDB.ccScoreOutput2",
 	"WHERE (ccFraudProb > .99)"),
	connectionString = tdConnString)
highRisk <- rxImport(teradataProbDS)
orderedHighRisk <- highRisk[order(-highRisk$ccFraudProb),]
row.names(orderedHighRisk) <- NULL  # Reset row numbers
head(orderedHighRisk)
  
# Using rxDataStep to Create a Teradata Table

rxSetComputeContext("local")
xdfAirDemo <- RxXdfData(file.path(rxGetOption("sampleDataDir"),
    "AirlineDemoSmall.xdf"))
rxGetVarInfo(xdfAirDemo)

teradataAirDemo <- RxTeradata(table = "RevoTestDB.AirDemoSmallTest",
	connectionString = tdConnString)
if (rxTeradataTableExists("RevoTestDB.AirDemoSmallTest",
    connectionString = tdConnString))
    rxTeradataDropTable("RevoTestDB.AirDemoSmallTest", 
    connectionString = tdConnString)

rxDataStep(inData = xdfAirDemo, outFile = teradataAirDemo, 
	transforms = list(
		DayOfWeek = as.integer(DayOfWeek),
		rowNum = .rxStartRow : (.rxStartRow + .rxNumRows - 1)
	)
)

rxSetComputeContext(tdCompute)
teradataAirDemo <- RxTeradata(sqlQuery = 
	"SELECT * FROM RevoTestDB.AirDemoSmallTest",
	connectionString = tdConnString,
      rowsPerRead = 50000,
	colInfo = list(DayOfWeek = list(type = "factor", 
		levels = as.character(1:7))))

rxSummary(~., data = teradataAirDemo)
  
# Performing Your Own Chunking Analysis
  

ProcessChunk <- function( dataList)
{	
    # Convert the input list to a data frame and 
    # call the 'table' function to compute the
    # contingency table 
	chunkTable <- table(as.data.frame(dataList))

	# Convert table output to data frame with single row
	varNames <- names(chunkTable)
	varValues <- as.vector(chunkTable)
	dim(varValues) <- c(1, length(varNames))
	chunkDF <- as.data.frame(varValues)
	names(chunkDF) <- varNames

	# Return the data frame, which has a single row
	return( chunkDF )
}
rxSetComputeContext( tdCompute )
tdQuery <- 
"select DayOfWeek from RevoTestDB.AirDemoSmallTest"
inDataSource <- RxTeradata(sqlQuery = tdQuery, 
 	rowsPerRead = 50000,
	colInfo = list(DayOfWeek = list(type = "factor", 
		levels = as.character(1:7))))
iroDataSource = RxTeradata(table = "RevoTestDB.iroResults", 
	connectionString = tdConnString)
rxDataStep( inData = inDataSource, outFile = iroDataSource,
        	transformFunc = ProcessChunk,
		overwrite = TRUE)

iroResults <- rxImport(iroDataSource)
iroResults
finalResults <- colSums(iroResults)
finalResults
##########################################################
# Performing Simulations In-Database
##########################################################  
 
playCraps <- function()
{
	result <- NULL
	point <- NULL
	count <- 1
	while (is.null(result))
	{
		roll <- sum(sample(6, 2, replace=TRUE))
		
		if (is.null(point))
		{
			point <- roll
		}
		if (count == 1 && (roll == 7 || roll == 11))
		{ 
			result <- "Win"
		}
 		else if (count == 1 && 
               (roll == 2 || roll == 3 || roll == 12)) 
		{
			result <- "Loss"
		} 
		else if (count > 1 && roll == 7 )
		{
			result <- "Loss"
		} 
		else if (count > 1 && point == roll)
		{
			result <- "Win"
		} 
		else
		{
			count <- count + 1
		}
	}
	result
}

   
# Calling rxExec
   	  
teradataExec <- rxExec(playCraps, timesToRun=1000, RNGseed="auto")
length(teradataExec)

table(unlist(teradataExec))	
##########################################################
# Analyzing Your Data Alongside Teradata
##########################################################  
# Set the compute context to compute locally
rxSetComputeContext ("local") 
tdQuery <- "SELECT * FROM RevoTestDB.ccFraudScore10"  
  
# Perform an rxSummary using a local compute context
  
teradataDS1 <- RxTeradata(connectionString = tdConnString, 
    sqlQuery = tdQuery, colInfo = ccColInfo, rowsPerRead = 500000)

rxSummary(formula = ~gender + balance + numTrans + numIntlTrans +
creditLine, data = teradataDS1)
statesToKeep <- sapply(c("CA", "OR", "WA"), grep, stateAbb)
statesToKeep
  
# Fast import of iata from a Teradata Database
  
importQuery <- paste("SELECT gender,cardholder,balance,state",
 	"FROM RevoTestDB.ccFraud10",
 	"WHERE (state = 5 OR state = 38 OR state = 48)")
importColInfo <- list(		
    gender = list(
	  type = "factor", 
        levels = c("1", "2"),
   	  newLevels = c("Male", "Female")),		
    cardholder = list(
	    type = "factor", 
	    levels = c("1", "2"),	
          newLevels = c("Principal", "Secondary")),
	state = list(
          type = "factor", 
          levels = as.character(statesToKeep),
	      newLevels = names(statesToKeep))
	)

rxSetComputeContext("local")
teradataImportDS <- RxTeradata(connectionString = tdConnString, 
    	sqlQuery = importQuery, colInfo = importColInfo)

localDS <- rxImport(inData = teradataImportDS,
    	outFile = "ccFraudSub.xdf", 
    	overwrite = TRUE)
  
# Using the imported data
  
rxGetVarInfo(data = localDS)
rxSummary(~gender + cardholder + balance + state, data = localDS)	

