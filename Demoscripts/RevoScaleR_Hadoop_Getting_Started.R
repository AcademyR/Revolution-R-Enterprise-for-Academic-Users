##########################################################
# Code Contained in
# A Tutorial Introduction to RevoScaleR in Hadoop
##########################################################
# 
# Before running, set the following variables to match
# your setup
myNameNode <- "master.local"
myPort <- 8020
mySshUsername <- Sys.info()["login"] 
bigDataDirRoot <- "/share"  # Location of the data
#
# If you want to use a client, specify this information
# and use the RxHadoopMr code here instead of the 
# code in 'Creating a Compute Context'
# mySshUsername <- "alpha1"
# mySshHostname <- "12.213.333.45"
# mySshSwitches <- "-i /home/yourName/alpha1.pem"

 #myHadoopCluster <- RxHadoopMR(
    #sshUsername = mySshUsername, 
    #sshHostname = mySshHostname, 
    #sshSwitches = mySshSwitches)

##########################################################
# 5 A Tutorial Introduction to RevoScaleR in Hadoop
##########################################################

#
# 5.2 Specifying Information about Your Hadoop Cluster 
#

#
# 5.3 Creating a Compute Context
#
myHadoopCluster <- RxHadoopMR(consoleOutput=TRUE)
myShareDir = paste( "/var/RevoShare", Sys.info()[["user"]], 
sep="/" )
myHdfsShareDir = paste( "/user/RevoShare", Sys.info()[["user"]], 
sep="/" )
rxSetComputeContext(myHadoopCluster)

# 
# 5.4 Copying a Data File into the HDFS
#
source <-system.file("SampleData/AirlineDemoSmall.csv", 
    package="RevoScaleR")
inputDir <- file.path(bigDataDirRoot,"AirlineDemoSmall")
rxHadoopMakeDir(inputDir)
rxHadoopCopyFromLocal(source, inputDir)
rxHadoopListFiles(inputDir)

# 
# 5.5 Creating a Data Source
#
hdfsFS <- RxHdfsFileSystem(hostName=myNameNode, port=myPort)
colInfo <- list(DayOfWeek = list(type = "factor",
    levels = c("Monday", "Tuesday", "Wednesday", "Thursday", 
    "Friday", "Saturday", "Sunday")))

airDS <- RxTextData(file = inputDir, missingValueString = "M", 
    colInfo  = colInfo, fileSystem = hdfsFS)
  
#
# 5.6 Summarizing Your Data
#
adsSummary <- rxSummary(~ArrDelay+CRSDepTime+DayOfWeek, 
    data = airDS)
adsSummary
#
# Computing summary information by category
#
rxSummary(~ArrDelay:DayOfWeek, data = airDS)

#
# 5.7 Using a Local Compute Context
#
rxSetComputeContext("local")
inputFile <-file.path(bigDataDirRoot,"AirlineDemoSmall/AirlineDemoSmall.csv")
airDSLocal <- RxTextData(file = inputFile, 
    missingValueString = "M", 
    colInfo  = colInfo, fileSystem = hdfsFS)

adsSummary <- rxSummary(~ArrDelay+CRSDepTime+DayOfWeek, 
data = airDSLocal)
adsSummary

rxSetComputeContext(myHadoopCluster)

#
# 5.8 Fitting a simple linear model
#
arrDelayLm1 <- rxLinMod(ArrDelay ~ DayOfWeek, data = airDS)
summary(arrDelayLm1)

#
# 5.9 Creating a Non-Waiting Compute Context
#
myHadoopNoWaitCluster <- RxHadoopMR(myHadoopCluster, wait = FALSE)
rxSetComputeContext(myHadoopNoWaitCluster)

job1 <- rxLinMod(ArrDelay ~ DayOfWeek, data = airDS)
rxGetJobStatus(job1)

arrDelayLm1 <- rxGetJobResults(job1) 
summary(arrDelayLm1)

job2 <- rxLinMod(ArrDelay ~ DayOfWeek, data = airDS)
rxCancelJob(job2)

rxSetComputeContext(myHadoopCluster)

#####################################################
# 
# 6 Analyzing a Large Data Set
#
#####################################################
#
# 6.1 Setup to Use Your CSV Files
#
airDataDir <- file.path(bigDataDirRoot,"/airOnTime12/CSV")
rxHadoopMakeDir(airDataDir)
rxHadoopCopyFromLocal("/tmp/airOT2012*.csv", airDataDir)
airlineColInfo <- list(
     MONTH = list(newName = "Month", type = "integer"),
    DAY_OF_WEEK = list(newName = "DayOfWeek", type = "factor", 
        levels = as.character(1:7),
        newLevels = c("Mon", "Tues", "Wed", "Thur", "Fri", "Sat",
                      "Sun")), 
    UNIQUE_CARRIER = list(newName = "UniqueCarrier", type = 
                            "factor"),
    ORIGIN = list(newName = "Origin", type = "factor"),
    DEST = list(newName = "Dest", type = "factor"),
    CRS_DEP_TIME = list(newName = "CRSDepTime", type = "integer"),
    DEP_TIME = list(newName = "DepTime", type = "integer"),
    DEP_DELAY = list(newName = "DepDelay", type = "integer"),
    DEP_DELAY_NEW = list(newName = "DepDelayMinutes", type = 
                         "integer"),
    DEP_DEL15 = list(newName = "DepDel15", type = "logical"),
    DEP_DELAY_GROUP = list(newName = "DepDelayGroups", type = 
                           "factor",
       levels = as.character(-2:12),
       newLevels = c("< -15", "-15 to -1","0 to 14", "15 to 29", 
                     "30 to 44", "45 to 59", "60 to 74",
                     "75 to 89", "90 to 104", "105 to 119",
                     "120 to 134", "135 to 149", "150 to 164", 
                     "165 to 179", ">= 180")),
    ARR_DELAY = list(newName = "ArrDelay", type = "integer"),
    ARR_DELAY_NEW = list(newName = "ArrDelayMinutes", type = 
                         "integer"),  
    ARR_DEL15 = list(newName = "ArrDel15", type = "logical"),
    AIR_TIME = list(newName = "AirTime", type =  "integer"),
    DISTANCE = list(newName = "Distance", type = "integer"),
    DISTANCE_GROUP = list(newName = "DistanceGroup", type = 
                         "factor",
     levels = as.character(1:11),
     newLevels = c("< 250", "250-499", "500-749", "750-999",
         "1000-1249", "1250-1499", "1500-1749", "1750-1999",
         "2000-2249", "2250-2499", ">= 2500")))

varNames <- names(airlineColInfo)
hdfsFS <- RxHdfsFileSystem(hostName=myNameNode, port=myPort)
bigAirDS <- RxTextData( airDataDir, 
                        colInfo = airlineColInfo,
                        varsToKeep = varNames,
                        fileSystem = hdfsFS )    

#
# 6.2 Estimating a Linear Model with a Big Data Set
#
system.time(
     delayArr <- rxLinMod(ArrDelay ~ DayOfWeek, data = bigAirDS, 
          cube = TRUE)
)
print(
     summary(delayArr)
)

#
# 6.3 Importing Data as Composite XDF Files
#
bigAirXdfName <- "/user/RevoShare/v7alpha/AirlineOnTime2012"
airData <- RxXdfData( bigAirXdfName,
                        fileSystem = hdfsFS )
blockSize <- 250000
numRowsToRead = -1
rxImport(inData = bigAirDS,
         outFile = airData,
         rowsPerRead = blockSize,
         overwrite = TRUE,
         numRows = numRowsToRead )

#
# 6.4 Estimating a Linear Model Using Composite XDF Files
#
system.time(
     delayArr <- rxLinMod(ArrDelay ~ DayOfWeek, data = airData, 
          cube = TRUE)
)
print(
     summary(delayArr)
)

#
# 6.5 Handling larger linear models
#
delayCarrier <- rxLinMod(ArrDelay ~ UniqueCarrier, 
     data = airData, cube = TRUE)
	 
dcCoef <- sort(coef(delayCarrier))
dcCoef

print(
sprintf("Frontier's additional delay compared with Alaska: %f",
     dcCoef["UniqueCarrier=F9"]-dcCoef["UniqueCarrier=AS"])
)

logitObj <- rxLogit(ArrDel15 ~ DayOfWeek, data = airData)
logitObj

#
# 6.7 Prediction on Large Data
#
rxPredict(modelObject=logitObj, data=airData,
          computeResiduals=TRUE, overwrite=TRUE)

#
# 6.8 Performing Data Operations on Large Data
#
newAirDir <- "/user/RevoShare/v7alpha/newAirData"
newAirXdf <- RxXdfData(newAirDir,fileSystem=hdfsFS)

rxDataStep(inData = airData, outFile = newAirXdf,
           varsToDrop=c("ArrDel15_Pred","ArrDel15_Resid"),
           rowSelection = !is.na(ArrDelay) & (DepDelay > -60))


