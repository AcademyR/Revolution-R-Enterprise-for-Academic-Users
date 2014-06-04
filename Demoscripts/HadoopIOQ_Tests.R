
############################################################################################
# Copyright (C) 2013 Revolution Analytics, Inc
############################################################################################
#
# Run tests using both RxHadoopMR and local compute contexts
#
############################################################################################

# HadoopIOQ_Setup.R creates the hadoopTestInfoList required by these functions

#############################################################################################
# Function to get compute contexts
#############################################################################################
"getHadoopComputeContext" <- function()
{
    RxHadoopMR(
        nameNode     = hadoopTestInfoList$myNameNode,
        hdfsShareDir = hadoopTestInfoList$myHdfsShareDir,
        shareDir     = hadoopTestInfoList$myShareDir,
        sshUsername  = hadoopTestInfoList$mySshUsername, 
        sshHostname  = hadoopTestInfoList$mySshHostname, 
        sshSwitches  = hadoopTestInfoList$mySshSwitches,
        wait = TRUE,
        consoleOutput = hadoopTestInfoList$myConsoleOutput,
	    fileSystem = hadoopTestInfoList$myHdfsFS,
        sshProfileScript = hadoopTestInfoList$mySshProfileScript)
    
}

"getLocalComputeContext" <- function()
{
    RxLocalSeq()
}

#############################################################################################
# Functions to get data sources
#############################################################################################
"getAirDemoTextHdfsDS" <- function()
{
    airColInfo <- list(DayOfWeek = list(type = "factor", 
        levels = c("Monday", "Tuesday", "Wednesday", "Thursday", 
            "Friday", "Saturday", "Sunday")))
    # In .setup, a directory 'AirlineDemoSmallXdf' will be created with the file
    RxTextData(file = file.path(hadoopTestInfoList$myHdfsShareDir, 
        hadoopTestInfoList$myHdfsAirDemoCsvSubdir), 
        missingValueString = "M", 
        colInfo = airColInfo, 
        fileSystem = hadoopTestInfoList$myHdfsFS)         
}

"getAirDemoXdfHdfsDS" <- function()
{
    # In the first test a directory 'IOQ\AirlineDemoSmallXdf' will be created with a composite xdf file
    RxXdfData( file = file.path(hadoopTestInfoList$myHdfsShareDir,
        hadoopTestInfoList$myHdfsAirDemoXdfSubdir),
        fileSystem = hadoopTestInfoList$myHdfsFS )
}

"getTestOutFileHdfsDS" <- function( testFileDirName )
{
    RxXdfData( file = file.path(hadoopTestInfoList$myHdfsShareDir, 
        hadoopTestInfoList$myHdfsTestOuputSubdir, testFileDirName),
        fileSystem = hadoopTestInfoList$myHdfsFS )
}

"removeTestOutFileHdfsDS" <- function( testDS )
{
    rxSetComputeContext(getHadoopComputeContext())
    rxHadoopRemoveDir(testDS@file)
    rxSetComputeContext(getLocalComputeContext()) 

}

"getAirDemoTextLocalDS" <- function()
{
    airColInfo <- list(DayOfWeek = list(type = "factor", 
        levels = c("Monday", "Tuesday", "Wednesday", "Thursday", 
            "Friday", "Saturday", "Sunday")))
    RxTextData(file = file.path(rxGetOption("sampleDataDir"), "AirlineDemoSmall.csv"),
        colInfo = airColInfo,
        missingValueString = "M", 
        fileSystem = "native")    
}

"getAirDemoXdfLocalDS" <- function()
{
    RxXdfData(file = file.path(rxGetOption("sampleDataDir"), "AirlineDemoSmall.xdf"),
              fileSystem = "native")
}

"getTestOutFileLocalDS" <- function( testFileDirName )
{
    RxXdfData( file = file.path(hadoopTestInfoList$localTestDataDir, 
        testFileDirName),
        createCompositeSet = TRUE,
        fileSystem = "native")
}

"removeTestOutFileLocalDS" <- function( testDS )
{
    unlink(testDS@file, recursive = TRUE)
}

#############################################################################################
# Tests to put data on Hadoop cluster
#############################################################################################

"test.hadoop.aaa.initializeHadoop" <- function()
{
    rxSetComputeContext(getHadoopComputeContext())
    # Get the curent version from the Hadoop cluster
    "getRevoVersion" <- function() 
    { 
        Revo.version
    } 

    versionOutput <- rxExec(getRevoVersion)
    print(paste("RRE version on Hadoop cluster:", versionOutput$rxElem1$version.string))

    output1 <- capture.output(
        rxHadoopListFiles(hadoopTestInfoList$myHdfsShareDirRoot) 
        )
    hasUserDir <- grepl(hadoopTestInfoList$myHdfsShareDir, output1)
    print("Hadoop Compute Context being used:")
    print(getHadoopComputeContext())
    
    if (sum(hasUserDir) == 0)
    {
        print("WARNING: myHdfsSharDir does not exist or problem with compute context.")
    }
            
    if (hadoopTestInfoList$createHadoopData )
    {  
        # Make directory for writing temporary test data
        testOutputDir <- file.path(hadoopTestInfoList$myHdfsShareDir,
            hadoopTestInfoList$myHdfsTestOuputSubdir)
        rxHadoopMakeDir(testOutputDir)
        
        # Copy the AirlineDemoSmall.csv file from local machine to HDFS
        #    File is automatically installed locally with RRE
        airDemoSmallLocal <- file.path(rxGetOption("sampleDataDir"), "AirlineDemoSmall.csv")  
        
        # Destination path for file in Hadoop
        airDemoSmallCsvHdfsPath <- file.path(hadoopTestInfoList$myHdfsShareDir, 
            hadoopTestInfoList$myHdfsAirDemoCsvSubdir)  
               
        # Remove the directory if it's already there 
        #rxHadoopListFiles(airDemoSmallCsvHdfsPath) 
        print("Trying to remove directories in case they already exist.")     
        rxHadoopRemoveDir(airDemoSmallCsvHdfsPath)  
        # Create the directory               
        rxHadoopMakeDir(airDemoSmallCsvHdfsPath)
        
        # Copy the file from local client
        rxHadoopCopyFromClient(source = airDemoSmallLocal, 
            nativeTarget = hadoopTestInfoList$myHdfsNativeDir,
            hdfsDest = airDemoSmallCsvHdfsPath, 
            computeContext = getHadoopComputeContext(), 
            sshUsername = hadoopTestInfoList$mySshUsername, 
            sshHostname = hadoopTestInfoList$mySshHostname, 
            sshSwitches = hadoopTestInfoList$mySshSwitches)
        #rxHadoopListFiles(airDemoSmallCsvHdfsPath) 
         
        # Import the csv file to a composite xdf file in HDFS
        airDemoSmallXdfHdfsPath <- file.path(hadoopTestInfoList$myHdfsShareDir, 
            hadoopTestInfoList$myHdfsAirDemoXdfSubdir)
        rxHadoopRemoveDir(airDemoSmallXdfHdfsPath) # Remove directory if it's already there
        rxImport(inData = getAirDemoTextHdfsDS(), outFile = getAirDemoXdfHdfsDS(), 
            rowsPerRead = 200000, overwrite = TRUE)
        #rxHadoopListFiles(airDemoSmallXdfHdfsPath) 

    }  
    rxSetComputeContext(getLocalComputeContext())    
}

#############################################################################################
# Tests for getting file information
#############################################################################################

"test.hadoop.rxGetInfo" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # Test simple case
    rxSetComputeContext( getLocalComputeContext() )
    localInfo <- rxGetInfo( data = getAirDemoXdfLocalDS())
    
    rxSetComputeContext( getHadoopComputeContext() )
    hadoopInfo <- rxGetInfo( data = getAirDemoXdfHdfsDS() )
    
    checkEquals(localInfo$numRows, hadoopInfo$numRows)
    checkEquals(localInfo$numVars, hadoopInfo$numVars)
    
    # Test with arguments
    rxSetComputeContext( getLocalComputeContext() )
    localInfoArgs <- rxGetInfo( data = getAirDemoXdfLocalDS(), 
        getVarInfo = TRUE, getBlockSizes = TRUE, numRows = 5)
    
    rxSetComputeContext( getHadoopComputeContext() )
    hadoopInfoArgs <- rxGetInfo( data = getAirDemoXdfHdfsDS(),
         getVarInfo = TRUE, getBlockSizes = TRUE, numRows = 5)    
    
    checkIdentical(localInfoArgs$varInfo, hadoopInfoArgs$varInfo)
    # This will only be equal if there is only one composite data file
    checkEquals(localInfoArgs$rowsPerBlock, hadoopInfoArgs$rowsPerBlock)
    checkIdentical(localInfoArgs$data, hadoopInfoArgs$data)
    
    rxSetComputeContext( getLocalComputeContext() )
}

"test.hadoop.rxGetInfo.data.csv" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
       
    # Test using Text Data Sources
    rxSetComputeContext( getLocalComputeContext() )
    localInfoArgs <- rxGetInfo( data = getAirDemoTextLocalDS(), 
        getVarInfo = TRUE, getBlockSizes = TRUE, numRows = 5)
    
    rxSetComputeContext( getHadoopComputeContext() )
    hadoopInfoArgs <- rxGetInfo( data = getAirDemoTextHdfsDS(),
         getVarInfo = TRUE, getBlockSizes = TRUE, numRows = 5)    
    
    checkIdentical(localInfoArgs$varInfo, hadoopInfoArgs$varInfo)

    # This will only be equal if read from a small file - otherwise order may vary
    checkIdentical(localInfoArgs$data, hadoopInfoArgs$data)
    
    rxSetComputeContext( getLocalComputeContext() )
}

"test.hadoop.rxGetVarInfo" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    rxSetComputeContext( getLocalComputeContext() )
    localVarInfo <- rxGetVarInfo( getAirDemoXdfLocalDS() )
    
    rxSetComputeContext( getHadoopComputeContext() )
    hadoopVarInfo <- rxGetVarInfo( getAirDemoXdfHdfsDS() )
    
    checkIdentical(localVarInfo, hadoopVarInfo)
    
    rxSetComputeContext( getLocalComputeContext() )
}

"test.hadoop.rxGetVarNames" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    rxSetComputeContext( getLocalComputeContext() )
    localVarNames <- rxGetVarNames( getAirDemoXdfLocalDS() )
    
    rxSetComputeContext( getHadoopComputeContext() )
    hadoopVarNames <- rxGetVarNames( getAirDemoXdfHdfsDS() )
    
    checkEquals(localVarNames, hadoopVarNames)
    
    rxSetComputeContext( getLocalComputeContext() ) 
}

"test.hadoop.rxLocateFile" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # Local tests
    rxSetComputeContext( getLocalComputeContext() )
    
    # CSV file

    dataSource <- getAirDemoTextLocalDS()
    localFileName <- basename(dataSource@file)
    localDir <- dirname(dataSource@file)
    localLocated <- rxLocateFile(file = localFileName, pathsToSearch = localDir)
    checkEquals(normalizePath(localLocated), normalizePath(dataSource@file))
    
    dataSource <- getAirDemoXdfLocalDS()
    localFileName <- basename(dataSource@file)
    localDir <- dirname(dataSource@file)
    localLocated <- rxLocateFile(file = localFileName, pathsToSearch = localDir)
    checkEquals(normalizePath(localLocated), normalizePath(dataSource@file))
    
    # Hadoop
    # Csv
    rxSetComputeContext( getHadoopComputeContext() )
    dataSource <- getAirDemoTextHdfsDS()
    hadoopFileName <- basename(dataSource@file)
    hadoopDir <- dirname(dataSource@file)
    hadoopLocated <- rxLocateFile(file = hadoopFileName, pathsToSearch = hadoopDir, defaultExt = "", fileSystem = dataSource@fileSystem)
    checkEquals(hadoopLocated, file.path(dataSource@file, "AirlineDemoSmall.csv"))
    
    # Xdf
    dataSource <- getAirDemoXdfHdfsDS()
    hadoopFileName <- basename(dataSource@file)
    hadoopDir <- dirname(dataSource@file)
    hadoopLocated <- rxLocateFile(file = hadoopFileName, pathsToSearch = hadoopDir, defaultExt = ".xdf", fileSystem = dataSource@fileSystem)
    checkEquals(hadoopLocated, dataSource@file)
    
    rxSetComputeContext( getLocalComputeContext() )
}
 

"test.hadoop.rxReadXdf" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    rxSetComputeContext( getLocalComputeContext() )
    localDataFrame <- rxReadXdf( getAirDemoXdfLocalDS() )
    
    rxSetComputeContext( getHadoopComputeContext() )
    hadoopDataFrame <- rxReadXdf( getAirDemoXdfHdfsDS() )
    
    checkIdentical(localDataFrame, hadoopDataFrame)
    
    rxSetComputeContext( getLocalComputeContext() )
}

#############################################################################################
# Tests for plotting functions
#############################################################################################
"test.hadoop.rxHistogram" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    rxSetComputeContext( getLocalComputeContext() )
    localHistogram <- rxHistogram(~ArrDelay, data = getAirDemoXdfLocalDS(), 
        rowSelection = ArrDelay > 0, title = "Local Histogram" )
    
    rxSetComputeContext( getHadoopComputeContext() )
    hadoopHistogram <- rxHistogram(~ArrDelay, data = getAirDemoXdfHdfsDS(), 
        rowSelection = ArrDelay > 0, title = "Hadoop Histogram" )
    
    checkIdentical(localHistogram$panel.args, hadoopHistogram$panel.args)
    checkIdentical(localHistogram$call, hadoopHistogram$call)
   
    rxSetComputeContext( getLocalComputeContext() )
}

"test.hadoop.rxLinePlot" <- function()
{  
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    rxSetComputeContext( getLocalComputeContext() )
    localLinePlot <- rxLinePlot(ArrDelay~CRSDepTime, data = getAirDemoXdfLocalDS(), 
         rowSelection = ArrDelay > 60, type = "p", title = "Local Line Plot" )
    
    rxSetComputeContext( getHadoopComputeContext() )
    hadoopLinePlot <- rxLinePlot(ArrDelay~CRSDepTime,, data = getAirDemoXdfHdfsDS(), 
        rowSelection = ArrDelay > 60, type = "p", title = "Hadoop Line Plot" )
    
    checkIdentical(localLinePlot$panel.args, hadoopLinePlot$panel.args)
    
    hadoopCsvLinePlot <- rxLinePlot(ArrDelay~CRSDepTime,, data = getAirDemoTextHdfsDS(), 
        rowSelection = ArrDelay > 60, type = "p", title = "Hadoop Line Plot from Text File" )
    
    checkIdentical(localLinePlot$panel.args, hadoopCsvLinePlot$panel.args)
   
    rxSetComputeContext( getLocalComputeContext() )
}

#############################################################################################
# Tests for data step
#############################################################################################
"test.hadoop.rxDataStep.data.XdfToXdf" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # Run example locally
    rxSetComputeContext( getLocalComputeContext() )
    outLocalDS <- getTestOutFileLocalDS("DataStepTest")
    rxDataStep(inData = getAirDemoXdfLocalDS(), 
        outFile = outLocalDS, overwrite = TRUE)
    on.exit(removeTestOutFileLocalDS( outLocalDS ), add = TRUE)
    localInfo <- rxGetInfo(data = outLocalDS, getVarInfo = TRUE)

    rxSetComputeContext( getHadoopComputeContext() )
    outHdfsDS <- getTestOutFileHdfsDS("DataStepTest")
    rxDataStep(inData = getAirDemoXdfHdfsDS(),
        outFile = outHdfsDS, overwrite = TRUE)
    hadoopInfo = rxGetInfo(data = outHdfsDS, getVarInfo = TRUE)
    
    checkEquals(localInfo$numRows, hadoopInfo$numRows)
    checkIdentical(localInfo$varInfo, hadoopInfo$varInfo)
   
    # Clean-up
    removeTestOutFileHdfsDS( outHdfsDS )

    rxSetComputeContext( getLocalComputeContext() )
    removeTestOutFileLocalDS( outLocalDS )
}

"test.hadoop.rxDataStep.data.XdfToDataFrame" <- function()
{     
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    rxSetComputeContext( getLocalComputeContext() )
    localDataFrame <- rxDataStep(inData = getAirDemoXdfLocalDS()) 

    rxSetComputeContext( getHadoopComputeContext() )
    hadoopDataFrame <- rxDataStep(inData = getAirDemoXdfHdfsDS())
    
    checkIdentical(localDataFrame, hadoopDataFrame)
   
    rxSetComputeContext( getLocalComputeContext() )
}



#############################################################################################
# Tests for analysis functions that read data
#############################################################################################
"test.hadoop.rxCube.data.xdf" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # rxCube with input xdf file
    rxSetComputeContext( getLocalComputeContext() )
    localCube <- rxCube(ArrDelay~DayOfWeek, data = getAirDemoXdfLocalDS())

    rxSetComputeContext( getHadoopComputeContext() )
    hadoopCube <- rxCube(ArrDelay~DayOfWeek, data = getAirDemoXdfHdfsDS() )
    
    checkIdentical(localCube$Counts, hadoopCube$Counts)
   
    rxSetComputeContext( getLocalComputeContext() )
}
    
"test.hadoop.rxCube.data.data.frame" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )    
    ### rxCube with input data frame
 
    rxSetComputeContext( getLocalComputeContext() )
    localCube <- rxCube(~Species, data = iris)

    rxSetComputeContext( getHadoopComputeContext() )
    hadoopCube <- rxCube(~Species, data = iris)
    
    checkIdentical(localCube$Counts, hadoopCube$Counts)    
    rxSetComputeContext( getLocalComputeContext())
}

"test.hadoop.rxCrossTabs" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # rxCrosstabs with input xdf file
    rxSetComputeContext( getLocalComputeContext() )
    localCrossTabs <- rxCrossTabs(ArrDelay10~DayOfWeek, data = getAirDemoXdfLocalDS(),
		rowSelection = CRSDepTime > 10, transforms = list(ArrDelay10 = ArrDelay*10) )
	
    rxSetComputeContext( getHadoopComputeContext() )
    hadoopCrossTabs <- rxCrossTabs(ArrDelay10~DayOfWeek, data = getAirDemoXdfHdfsDS(),
		rowSelection = CRSDepTime > 10, transforms = list(ArrDelay10 = ArrDelay*10) )
    
    checkIdentical(localCrossTabs$sums, hadoopCrossTabs$sums)
	checkIdentical(localCrossTabs$counts, hadoopCrossTabs$counts)
	checkIdentical(localCrossTabs$chisquare, hadoopCrossTabs$chisquare)
   
    rxSetComputeContext( getLocalComputeContext() )
}

"test.hadoop.rxSummary" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # rxSummary with input xdf file
    rxSetComputeContext( getLocalComputeContext() )
    localSummary <- rxSummary(ArrDelay10~DayOfWeek, data = getAirDemoXdfLocalDS(), 
		rowSelection = CRSDepTime > 10, transforms = list(ArrDelay10 = 10*ArrDelay))

    rxSetComputeContext( getHadoopComputeContext() )
    hadoopSummary <- rxSummary(ArrDelay10~DayOfWeek, data = getAirDemoXdfHdfsDS(),
        rowSelection = CRSDepTime > 10, transforms = list(ArrDelay10 = 10*ArrDelay))
    
    checkEquals(localSummary$categorical[[1]], hadoopSummary$categorical[[1]])
   
    rxSetComputeContext( getLocalComputeContext() )
}

"test.hadoop.rxQuantile" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # rxQuantile with input xdf file
    rxSetComputeContext( getLocalComputeContext() )
    localQuantile <- rxQuantile("ArrDelay", data = getAirDemoXdfLocalDS()) 

    rxSetComputeContext( getHadoopComputeContext() )
    hadoopQuantile <- rxQuantile("ArrDelay", data = getAirDemoXdfHdfsDS())
    
    checkEquals(localQuantile, hadoopQuantile)
   
    rxSetComputeContext( getLocalComputeContext() )
}

"test.hadoop.rxLinMod" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # rxLinMod with input xdf file
    rxSetComputeContext( getLocalComputeContext() )
    localLinMod <- rxLinMod(ArrDelay~DayOfWeek, data = getAirDemoXdfLocalDS(), 
		rowSelection = CRSDepTime > 10)

    rxSetComputeContext( getHadoopComputeContext() )
    hadoopLinMod <- rxLinMod(ArrDelay~DayOfWeek, data = getAirDemoXdfHdfsDS(),
        rowSelection = CRSDepTime > 10)
    
    checkEquals(localLinMod$coefficients, hadoopLinMod$coefficients)
   
    rxSetComputeContext( getLocalComputeContext() )
}

"test.hadoop.rxLinMod.data.csv" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # rxLinMod with input xdf file
    rxSetComputeContext( getLocalComputeContext() )
    localLinMod <- rxLinMod(ArrDelay~DayOfWeek, data = getAirDemoTextLocalDS(), 
		rowSelection = CRSDepTime > 10)

    rxSetComputeContext( getHadoopComputeContext() )
    hadoopLinMod <- rxLinMod(ArrDelay~DayOfWeek, data = getAirDemoTextHdfsDS(),
        rowSelection = CRSDepTime > 10)
    
    checkEquals(localLinMod$coefficients, hadoopLinMod$coefficients)
   
    rxSetComputeContext( getLocalComputeContext() )
}

"test.hadoop.rxCovCor" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # rxCovCor with input xdf file
    rxSetComputeContext( getLocalComputeContext() )
    localCovCor <- rxCovCor(~ArrDelay+DayOfWeek+CRSDepTime10, data = getAirDemoXdfLocalDS(), 
		transforms = list(CRSDepTime10 = 10*CRSDepTime))

    rxSetComputeContext( getHadoopComputeContext() )
    hadoopCovCor <- rxCovCor(~ArrDelay+DayOfWeek+CRSDepTime10, data = getAirDemoXdfHdfsDS(), 
		transforms = list(CRSDepTime10 = 10*CRSDepTime))
    
    checkEquals(localCovCor$CovCor, hadoopCovCor$CovCor)
	checkEquals(localCovCor$StdDevs, hadoopCovCor$StdDevs)
	checkEquals(localCovCor$Means, hadoopCovCor$Means)
   
    rxSetComputeContext( getLocalComputeContext() )
}

"test.hadoop.rxGlm" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
    # rxGlm with input xdf file
    rxSetComputeContext( getLocalComputeContext() )
		
    localRxGlm <- rxGlm(ArrOnTime ~ CRSDepTime, data = getAirDemoXdfLocalDS(), 
		family = binomial(link = "probit"), transforms = list(ArrOnTime = ArrDelay < 2))

    rxSetComputeContext( getHadoopComputeContext() )
    hadoopRxGlm <- rxGlm(ArrOnTime ~ CRSDepTime, data = getAirDemoXdfHdfsDS(), 
		family = binomial(link = "probit"), transforms = list(ArrOnTime = ArrDelay < 2))
    
    checkEquals(localRxGlm$coefficients, hadoopRxGlm$coefficients)
	checkEquals(localRxGlm$coef.std.error, hadoopRxGlm$coef.std.error)
	checkEquals(localRxGlm$df, hadoopRxGlm$df)
   
    rxSetComputeContext( getLocalComputeContext() )
}


#############################################################################################
# Tests for analysis functions that write data
#############################################################################################

"test.hadoop.rxLogit.rxPredict" <- function()
{
	on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
	### Local computations
    rxSetComputeContext( getLocalComputeContext() )
	inLocalDS <- getAirDemoXdfLocalDS()
	outLocalDS <- getTestOutFileLocalDS("LogitPred")
	
	# Logistic regression
    localLogit <- rxLogit(ArrDel15~DayOfWeek, data = inLocalDS,
		transforms = list(ArrDel15 = ArrDelay > 15), maxIterations = 6 )
		
	# Predictions
    localPredOut <- rxPredict(modelObject = localLogit, data = inLocalDS, 
        writeModelVars = TRUE, predVarNames = "delayPred1", outData = outLocalDS, overwrite = TRUE) 
	
	outLocalVarInfo <- rxGetVarInfo( outLocalDS )	
	
	### Hadoop computations
    rxSetComputeContext( getHadoopComputeContext() )
	inHadoopDS <- getAirDemoXdfHdfsDS()
	outHadoopDS <- getTestOutFileHdfsDS("LogitPred")
	
	# Logistic regression
    hadoopLogit <- rxLogit(ArrDel15~DayOfWeek, data = inHadoopDS,
		transforms = list(ArrDel15 = ArrDelay > 15), maxIterations = 6 )
	
	checkEquals(hadoopLogit$coefficients, localLogit$coefficients)
	checkEquals(hadoopLogit$coef.std.error, localLogit$coef.std.error)
		
	# Predictions
    hadoopPredOut <- rxPredict(modelObject = hadoopLogit, data = inHadoopDS, 
        writeModelVars = TRUE, predVarNames = "delayPred1", outData = outHadoopDS, overwrite = TRUE) 
	
	outHadoopVarInfo <- rxGetVarInfo( outHadoopDS )	
    
    checkEquals(outHadoopVarInfo$delayPred1, outLocalVarInfo$delayPred1)
    checkEquals(outHadoopVarInfo$ArrDelay, outLocalVarInfo$ArrDelay)
    
    # Clean-up
    removeTestOutFileHdfsDS( outHadoopDS )

    rxSetComputeContext( getLocalComputeContext() )
    removeTestOutFileLocalDS( outLocalDS )    
	
}

"test.hadoop.rxKmeans.args.outFile" <- function()
{
    on.exit(rxSetComputeContext( getLocalComputeContext()), add = TRUE )
    
	testCenters <- matrix(c(13, 20, 7, 17, 10), ncol = 1)
	
	### Local computations
    rxSetComputeContext( getLocalComputeContext() )
    outLocalDS <- getTestOutFileLocalDS("KmeansOut")
	
    localKmeans <- rxKmeans( ~CRSDepTime, data = getAirDemoXdfLocalDS(), outFile = outLocalDS,
        centers = testCenters, writeModelVars = TRUE, overwrite = TRUE)
	
	localInfo <- rxGetInfo(outLocalDS, getVarInfo = TRUE)
	
    ### Hadoop computations
	rxSetComputeContext( getHadoopComputeContext() )
    outHadoopDS <- getTestOutFileHdfsDS("KmeansOut")
	
    hadoopKmeans <- rxKmeans( ~CRSDepTime, data = getAirDemoXdfHdfsDS(), outFile = outHadoopDS,
        centers = testCenters, writeModelVars = TRUE, overwrite = TRUE)
	
	hadoopInfo <- rxGetInfo(outHadoopDS, getVarInfo = TRUE)
    
    # Comparison tests
	checkEquals(hadoopInfo$numRows, localInfo$numRows)
	checkIdentical(hadoopInfo$varInfo, localInfo$varInfo)
	checkEquals(hadoopKmeans$centers, localKmeans$centers)
    
    # Clean-up
    removeTestOutFileHdfsDS( outHadoopDS )

    rxSetComputeContext( getLocalComputeContext() )
    removeTestOutFileLocalDS( outLocalDS )      
   
}

"test.hadoop.rxDTree.regression" <- function()             
{
    # Purpose:   Test basic regression tree functionality on Hadoop 

    formula <- as.formula( "ArrDelay ~ CRSDepTime + DayOfWeek" )
    
    ### Local computations   
    rxSetComputeContext( getLocalComputeContext() ) 
    inLocalDS <- getAirDemoXdfLocalDS()
    outLocalDS <- getTestOutFileLocalDS("DTreeOut")         
    localDTree <- rxDTree( formula, data  = inLocalDS,
                        blocksPerRead  = 30,
                        minBucket      = 500,
                        maxDepth       = 10,
                        cp             = 0,
                        xVal           = 0,
                        maxCompete     = 0,
                        maxSurrogate   = 0,
                        maxNumBins     = 101,
                        verbose        = 1,
                        reportProgress = 0 ) 
                         
    # Compute predictions
    localPred <- rxPredict( localDTree, data = inLocalDS, outData = outLocalDS, overwrite = TRUE,
                                      verbose = 1, reportProgress = 0 )                           
    localPredSum <- rxSummary( ~ ArrDelay_Pred, data = outLocalDS )
    
    
    ### Hadoop computations   
    rxSetComputeContext( getHadoopComputeContext() )  
    inHadoopDS <- getAirDemoXdfHdfsDS()
    outHadoopDS <- getTestOutFileHdfsDS("DTreeOut")           
    hadoopDTree <- rxDTree( formula, data  = inHadoopDS,
                        blocksPerRead  = 30,
                        minBucket      = 500,
                        maxDepth       = 10,
                        cp             = 0,
                        xVal           = 0,
                        maxCompete     = 0,
                        maxSurrogate   = 0,
                        maxNumBins     = 101,
                        verbose        = 1,
                        reportProgress = 0 ) 
                         
    # Compute predictions

    hadoopPred <- rxPredict( hadoopDTree, data = inHadoopDS, outData = outHadoopDS, overwrite = TRUE,
                                      verbose = 1, reportProgress = 0 )                           
    hadoopPredSum <- rxSummary( ~ ArrDelay_Pred, data = outHadoopDS)    
    
    # Perform checks
    
    #checkEquals( sum( ADSrxdt$splits[ , "improve" ] ), 0.181093099185139 )  
    checkEquals(sum(localDTree$splits[ , "improve" ] ), sum(hadoopDTree$splits[ , "improve" ] ))
    
    localPrune <- prune(localDTree,  cp = 1e-6 )
    hadoopPrune <- prune(hadoopDTree,  cp = 1e-6 ) 
    checkEquals(sum(localPrune$splits[ , "improve" ] ), sum(hadoopPrune$splits[ , "improve" ] ))  
            
    checkEquals(localPredSum$sDataFrame, hadoopPredSum$sDataFrame)
    
    # Clean-up
    removeTestOutFileHdfsDS( outHadoopDS )

    rxSetComputeContext( getLocalComputeContext() )
    removeTestOutFileLocalDS( outLocalDS )             
                      
}

#############################################################################################
# rxExec Example
#############################################################################################
 "test.hadoop.rxExec" <- function()
{
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
 		    else if (count == 1 && (roll == 2 || roll == 3 || roll == 12)) 
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
	### Local computations   
    rxSetComputeContext( getLocalComputeContext() ) 
    localExec <- rxExec(playCraps, timesToRun=100, taskChunkSize=25)
	
	rxSetComputeContext( getHadoopComputeContext() ) 
	hadoopExec <- rxExec(playCraps, timesToRun=100, taskChunkSize=25)
	checkEquals(length(localExec), length(hadoopExec))
	checkEquals(length(localExec[[1]]), length(hadoopExec[[1]]))

}	

#############################################################################################
# Always run last: Test to remove data from Hadoop cluster
#############################################################################################
"test.hadoop.zzz.removeHadoopData" <- function()
{
    # Always remove temp data
    rxSetComputeContext(getHadoopComputeContext())
    testOutputDir <- file.path(hadoopTestInfoList$myHdfsShareDir,
            hadoopTestInfoList$myHdfsTestOuputSubdir)
    rxHadoopRemoveDir(testOutputDir)
    if (hadoopTestInfoList$removeHadoopDataOnCompletion)
    {
        # Remove air data from Hadoop cluster
        
        airDemoSmallCsvHdfsPath <- file.path(hadoopTestInfoList$myHdfsShareDir, 
            hadoopTestInfoList$myHdfsAirDemoCsvSubdir) 
        rxHadoopRemoveDir(airDemoSmallCsvHdfsPath) 
            
        airDemoSmallXdfHdfsPath <- file.path(hadoopTestInfoList$myHdfsShareDir, 
            hadoopTestInfoList$myHdfsAirDemoXdfSubdir)
        rxHadoopRemoveDir(airDemoSmallXdfHdfsPath)          
    } 
    else
    {
        rxHadoopMakeDir(testOutputDir)
    }
    rxSetComputeContext(getLocalComputeContext()) 
}

