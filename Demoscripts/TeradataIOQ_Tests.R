
############################################################################################
# Copyright (C) 2014 Revolution Analytics, Inc
############################################################################################
#
# Run tests using both RxInTeradata and local compute contexts and compare results
# See TeradataIOQ_SetupAndRun.R for setup information
#
############################################################################################


#############################################################################################
# Function to get compute contexts
#############################################################################################
            
"getTeradataComputeContext" <- function()
{
    rxTestArgs <- getOption("rxTestArgs")
    if (is.null(rxTestArgs))
    {
        stop("Compute context information must be in 'rxTestArgs' option.  See TeradataIOQ_SetupAndRun.R.")
    }
    
    testCluster <- RxInTeradata( 
        connectionString = rxTestArgs$connectionString,
        shareDir = rxTestArgs$shareDir,
        remoteShareDir = rxTestArgs$remoteShareDir,
        revoPath = rxTestArgs$revoPath,
        consoleOutput = rxTestArgs$consoleOutput,
        wait = rxTestArgs$wait,
        autoCleanup = rxTestArgs$autoCleanup,
        traceEnabled = rxTestArgs$traceEnabled,
        traceLevel = rxTestArgs$traceLevel
    ) 

    return( testCluster )
}

"getLocalComputeContext" <- function()
{
    RxLocalSeq()
}

#############################################################################################
# Functions to get data sources
#############################################################################################
"getAirDemoTeradataDS" <- function( specifyLevels = TRUE)
{
    rxTestArgs <- getOption("rxTestArgs")
    if (is.null(rxTestArgs))
    {
        stop("Compute context information must be in 'rxTestArgs' option.  See TeradataIOQ_SetupAndRun.R.")
    }
    
    tdConnString = rxTestArgs$connectionString
    airColInfo <- NULL
    if (specifyLevels == TRUE)
    {
        airColInfo <- list(
            ArrDelay = list(type = "integer", low = -86, high = 1490),
			CRSDepTime = list(low = .016667, high = 23.98333),
            DayOfWeek = list(type = "factor", levels = as.character(1:7),
                newLevels = c("Monday", "Tuesday", "Wednesday", "Thursday", 
                "Friday", "Saturday", "Sunday")))
    }
    else
    {
         airColInfo <- list(
            ArrDelay = list(type = "integer"),
            DayOfWeek = list(type = "factor"),
            CRSDepTime = list(type = "float32"))
    }
       
    tdQuery <- "select * from RevoTestDB.AirlineDemoSmallR"
    testDataSource <- RxTeradata(
        connectionString = tdConnString, 
        sqlQuery = tdQuery, 
        colInfo = airColInfo,
        rowsPerRead = 5000) 
    
    return(testDataSource)   
     
}

"getAirDemoXdfLocalDS" <- function()
{
    RxXdfData(file = file.path(rxGetOption("sampleDataDir"), "AirlineDemoSmall.xdf"),
              fileSystem = "native")
}

"getTestTPT" <- function()
{
    rxTestArgs <- getOption("rxTestArgs")
    if (!is.null(rxTestArgs) && !is.null(rxTestArgs$rxTestTPT) && (rxTestArgs$rxTestTPT == TRUE))
    {
        return( TRUE )
    }
    return( FALSE)
}

"rxuTestAnalysisFunctionWithAirDemo" <- function( analysisName, analysisParams, testValues,
    printFirstOutput = FALSE)
{        
    origComputeContext <- rxGetComputeContext()
    on.exit(rxSetComputeContext(origComputeContext), add = TRUE)
    
    localComputeContext <- getLocalComputeContext() 
    distributedComputeContext <- getTeradataComputeContext()    
    localTestDataSource <- getAirDemoXdfLocalDS()
    teradataTestDataSource <- getAirDemoTeradataDS()
   
    rxSetComputeContext(localComputeContext)

	analysisParams$data <- localTestDataSource
	localResult <- do.call(analysisName, analysisParams)
    if (printFirstOutput)
    {
        cat("\nLocal results are:\n")
        print(localResult)
	}
    else
    {
        out1 <- capture.output(print(localResult))
        cat("\n", out1[1], "\n")
        cat("\n", out1[2], "\n")
    }
    
    testTPT <- getTestTPT()
    if (testTPT)
    {
        testDataSources <- list(teradataTestDataSource, teradataTestDataSource)
    }
    else
    {
        testDataSources <- list(teradataTestDataSource)
    }
    
      
	for (i in 1:length(testDataSources))
	{
		thisDataSource <- testDataSources[[i]]
        cat("-----------------------------------------------------------------------------\n")
        if (!testTPT || (i == 2))
        {
            cat(paste("Testing RxInTeradata compute context\n"))
            rxSetComputeContext( distributedComputeContext) 
            teradataType <- "InTeradata"
        }
        else
        {
            cat(paste("Testing RxTeradata data source in local compute context\n"))
            teradataType <- "TPT"
        }
        
		cat("\n")
		analysisParams$data <- testDataSources[[i]]
        tm <- system.time(
		    currentResult <- do.call(analysisName, analysisParams)
            )
        cat("Elapsed time for analysis:", tm["elapsed"], "\n")
        thisTime <- as.numeric(tm["elapsed"])
        if (!exists("rxTeradataTimings"))
        {
            rxTeradataTimings <<- data.frame(Analysis = analysisName,
                Type = teradataType, Time = thisTime, stringsAsFactors = FALSE)
        }
        else
        {
            newRow <- nrow(rxTeradataTimings) + 1
            rxTeradataTimings[newRow, "Analysis"] <<- analysisName
            rxTeradataTimings[newRow, "Type"] <<- teradataType
            rxTeradataTimings[newRow, "Time"] <<- thisTime
        }
        
              
        if (is.null(testValues))
        {
            cat(paste("    Testing for equal output.\n")) 
            checkEquals(localResult, currentResult)
        }
        else
        {
		    for (j in 1:length(testValues))
		    {
                currentTestValue <- currentResult[testValues[j]]
                cat(paste("    Testing output component:", testValues[j], "\n"))     
	            checkEquals(localResult[testValues[j]], currentTestValue)
		    }
        }
	}
	rxSetComputeContext( localComputeContext)	    
}


#############################################################################################
# Core High-Performance Analytics Algorithms Available In-Database
#############################################################################################

"test.teradata.HPA.rxSummary" <- function()
{
    rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxSummary", 
	    analysisParams = list(
            formula = ArrDelay10~DayOfWeek, 
            transforms = substitute(list(ArrDelay10 = ArrDelay)),
            rowSelection = substitute(CRSDepTime > 10),
            reportProgress = 0
            ),
	    testValues = c("categorical")
        )    
    
	# Underlying code:
	# Test code for local computations
    #rxSetComputeContext( getLocalComputeContext() )
    #localSummary <- rxSummary(ArrDelay10~DayOfWeek, data = getAirDemoXdfLocalDS(), 
	#	rowSelection = CRSDepTime > 10, transforms = list(ArrDelay10 = 10*ArrDelay))
    #
	# Test code for in-database computations
    #rxSetComputeContext( getTeradataComputeContext() )
	#myDataSource <- getAirDemoTeradataDS()
    #teradataSummary <- rxSummary(ArrDelay10~DayOfWeek, data = myDataSource,
       #rowSelection = CRSDepTime > 10, transforms = list(ArrDelay10 = 10*ArrDelay))	
            
}

"test.aaa.teradata" <- function()
{
	tdCompute <- getTeradataComputeContext()
	cat(paste("Connection String Tested:\n",
		"  ", tdCompute@connectionString, "\n"))
	print(rxGetNodeInfo(tdCompute))
}

"test.teradata.HPA.rxCrossTabs" <- function()
{ 
    rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxCrossTabs", 
	    analysisParams = list(
            formula = ArrDelay10~DayOfWeek, 
            transforms = substitute(list(ArrDelay10 = ArrDelay)),
            rowSelection = substitute(CRSDepTime > 10),
            reportProgress = 0
            ),
	    testValues = c("sums",  "counts", "chisquare")
        )
}


"test.teradata.HPA.rxCube" <- function()
{
    
    rxuTestAnalysisFunctionWithAirDemo( 
		analysisName = "rxCube", 
		analysisParams = list(
            formula = ArrDelay10~DayOfWeek, 
            transforms = substitute(list(ArrDelay10 = ArrDelay)),
            reportProgress = 0
            ),
		testValues = c("ArrDelay10",  "Counts")
        )    
}
    

"test.teradata.HPA.rxLinMod" <- function()
{ 
    rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxLinMod", 
	    analysisParams = list(
            formula = ArrDelay~DayOfWeek, 
            rowSelection = substitute(CRSDepTime > 10),
            reportProgress = 0
            ),
	    testValues = c("coefficients", "residual.squares", "condition.number",
            "rank", "coef.std.error", "coef.t.value", "df"  )
        )          
}

"test.teradata.HPA.rxLinMod.stepwise" <- function()
{
    direction <- "both"    
    scope <- NULL
    steps <- 1000
    scale <- 0
    k <- 2
    test <- NULL       
    
    varSel <- list( method = "stepwise", maxSteps = 100, scale = 0, k = 2, matchRStep = FALSE)
    
    rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxLinMod", 
	    analysisParams = list(
            formula = ArrDelay~DayOfWeek + CRSDepTime, 
            variableSelection = substitute(varSel),
            reportProgress = 0
            ),
	    testValues = c("coefficients", "residual.squares", "condition.number",
            "rank", "coef.std.error", "coef.t.value", "df"  )
        )         
}

"test.teradata.HPA.rxLinMod.args.formula.F.high.low" <- function()
{

    rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxLinMod", 
	    analysisParams = list(
            formula = ArrDelay~DayOfWeek + F(CRSDepTime, low = 0, high = 24), 
            reportProgress = 0
            ),
	    testValues = c("coefficients", "residual.squares", "condition.number",
            "rank", "coef.std.error", "coef.t.value", "df"  )
        )     
}

"test.teradata.HPA.rxCovCor" <- function()
{
    rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxCovCor", 
	    analysisParams = list(
            formula = ~ArrDelay+DayOfWeek+CRSDepTime10, 
            transforms = substitute(list(CRSDepTime10 = CRSDepTime*10)),
            reportProgress = 0
            ),
	    testValues = c("CovCor", "StdDevs", "Means")
        ) 

}

"test.teradata.HPA.rxCov" <- function()
{
    
     rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxCov", 
	    analysisParams = list(
            formula = ~ArrDelay+DayOfWeek+CRSDepTime10, 
            transforms = substitute(list(CRSDepTime10 = CRSDepTime * 10)),
            rowSelection = substitute(CRSDepTime > 10),
            reportProgress = 0
            ),
	    testValues = NULL
        )      
}

"test.teradata.HPA.rxCor" <- function()
{
    
     rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxCor", 
	    analysisParams = list(
            formula = ~ArrDelay+DayOfWeek+CRSDepTime, 
            rowSelection = substitute(CRSDepTime > 10),
            reportProgress = 0
            ),
	    testValues = NULL
        )
}

"test.teradata.HPA.rxSSCP" <- function()
{
    
      rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxSSCP", 
	    analysisParams = list(
            formula = ~ArrDelay+DayOfWeek+CRSDepTime, 
            reportProgress = 0
            ),
	    testValues = NULL
        )

}

"test.teradata.HPA.rxLogit" <- function()
{
     rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxLogit", 
	    analysisParams = list(
            formula = Late~DayOfWeek, 
            transforms = substitute(list(Late = ArrDelay > 15)),
            maxIterations = 6,
            reportProgress = 0
            ),
	    testValues = c("coefficients", "condition.number", "rank", "coef.std.error",
            "coef.t.value", "total.squares", "df", "deviance", "aic")
        ) 
       
}


"test.teradata.HPA.rxGlm" <- function()
{
      rxuTestAnalysisFunctionWithAirDemo( 
	    analysisName = "rxGlm", 
	    analysisParams = list(
            formula = ArrOnTime ~ CRSDepTime, 
            family = binomial(link = "probit"),
            transforms = substitute(list(ArrOnTime = ArrDelay < 2)),
            maxIterations = 6,
            reportProgress = 0
            ),
	    testValues = c("coefficients", "condition.number", "rank", "coef.std.error",
            "coef.t.value", "df", "deviance")
        ) 
    
}

"test.teradata.HPA.rxKmeans" <- function()
{
  
  testCenters <- matrix(c(13, 20, 7, 17, 10), ncol = 1)     
  rxuTestAnalysisFunctionWithAirDemo( 
    analysisName = "rxKmeans", 
    analysisParams = list(
        formula = ~CRSDepTime, 
        centers = substitute(testCenters),
        reportProgress = 0
        ),
    testValues = c("centers", "withinss", "size")
    )         
}

"test.teradata.HPA.rxKmeans.outFile" <- function()
{
   # Local computation
    testCenters <- matrix(c(13, 20, 7, 17, 10), ncol = 1)
    localOutFile <- tempfile(pattern = "rxKmeans", fileext = ".xdf")
    rxSetComputeContext( getLocalComputeContext() )
    localKmeans <- rxKmeans( ~CRSDepTime, data = getAirDemoXdfLocalDS(), 
        outFile = localOutFile, centers = testCenters, 
        writeModelVars = TRUE, overwrite = TRUE)

    localSummary <- rxSummary(~., data = localOutFile)  
    file.remove(localOutFile)      
   
    # In-Database computation
    tdCompute <- getTeradataComputeContext()
    rxSetComputeContext( tdCompute )
	
	# Create output data source
	tempTable <- "rxTempKmeansTest"
	if (rxTeradataTableExists(tempTable)) rxTeradataDropTable(tempTable)	
    teradataOutDS <- RxTeradata( table=tempTable, connectionString = tdCompute@connectionString)
	# Run rxKmeans	
    tdKmeans <- rxKmeans( ~CRSDepTime, data = getAirDemoTeradataDS(), 
        outFile = teradataOutDS, centers = testCenters, 
        writeModelVars = TRUE, overwrite = TRUE) 
	# Compute summaries of output data
	tdSummary <- rxSummary(~., data = teradataOutDS)
	rxTeradataDropTable(tempTable)
	# Compare results
	checkEquals(localSummary$sDataFrame, tdSummary$sDataFrame, tolerance=1e-6)   
}

"test.teradata.HPA.rxDTree" <- function()
{  
 
	#rxSetComputeContext( getLocalComputeContext() )
	#myDataSource <- getAirDemoXdfLocalDS()
    #localTree <- rxDTree(ArrDelay~ CRSDepTime, data = myDataSource,
         #maxDepth = 2,  xVal = 0, minSplit = 775,
	     #maxNumBins = 101, allowDiskWrite = FALSE, reportProgress = 0)	
	#
    #rxSetComputeContext( getTeradataComputeContext() )
	#myDataSource <- getAirDemoTeradataDS()
    #teradataTree <- rxDTree(ArrDelay~ CRSDepTime, data = myDataSource,
         #maxDepth = 2,  xVal = 0, minSplit = 775,
	     #maxNumBins = 101, allowDiskWrite = FALSE, reportProgress = 0)	
	
  rxuTestAnalysisFunctionWithAirDemo( 
    analysisName = "rxDTree", 
    analysisParams = list(
        formula = ArrDelay~CRSDepTime, 
        maxDepth       = 22,
        xVal           = 0,
        minSplit       = 775,
        maxNumBins     = 101,
		allowDiskWrite = FALSE,
        reportProgress = 0
        ),
    testValues = c("frame", "where", "cptable")
    )    
                         
}  

"test.teradata.HPA.rxDForest" <- function()
{  
	
    # Compute local forest
	rxSetComputeContext( getLocalComputeContext() )
    myDataSource <-  getAirDemoXdfLocalDS()
 	
    localForest <- rxDForest(ArrDelay~CRSDepTime, data = myDataSource,
        computeOobError = -1, nTree = 2, maxDepth = 2, numThreads = 1, 
		batchLength = 1, reportProgress = 0) 
		
	# Compute Teradata forest
    rxSetComputeContext( getTeradataComputeContext() )
	myDataSource <- getAirDemoTeradataDS()
    teradataForest <- rxDForest(ArrDelay~CRSDepTime, data = myDataSource,
        computeOobError = -1, nTree = 2, maxDepth = 2, numThreads = 1, 
		batchLength = 1, reportProgress = 0) 	
	
    checkEquals( sort(class( teradataForest )), "rxDForest" )

	# NOTE: the forests will be different for local and distributed 
	# as the RNG used in local and distributed cases are different.

	checkEquals(length(teradataForest$forest), length(localForest$forest))
	
				
                  
}  
	
#############################################################################################
#
# Additional HPA Functions
#
#############################################################################################

"test.teradata.HPA.wrapper.rxQuantile" <- function()
{
    rxSetComputeContext( getLocalComputeContext() )
    localQuantile <- rxQuantile("ArrDelay", data = getAirDemoXdfLocalDS()) 
    
    if (getTestTPT())
    {    
        tptQuantile <- rxQuantile("ArrDelay", data = getAirDemoTeradataDS())
        checkEquals(localQuantile, tptQuantile)
    }
    
    rxSetComputeContext( getTeradataComputeContext() )
    teradataQuantile <- rxQuantile("ArrDelay", data = getAirDemoTeradataDS())
    
    checkEquals(localQuantile, teradataQuantile)
    
    # Try with low/high value specified in colInfo
    teradataDS <- getAirDemoTeradataDS()
    teradataDS@colInfo <- list(ArrDelay = list(low = -86, high = 1490))
    teradataQuantile1 <- rxQuantile("ArrDelay", data = teradataDS)
    checkEquals(localQuantile, teradataQuantile1)
    
    rxSetComputeContext( getLocalComputeContext() )
}

"test.teradata.HPA.wrapper.rxLorenz" <- function()
{
    rxSetComputeContext( getLocalComputeContext() )
    
	localLorenz <- rxLorenz(orderVarName = "CRSDepTime", data = getAirDemoXdfLocalDS())
	
	lorenzPlot <- plot(localLorenz, equalityWidth = 1, 
		title = "Lorenz Curve for CRSDepTime Computed Locally",
		equalityColor = "black", equalityStyle = "longdash", lineWidth = 3)
	
	# Compute Gini
	localGini <- rxGini(localLorenz)

    if (getTestTPT())
    {    
        tptLorenz <- rxLorenz(orderVarName = "CRSDepTime", data = getAirDemoTeradataDS())
        tptGini <- rxGini(tptLorenz)
        checkEquals(localLorenz, tptLorenz)
        checkEquals(localGini, tptGini)
    }
    
    rxSetComputeContext( getTeradataComputeContext() )
    tdLorenz <- rxLorenz(orderVarName = "CRSDepTime", data = getAirDemoTeradataDS())
    tdGini <- rxGini(tdLorenz)
	checkEquals(localLorenz, tdLorenz)
    checkEquals(localGini, tdGini)
    
	lorenzPlot <- plot(localLorenz, equalityWidth = 1, 
		title = "Lorenz Curve for CRSDepTime Computed In-Database",
		equalityColor = "black", equalityStyle = "longdash", lineWidth = 3)    
    
    # Try with low/high value specified in colInfo
    teradataDS <- getAirDemoTeradataDS()
    teradataDS@colInfo <- list(CRSDepTime = list(low = 0.0167, high = 23.9833))
    tdLorenz2 <- rxLorenz(orderVarName = "CRSDepTime", data = teradataDS)
    tdGini2 <- rxGini(tdLorenz2)
    checkEquals(localLorenz, tdLorenz2)
    checkEquals(localGini, tdGini2) 
    rxSetComputeContext( getLocalComputeContext() )
}

"test.teradata.HPA.wrapper.rxRoc" <- function()
{
    rxSetComputeContext( getLocalComputeContext() )
    
	localLogit <- rxLogit( formula = Late~DayOfWeek, 
            transforms = list(Late = ArrDelay > 15), data = getAirDemoXdfLocalDS())

	tempXdf <- tempfile(pattern = "rxTestRoc", fileext = ".xdf")
	localPredict <- rxPredict(localLogit, data = getAirDemoXdfLocalDS(), 
		outData = tempXdf, writeModelVars = TRUE, predVarNames = "LatePred")
	localRoc <- rxRoc(actualVarName = "Late", predVarNames = "LatePred", 
		data = localPredict)
	
	
	plot(localRoc, title = "Local ROC Curve")
	#localCurve <- rxRocCurve(actualVarName = "Late", predVarNames = "LatePred", 
		#data = localPredict)
	file.remove(tempXdf)
	
    if (getTestTPT())
    {    
    }
    
	tdCompute <- getTeradataComputeContext()
    rxSetComputeContext( tdCompute )
	
	teraLogit <- rxLogit( formula = Late~DayOfWeek, 
            transforms = list(Late = ArrDelay > 15), data = getAirDemoTeradataDS())

	tempTableName <- "rxRocTemp"
	if (rxTeradataTableExists(tempTableName)) rxTeradataDropTable(tempTableName)
	tempTeraDS <- RxTeradata(table = tempTableName, connectionString = tdCompute@connectionString)
	teraPredict <- rxPredict(teraLogit, data = getAirDemoTeradataDS(), 
		outData = tempTeraDS, writeModelVars = TRUE, predVarNames = "LatePred", overwrite = TRUE)
	
	teraRoc <- rxRoc(actualVarName = "Late", predVarNames = "LatePred", 
		data = tempTeraDS)
	plot(teraRoc, title = "ROC Curve Computed In-Database")
	rxTeradataDropTable(tempTableName)
		
    checkEquals(teraRoc, localRoc)
    rxSetComputeContext( getLocalComputeContext() )
}

"test.teradata.HPA.wrapper.rxHistogram.data.factor" <- function()
{
    rxSetComputeContext( getLocalComputeContext() )
    localHistogram <- rxHistogram(~DayOfWeek, data = getAirDemoXdfLocalDS(), 
        rowSelection = ArrDelay > 0, title = "Local Histogram: Factor Data" )
    
    rxSetComputeContext( getTeradataComputeContext() )
    teradataDS <- getAirDemoTeradataDS()
    teradataHistogram <- rxHistogram(~DayOfWeek, data = teradataDS, 
        rowSelection = ArrDelay > 0, title = "Histogram Computed In-Database: Factor Data" )
    
    checkIdentical(localHistogram$panel.args, teradataHistogram$panel.args)
   
    rxSetComputeContext( getLocalComputeContext() )
}

"test.teradata.HPA.wrapper.rxHistogram.data.continuous" <- function()
{
    rxSetComputeContext( getLocalComputeContext() )
    localHistogram <- rxHistogram(~ArrDelay, data = getAirDemoXdfLocalDS(), 
        rowSelection = ArrDelay > 0, title = "Local Histogram: Continuous Data" )
    
    rxSetComputeContext( getTeradataComputeContext() )
    teradataHistogram <- rxHistogram(~ArrDelay, data = getAirDemoTeradataDS(), 
        rowSelection = ArrDelay > 0, title = "Histogram Computed In-Database: Continuous Data" )
    
    checkIdentical(localHistogram$panel.args, teradataHistogram$panel.args)
    checkIdentical(localHistogram$call, teradataHistogram$call)
   
    rxSetComputeContext( getLocalComputeContext() )
}

"test.teradata.HPA.wrapper.rxHistogram.data.few.integers" <- function()
{
    rxSetComputeContext( getLocalComputeContext() )
    localHistogram <- rxHistogram(~DayOfWeekInt, data = getAirDemoXdfLocalDS(), 
        transforms = list(DayOfWeekInt = as.integer(DayOfWeek)), title = "Local Histogram: Few Integers" )
    
    rxSetComputeContext( getTeradataComputeContext() )
    tdDataSource <- getAirDemoTeradataDS()
    tdDataSource@colInfo[["DayOfWeek"]] <- list(type = "integer", low = 1, high = 7)
    teradataHistogram <- rxHistogram(~DayOfWeek, data = tdDataSource, 
         title = "Histogram Computed In-Database: Few Integers" )
    
    checkIdentical(localHistogram$panel.args, teradataHistogram$panel.args)
    checkIdentical(localHistogram$call, teradataHistogram$call)
   
    rxSetComputeContext( getLocalComputeContext() )
}


#############################################################################################
#
# Tests for compute context helper functions
#
#############################################################################################
"test.teradata.helper.rxGetNodeInfo" <- function()
{
    tdComputeContext <- getTeradataComputeContext()
    nodeInfo <- rxGetNodeInfo(tdComputeContext)
    numAmps <-  getOption("rxTestArgs")$rxNumAmps
    if (!is.null(numAmps))
    {
        checkEquals(nodeInfo[[1]]$numAmps, numAmps)
    }
}
    
#############################################################################################
# Tests for getting data source information
#############################################################################################

"test.teradata.dataSource.rxGetInfo" <- function()
{
    
    rxSetComputeContext( getLocalComputeContext() )
    localInfo1 <- rxGetInfo( data = getAirDemoXdfLocalDS())
    localInfo2 <- rxGetInfo( data = getAirDemoXdfLocalDS(), getVarInfo = TRUE)
    localInfo3 <- rxGetInfo( data = getAirDemoXdfLocalDS(), getVarInfo = TRUE, numRows = 5)
    
    rxSetComputeContext( getTeradataComputeContext() )
    teradataInfo1 <- rxGetInfo(data = getAirDemoTeradataDS())
    teradataInfo2 <- rxGetInfo( data = getAirDemoTeradataDS(), getVarInfo = TRUE,
        computeInfo = TRUE )
    teradataInfo3 <- rxGetInfo( data = getAirDemoTeradataDS(), getVarInfo = TRUE,
        computeInfo = TRUE, numRows = 5 )    
    
    checkEquals(localInfo2$numRows, teradataInfo2$numRows)
    checkEquals(localInfo2$numVars + 1L, teradataInfo2$numVars)  # Teradata has RowID
    
    # Order of data will differ
    checkEquals(nrow(localInfo3$data), nrow(teradataInfo3$data))
    rxSetComputeContext( getLocalComputeContext() )
}


"test.teradata.dataSource.rxGetVarInfo" <- function()
{
    
    rxSetComputeContext( getLocalComputeContext() )
    localVarInfo <- rxGetVarInfo( getAirDemoXdfLocalDS() )
    
    rxSetComputeContext( getTeradataComputeContext() )
    teradataVarInfo <- rxGetVarInfo(getAirDemoTeradataDS() )
    
    checkEquals(names(localVarInfo), names(teradataVarInfo)[1:3])  
      
    rxSetComputeContext( getLocalComputeContext() )
    teradataDS <- getAirDemoTeradataDS()
	
    # Test tested without pre-specifying factor levels in colInfo  
    tdColInfo <- teradataDS@colInfo
    tdColInfo[["DayOfWeek"]] <- list(type = "factor")
	tdColInfo["ArrDelay"] <- NULL
	tdColInfo["CRSDepTime"] <- NULL
    teradataDS@colInfo <- tdColInfo
    teradataVarInfo <- rxGetVarInfo(teradataDS, computeInfo = TRUE ) 
    checkEquals(localVarInfo$ArrDelay, teradataVarInfo$ArrDelay) 
    checkEquals(localVarInfo$CRSDepTime$low, teradataVarInfo$CRSDepTime$low, tolerance = 1e-6 ) 
    checkEquals(localVarInfo$CRSDepTime$high, teradataVarInfo$CRSDepTime$high, tolerance = 1e-6) 
    checkEquals(as.character(1:7), sort(teradataVarInfo$DayOfWeek$levels)) 
    
    rxSetComputeContext( getLocalComputeContext() )
}

"test.teradata.dataSource.rxGetVarNames" <- function()
{   
    rxSetComputeContext( getLocalComputeContext() )
    localVarNames <- rxGetVarNames( getAirDemoXdfLocalDS() )
    
    rxSetComputeContext( getTeradataComputeContext() )
    teradataVarNames <- rxGetVarNames( getAirDemoTeradataDS() )
    
    checkEquals(localVarNames, teradataVarNames[1:3])
    
    rxSetComputeContext( getLocalComputeContext() ) 
}


"test.teradata.extra.rxLinePlot" <- function()
{    
    rxSetComputeContext( getLocalComputeContext() )
    localLinePlot <- rxLinePlot(ArrDelay~CRSDepTime, data = getAirDemoXdfLocalDS(), 
         rowSelection = ArrDelay > 60, type = "p", title = "Scatter Plot: Local Data" )
    
    rxSetComputeContext( getTeradataComputeContext() )
    teradataLinePlot <- rxLinePlot(ArrDelay~CRSDepTime, data = getAirDemoTeradataDS(), 
        rowSelection = ArrDelay > 60, type = "p", title = "Scatter Plot: Teradata Data" )
    
    checkEquals(sort(localLinePlot$panel.args[[1]]$x), sort(teradataLinePlot$panel.args[[1]]$x))
    checkEquals(sort(localLinePlot$panel.args[[1]]$y), sort(teradataLinePlot$panel.args[[1]]$y))
   
    rxSetComputeContext( getLocalComputeContext() )
}


#############################################################################################
# Tests for import read
#############################################################################################


"test.teradata.dataSource.rxImport.to.data.frame" <- function()
{
    rxSetComputeContext( getLocalComputeContext() )
    localDataFrame <- rxImport( getAirDemoTeradataDS() )
    localDataFrameSum <- rxSummary(~., data = localDataFrame)
    
    rxSetComputeContext( getTeradataComputeContext() )
    # Note: this uses a local compute context under the hood
    teradataDataFrame <- rxImport( inData = getAirDemoTeradataDS() )  
    
    rxSetComputeContext( getLocalComputeContext() )
    teradataDataFrameSum <- rxSummary(~., data = teradataDataFrame)
    
    checkEquals(localDataFrameSum$sDataFrame, teradataDataFrameSum$sDataFrame)
    checkEquals(localDataFrameSum$categorical, teradataDataFrameSum$categorical)    
   
}
#############################################################################################
# Tests for data step
#############################################################################################

"test.teradata.rxDataStep.aggregate" <- function()
{
	ProcessChunk <- function( dataList)
	{	
		# Process Data 
		chunkTable <- table(as.data.frame(dataList))
		# Convert table to data frame with single row
		varNames <- names(chunkTable)
		  varValues <- as.vector(chunkTable)
		  dim(varValues) <- c(1, length(varNames))
		  chunkDF <- as.data.frame(varValues)
		  names(chunkDF) <- varNames
		  # Return the data frame 
		return( chunkDF )
	}
		
	tdCompute <- getTeradataComputeContext()
    rxSetComputeContext( tdCompute )
			
	inDataSource <- getAirDemoTeradataDS(specifyLevels = TRUE)
	tdQuery <- "select DayOfWeek from RevoTestDB.AirlineDemoSmallR"
	#inDataSource <- RxTeradata(sqlQuery = tdQuery, 
		#colInfo = list(DayOfWeek = list(type = "factor", levels = as.character(1:7))))
	inDataSource@sqlQuery = tdQuery
	
	iroDataSource = RxTeradata(table = "RevoTestDB.iroResults", connectionString = tdCompute@connectionString)
	if (rxTeradataTableExists("RevoTestDB.iroResults")) rxTeradataDropTable("RevoTestDB.iroResults")
		
	rxDataStep( inData = inDataSource, outFile = iroDataSource,
            varsToKeep = "DayOfWeek",
			transformFunc = ProcessChunk,
			reportProgress = 0, overwrite = TRUE)

	iroResults <- rxImport(iroDataSource)

	finalResults <- colSums(iroResults) 
	checkEquals(as.vector(finalResults) ,c(97975, 77725, 78875, 81304, 82987, 86159, 94975)) 
	rxTeradataDropTable("RevoTestDB.iroResults")
	
	rxSetComputeContext( getLocalComputeContext() )
}


"test.teradata.rxDataStep.new.table" <- function()
{
    rxSetComputeContext( getLocalComputeContext() )
	localDS <- getAirDemoXdfLocalDS()
	localTempOut <- tempfile(pattern = "rxTestDataStep",  fileext = ".xdf")
	rxDataStep(inData = localDS, outFile = localTempOut, 
		transforms = list(ArrDelay10 = ArrDelay*10), rowSelection = ArrDelay > 60,
		overwrite = TRUE)
	localSummary <- rxSummary(~., localTempOut)
	file.remove(localTempOut)
	
    # In-Database
    rxSetComputeContext( getTeradataComputeContext() )
	teradataDS <- getAirDemoTeradataDS()
	
	tempTable <- "rxTempTestDataStep"
	if (rxTeradataTableExists(tempTable)) rxTeradataDropTable(tempTable)
	teradataTempOut <- RxTeradata( table = tempTable)
	rxDataStep(inData = teradataDS, outFile = teradataTempOut, 
		transforms = list(ArrDelay10 = ArrDelay*10), rowSelection = ArrDelay > 60,
		overwrite = TRUE)
	# Note: factors will be written as character data
	teradataTempOut <- RxDataSource(dataSource = teradataTempOut, colInfo = list( DayOfWeek =
		list(type = "factor", levels = c("Monday", "Tuesday", 
			"Wednesday", "Thursday", "Friday", "Saturday", "Sunday"))))
	teradataSummary <- rxSummary(~., teradataTempOut)
	rxTeradataDropTable(tempTable)
	
	rxSetComputeContext( getLocalComputeContext() )
	
	localStats <- localSummary$sDataFrame
	tdStats <- teradataSummary$sDataFrame
	# Remove  extra variable from results
	tdStats <- tdStats[tdStats$Name != "RowID",]
	row.names(tdStats) <- NULL

	checkEquals(localStats, tdStats, tolerance = 1e-6 )
	checkEquals(localSummary$categorical, teradataSummary$categorical)


}

#############################################################################################
# Tests for rxPredict
#############################################################################################
"test.teradata.HPA.rxPredict" <- function()
{
    rxSetComputeContext( getLocalComputeContext() )
    
	localInDS <- getAirDemoXdfLocalDS()
    localLinMod <- rxLinMod(ArrDelay~DayOfWeek + CRSDepTime, data = localInDS)
	localOutFile <- tempfile(pattern = "rxPredictTempTest", fileext = ".xdf")
    localPred <- rxPredict(localLinMod, data = localInDS, outData = localOutFile, 
        writeModelVars = TRUE, overwrite = TRUE)
    localSummary <- rxSummary(~., data = localOutFile)  
    file.remove(localOutFile) 
	
    # In-Database computation
    tdCompute <- getTeradataComputeContext()
    rxSetComputeContext( tdCompute )
	
	teradataInDS <- getAirDemoTeradataDS()
	teradataLinMod <- rxLinMod(ArrDelay~DayOfWeek + CRSDepTime, data = teradataInDS)	
	
	# Create output data source
	tempTable <- "rxTempPredictTest"
	if (rxTeradataTableExists(tempTable)) rxTeradataDropTable(tempTable)	
    teradataOutDS <- RxTeradata( table=tempTable, connectionString = tdCompute@connectionString)
	
	# Compute predictions	
    teradataPred <- rxPredict(teradataLinMod, data = teradataInDS, outData = teradataOutDS, 
        writeModelVars = TRUE, overwrite = TRUE)	

	# Note: factor data is written out as character data
	teradataOutDS <- RxDataSource(dataSource = teradataOutDS, colInfo = list( DayOfWeek =
		list(type = "factor", levels = c("Monday", "Tuesday", 
			"Wednesday", "Thursday", "Friday", "Saturday", "Sunday"))))	

	tdSummary <- rxSummary(~., data = teradataOutDS)
	rxTeradataDropTable(tempTable)
	 rxSetComputeContext( getLocalComputeContext() )
	
	# Compare results
	checkEquals(localSummary$sDataFrame, tdSummary$sDataFrame, tolerance=1e-6)
	checkEquals(localSummary$categorical, tdSummary$categorical)
    
}
    
#############################################################################################
# rxExec Examples
#############################################################################################
"test.teradata.HPC.rxExec.elemArgs" <- function()
{
	# Pass in an argument
	countToX <- function(x)
	{
		return(1:x)
	}
	
	rxSetComputeContext( getLocalComputeContext() )
	argVals <- list(1,2,3)
    localExec <- rxExec(countToX, elemArgs = argVals)
	
	rxSetComputeContext( getTeradataComputeContext() ) 
	teradataExec <- rxExec(countToX, elemArgs = argVals)
		
	localVecOut <- as.vector(unlist(localExec))
	teradataVecOut <- as.vector(unlist(teradataExec))
	checkEquals(localVecOut, teradataVecOut)
	
	rxSetComputeContext( getLocalComputeContext() )
	
	"testFuncSingleUntypedParam" <- function( singleArg )
	{
		print( singleArg )
		singleArg
	}
	rxSetComputeContext( getTeradataComputeContext() ) 
	#teradataExec <- rxExec(testFuncSingleUntypedParam, elemArgs = NULL) # Doesn't work
	teradataExec <- rxExec( testFuncSingleUntypedParam, rxElemArg( list(NULL, NULL, NULL, NULL) ))
	checkEquals( 4L, length( teradataExec ) )
	checkTrue( is.null(unlist(teradataExec)) )	
	
	rxSetComputeContext( getTeradataComputeContext() ) 
	testOutput <- rxExec(function(){ rnorm(5) }, timesToRun=1, taskChunkSize=5)
	checkEquals(1L, length(testOutput))
}


"test.teradata.HPC.rxExec.timesToRun" <- function()
{
	countToTen <- function()
	{
		return(1:10)
	}
	
	rxSetComputeContext( getLocalComputeContext() ) 
    localExec <- rxExec(countToTen, timesToRun = 10)
	
	rxSetComputeContext( getTeradataComputeContext() ) 
	teradataExec <- rxExec(countToTen, timesToRun=10)
	
	localVecOut <- as.vector(unlist(localExec))
	teradataVecOut <- as.vector(unlist(teradataExec))
	checkEquals(localVecOut, teradataVecOut)
	
	# Pass in an argument
	countToX <- function(x)
	{
		return(1:x)
	}
	
	upperVal <- 3
	rxSetComputeContext( getLocalComputeContext() )
    localExec <- rxExec(countToX, x=upperVal, timesToRun = 100)
	
	rxSetComputeContext( getTeradataComputeContext() ) 
	teradataExec <- rxExec(countToX, x=upperVal, timesToRun=100)
		
	localVecOut <- as.vector(unlist(localExec))
	teradataVecOut <- as.vector(unlist(teradataExec))
	checkEquals(localVecOut, teradataVecOut)
	
	# Pass in vector of arguments
	
	timesToRun <- 50
	upperVal <- 1:timesToRun
	rxSetComputeContext( getLocalComputeContext() )
    localExec <- rxExec(countToX, rxElemArg(x=upperVal), timesToRun = timesToRun)
	
	rxSetComputeContext( getTeradataComputeContext() ) 
	teradataExec <- rxExec(countToX, rxElemArg(x=upperVal), timesToRun=timesToRun)
	
	rxSetComputeContext( getLocalComputeContext() ) 
	localVecOut <- as.vector(unlist(localExec))
	teradataVecOut <- as.vector(unlist(localExec))
	checkEquals(localVecOut, teradataVecOut)
	
	# Just rxElemArg
	
    timesToRun <- 50
	upperVal <- 1:timesToRun
	rxSetComputeContext( getLocalComputeContext() )
    localExec <- rxExec(countToX, rxElemArg(x=upperVal))
	
	rxSetComputeContext( getTeradataComputeContext() ) 
	teradataExec <- rxExec(countToX, rxElemArg(x=upperVal))
	
	rxSetComputeContext( getLocalComputeContext() ) 
	localVecOut <- as.vector(unlist(localExec))
	teradataVecOut <- as.vector(unlist(localExec))
	checkEquals(localVecOut, teradataVecOut)	
	
	#testOutput <- rxExec(function(x,y,z){x+y+z}, x=rxElemArg(1:10), y=rxElemArg(rnorm(10)), 
		#z=rxElemArg(runif(10)), taskChunkSize=3)
		
	testOutput <- rxExec(rnorm, 5, timesToRun=10, taskChunkSize=10/4)
	checkEquals(4, length(testOutput))
	checkEquals(3, length(testOutput[[1]]))
	checkEquals(1, length(testOutput[[4]]))	

}




"test.teradata.HPC.rxExec.taskChunkSize" <- function()
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
	
	rxSetComputeContext( getTeradataComputeContext() ) 
	teradataExec <- rxExec(playCraps, timesToRun=100, taskChunkSize=25)
	checkEquals(length(localExec), length(teradataExec))
	checkEquals(length(localExec[[1]]), length(teradataExec[[1]]))
	
	rxSetComputeContext( getLocalComputeContext() ) 
	
}	

"test.teradata.HPC.foreach" <- function()
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
		
	library(doRSR)
    registerDoRSR()

	### Local computations   
    rxSetComputeContext( getLocalComputeContext() ) 	
	localForeach <- foreach(i=1:1000, .options.rsr=list(chunkSize=200)) %dopar% playCraps()
	localForeachTable <- table(unlist(localForeach))

	### In Teradata
	rxSetComputeContext( getTeradataComputeContext() ) 
	teraForeach <- foreach(i=1:1000, .options.rsr=list(chunkSize=200)) %dopar% playCraps()
	teraForeachTable <- table(unlist(teraForeach))
	checkEquals(sum(localForeachTable), 1000)
	checkEquals(sum(teraForeachTable), 1000)
	checkTrue(as.integer(teraForeachTable["Win"]) > 350)
	checkTrue(as.integer(teraForeachTable["Loss"]) > 350)
	
	rxSetComputeContext( getLocalComputeContext() ) 
	
}	

#############################################################################################
# Tests for running arbitrary SQL statement
#############################################################################################

 "test.teradata.helper.rxTeradtaSql" <- function()
{
	tdDataSource <- getAirDemoTeradataDS()
		
	if (rxTeradataTableExists("RevoTestDB.AirlineCopy", connectionString = tdDataSource@connectionString)) 
		rxTeradataDropTable("RevoTestDB.AirlineCopy", connectionString = tdDataSource@connectionString)
	
	# Make a copy of the table
	sqlStatement <- "CREATE TABLE RevoTestDB.AirlineCopy AS RevoTestDB.AirlineDemoSmallR WITH DATA"
	rxTeradataSql(sqlStatement = sqlStatement, connectionString = tdDataSource@connectionString)
	
	tdDataSourceNew <- RxTeradata(sqlQuery = "select * from RevoTestDB.AirlineCopy",
		connectionString = tdDataSource@connectionString)
	
	rxSetComputeContext( getTeradataComputeContext() )
	sumOutOld <- rxSummary(~ArrDelay + CRSDepTime, data = tdDataSource)
	sumOutNew <- rxSummary(~ArrDelay + CRSDepTime, data = tdDataSourceNew)
	checkEquals(sumOutOld$sDataFrame, sumOutNew$sDataFrame)
	
	# Delete the copied table
	rxTeradataDropTable(table = "RevoTestDB.AirlineCopy", connectionString = tdDataSource@connectionString)
	rxSetComputeContext( getLocalComputeContext() ) 
	
	res <- try(
		rxGetVarInfo(tdDataSourceNew)
		)
	checkTrue(inherits(res,"try-error"))
	  
}