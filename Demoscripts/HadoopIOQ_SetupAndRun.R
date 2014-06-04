############################################################################################
# Copyright (C) 2013 Revolution Analytics, Inc
############################################################################################
#
# This script is provided as setup for running the Hadoop IOQ test suite, which
# can be used as part of your Installation/Operational Qualification protocol.
#
# In this script, specify setup information for your local client and hadoop cluster
# At the end of this script, the tests in the HadoopIOQ_Tests.R script (located in
# the inst/DemoScripts directory of the RevoScaleR installed package) are run.
# An HTML report is generated.
############################################################################################

require(RUnit)
require(RevoScaleR)

hadoopTestInfoList <- list(
    createHadoopData = TRUE,  # If TRUE, data is copied to Hadoop cluster & imported to xdf file as first step
    removeHadoopDataOnCompletion = FALSE, # Remove data from Hadoop cluster on completion
    localTestDataDir = "C:/Revolution/Data/Test", # Local directory for writing data; must exist
    myNameNode = "master.local",
    myPort = 8020,
    mySshUsername = "yourUserName",
    mySshHostname = "yourSshHost",
    mySshSwitches = "yourSShSwitches",
    myShareDirRoot = "/var/RevoShare",  # Share dir root on master node
    myHdfsShareDirRoot = "/user/RevoShare", # Share dir root in HDFS
    myConsoleOutput = FALSE, # Generate console output
    myHdfsNativeDir = "/tmp",  # Used for copying files from local machine to Hadoop cluster
    myHdfsAirDemoCsvSubdir = "IOQ/AirlineDemoSmallCsv", # Directory for file copied to HDFS
    myHdfsAirDemoXdfSubdir = "IOQ/AirlineDemoSmallXdf", # Directory for imported file
    myHdfsTestOuputSubdir = "IOQ/TempTestOutputXdf"     # Directory to place temporary test files
    )
   
hadoopTestInfoList$myShareDir <-
  paste(hadoopTestInfoList$myShareDirRoot, hadoopTestInfoList$mySshUsername, sep = "/")

# Location where data will be written on the Hadoop cluster:
hadoopTestInfoList$myHdfsShareDir <-
    paste(hadoopTestInfoList$myHdfsShareDirRoot, hadoopTestInfoList$mySshUsername, sep = "/")
    
hadoopTestInfoList$myHdfsFS <-
 RxHdfsFileSystem(hostName = hadoopTestInfoList$myNameNode, port = hadoopTestInfoList$myPort)    

# Run the tests
RevoScaleR:::rxuRunTests( 
    testDir = rxGetOption("demoScriptsDir"),
    testFileRegexp = "HadoopIOQ_Tests.R")

# If a test fails due to a temporary problem (such as with the connection to the cluster), 
# rerun the single tests. As an example:
 
#RevoScaleR:::rxuRunTests( 
    #testDir = rxGetOption("demoScriptsDir"),
    #testFuncRegexp = "test.hadoop.rxKmeans.args.outFile", 
    #testFileRegexp = "HadoopIOQ_Tests.R")