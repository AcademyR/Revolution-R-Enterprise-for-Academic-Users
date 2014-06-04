##############################################################################
# RevoScaleR Data Import Example: On-Time Airline Performance 1987 - 2012
##############################################################################

# Airline on-time performance CSV files used for import come from
#
# http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236
# On-Time Performance Data from the Research and Innovative Technology Administration (RITA)
# in the Bureau of Transportation Statistics
#
# The following variables should be selected when downloading the monthly data:
#    Year, Month,DayofMonth,DayOfWeek, FlightDate, UniqueCarrier, TailNum, 
#    FlightNum, OriginAirportID, Origin, OriginState, 
#    DestAirportID, Dest, DestState, CRSDepTime, DepTime, 
#    DepDelay, DepDelayMinutes, DepDel15, DepartureDelayGroups, TaxiOut, WheelsOff,
#    WheelsOn, TaxiIn, CRSArrTime, ArrTime, ArrDelay, ArrDelayMinutes, 
#    ArrDel15, ArrivalDelayGroups, Cancelled, CancellationCode, Diverted, CRSElapsedTime,
#    ActualElapsedTime, AirTime, Flights, Distance, DistanceGroup, CarrierDelay, WeatherDelay,
#    NASDelay, ScurityDelay, LateAircraftDelay

############################################################################
#
# Importing the CSV files to a single XDF file
#
###########################################################################
# Specify the local locations of the CSV files and the xdf output file here

csvAirDataDir <- "C:/Revolution/Data/AirOnTimeCSV"
xdfOutFile <- "C:/Revolution/Data/AirOnTime87to12/AirOnTime87to12.xdf"

# Uncomment out following lines if you have only the 2012 data
#csvAirDataDir <- "C:/Revolution/Data/AirOnTimeCSV2012"
#xdfOutFile <- "C:/Revolution/Data/AirOnTime2012.xdf"

# Set the number of rows in each block to a maximum of 250,000 
blockSize <- 250000


# Variable names in CSV files are in caps with underscores.  
# Rename variables to names in on RITA web site, and specify import types (and factor
# levels where appropriate)
airlineColInfo <- list(
    YEAR = list(newName = "Year", type = "integer"),
    MONTH = list(newName = "Month", type = "integer"),
    DAY_OF_MONTH = list(newName = "DayofMonth", type = "integer"),
    DAY_OF_WEEK = list(newName = "DayOfWeek", type = "factor", 
        levels = as.character(1:7),
        newLevels = c("Mon", "Tues", "Wed", "Thur", "Fri", "Sat", "Sun")), 
	FL_DATE = list(newName = "FlightDate", type = "character"),
	UNIQUE_CARRIER = list(newName = "UniqueCarrier", type = "factor"),
	TAIL_NUM = list(newName = "TailNum", type = "factor"),
    FL_NUM = list(newName = "FlightNum", type = "factor"),	
    ORIGIN_AIRPORT_ID = list(newName = "OriginAirportID", type = "factor"),
	ORIGIN = list(newName = "Origin", type = "factor"),
    ORIGIN_CITY_NAME = list(newName = "OriginCityName", type = "factor"),	
    ORIGIN_STATE_ABR = list(newName = "OriginState", type = "factor"),	
    DEST_AIRPORT_ID = list(newName = "DestAirportID", type = "factor"),
	DEST = list(newName = "Dest", type = "factor"),
    DEST_CITY_NAME = list(newName = "DestCityName", type = "factor"),	
    DEST_STATE_ABR = list(newName = "DestState", type = "factor"),		
    CRS_DEP_TIME = list(newName = "CRSDepTime", type = "integer"),	
    DEP_TIME = list(newName = "DepTime", type = "integer"),
    DEP_DELAY = list(newName = "DepDelay", type = "integer"),
	DEP_DELAY_NEW = list(newName = "DepDelayMinutes", type = "integer"),
	DEP_DEL15 = list(newName = "DepDel15", type = "logical"),
    DEP_DELAY_GROUP = list(newName = "DepDelayGroups", type = "factor",
       levels = as.character(-2:12),
       newLevels = c("< -15", "-15 to -1","0 to 14", "15 to 29", "30 to 44",
        "45 to 59", "60 to 74", "75 to 89", "90 to 104", "105 to 119",
        "120 to 134", "135 to 149", "150 to 164", "165 to 179", ">= 180")),
	TAXI_OUT = list(newName = "TaxiOut", type =  "integer"),
    WHEELS_OFF = list(newName = "WheelsOff", type =  "integer"),	
	WHEELS_ON = list(newName = "WheelsOn", type =  "integer"),
	TAXI_IN = list(newName = "TaxiIn", type =  "integer"),
	CRS_ARR_TIME = list(newName = "CRSArrTime", type = "integer"),	
    ARR_TIME = list(newName = "ArrTime", type = "integer"),
    ARR_DELAY = list(newName = "ArrDelay", type = "integer"),
    ARR_DELAY_NEW = list(newName = "ArrDelayMinutes", type = "integer"),  
	ARR_DEL15 = list(newName = "ArrDel15", type = "logical"),
    ARR_DELAY_GROUP = list(newName = "ArrDelayGroups", type = "factor",
      levels = as.character(-2:12),
       newLevels = c("< -15", "-15 to -1","0 to 14", "15 to 29", "30 to 44",
        "45 to 59", "60 to 74", "75 to 89", "90 to 104", "105 to 119",
        "120 to 134", "135 to 149", "150 to 164", "165 to 179", ">= 180")),
    CANCELLED = list(newName = "Cancelled", type = "logical"),
    CANCELLATION_CODE = list(newName = "CancellationCode", type = "factor", 
        levels = c("NA","A","B","C","D"),	
	        newLevels = c("NA", "Carrier", "Weather", "NAS", "Security")),
	DIVERTED = list(newName = "Diverted", type = "logical"), 
	CRS_ELAPSED_TIME = list(newName = "CRSElapsedTime", type = "integer"),		
    ACTUAL_ELAPSED_TIME = list(newName = "ActualElapsedTime", type = "integer"),
    AIR_TIME = list(newName = "AirTime", type =  "integer"),
    FLIGHTS = list(newName = "Flights", type = "integer"),
    DISTANCE = list(newName = "Distance", type = "integer"),
    DISTANCE_GROUP = list(newName = "DistanceGroup", type = "factor",
     levels = as.character(1:11),
     newLevels = c("< 250", "250-499", "500-749", "750-999",
         "1000-1249", "1250-1499", "1500-1749", "1750-1999",
         "2000-2249", "2250-2499", ">= 2500")),
    CARRIER_DELAY = list(newName = "CarrierDelay", type = "integer"),
    WEATHER_DELAY = list(newName = "WeatherDelay", type = "integer"),
    NAS_DELAY = list(newName = "NASDelay", type = "integer"),
    SECURITY_DELAY = list(newName = "SecurityDelay", type = "integer"),
    LATE_AIRCRAFT_DELAY = list(newName = "LateAircraftDelay", type = "integer"))

varNames <- names(airlineColInfo)

csvFiles <- list.files(csvAirDataDir, pattern = "*.csv")
ConvertToDecimalTime <- function( tm ){(tm %/% 100) + (tm %% 100)/60}
			
append <- FALSE
numRowsToRead <- -1

# Import and append the monthly files
for (file in csvFiles)
{
    print(" *******************    file")
    print(file)
    inDataSource <- RxTextData(file.path(csvAirDataDir, file), 
		colInfo = airlineColInfo,
		varsToKeep = varNames,
		stringsAsFactors = TRUE)
    rxImport(inData = inDataSource,
             outFile = xdfOutFile,
             transforms = list(
                FlightDate = as.Date(FlightDate, format = "%Y-%m-%d"),
                CRSDepTime = ConvertToDecimalTimeFn(CRSDepTime),
                DepTime = ConvertToDecimalTimeFn(DepTime),
				WheelsOff = ConvertToDecimalTimeFn(WheelsOff),
				WheelsOn = ConvertToDecimalTimeFn(WheelsOn),				
				CRSArrTime = ConvertToDecimalTimeFn(CRSArrTime),
                ArrTime = ConvertToDecimalTimeFn(ArrTime),
                MonthsSince198710 = as.integer((Year-1987)*12 + Month - 10),
                DaysSince19871001 = as.integer(FlightDate - as.Date("1987-10-01", format = "%Y-%m-%d"))),
             transformObjects = list(ConvertToDecimalTimeFn = ConvertToDecimalTime),
             rowsPerRead = blockSize, 
             overwrite = TRUE,
             append = append,
			 numRows = numRowsToRead )  
        append <- TRUE
 }

rxGetInfo(xdfOutFile)
rxGetVarInfo(xdfOutFile)


