/** 
   Last Modified Date :  06/17/2019/Nklastra at 1:51
   Required objects to run  : 
      1. SparkCommonUtils.scala   <<<---- Creates SPSession
      2. RC_GenFunctions.scala    <<<---- Contains All Generic Functions
      3. RC_DataParam_Main.Scala  <<<---- Reads JSON File and return extracted values
      4. RC_Constants.scala       <<<---- Methods related to Spark Dataframe
      5. RC_DailyFinalSet.scala   <<<---- Aggregates The Staging Into Daily
      6. RC_WeeklyFinalSet.scala  <<<---- Aggregates The Staing Into Weekly
      7. RC_DataPipe_Aggregate.scala   <<<---- Read Transaction Records and Dimension Tables to create the staging area
      8. RC_DataPipe_Main.scala  <<-- Main Entry Point
    
    Other useful spark commands : 
      spark-shell --driver-memory 15G --executor-memory 26G --executor-cores 5  --num-executors 10  --master yarn --deploy-mode client

    Not created yet ...     
      package com.rsi.pacific.datasynthesizer
      Summary Log Completed with Hive External,  Detailed Log - In progress
      Spark-Kafka Messaging - In Progress
  
**/


case class Storage(SourceStorage: String, OSAFactBaseUri: String, BlobAccount: String)

class LogList{
   var JobID=None:Option[String]
   /* var IO =None:Option[Storage] */
   var StepNo=None: Option[String]
   var StepDesc=None: Option[String]
   var StartTime=None: Option[String] 
   var Status=None: Option[String] 
}
    

object RC_DataPipe_Main{
        import org.apache.spark.sql.DataFrame 
        import org.apache.spark.sql.types._
        import org.apache.spark.sql._
        
        /* args can be the input param for the Json File Path */
        
        val genfunc = RC_GenFunctions
        val f = new RC_Constants()
        val u = new LogList()
        
        var mainLog1=Seq(Row( u.JobID, u.StepNo, u.StepDesc, u.StartTime, u.Status) ) 
        var mainLog2=Seq(Row( u.JobID, u.StepNo, u.StepDesc, u.StartTime, u.Status) )
        
        def main(args: Array[String]) = {
                var readyForDaily = false
                var readyForWeekly = false 
           
                var StartTime = RC_GenFunctions.todaywTime
                // Get Job Request Configuration File
                var configJsonPath = ""
                if (args.length > 0) {
                    configJsonPath = args(0)
                }

                f.info("Calls RC_DataParam_Main.Read - Reads JSON File and returns all required key values")
                val ( mRetailer, pVendor, mJobId , mFactBaseUri, mOSAFactBaseUri, mPeriodKey, 
                    mSubject, mLoadType, mAggregateBaseUri, mSuccess, mResults ) =  RC_DataParam_Main.Read(configJsonPath)
                
                val StartLog = Seq( Row( mJobId, "Step 1", "Application Starts .. Read and Parse the JSON File "+args , StartTime, "Success" ) )
                /* 3 years should be part of param read return as years of data to process */                
                var NoOfYears = genfunc.CurrentYear() - 3
                if( mSuccess==true ){              
                    val BlobAcctName= mFactBaseUri 
                    val SourceStorage:String =  mOSAFactBaseUri 
                    val mBlob_Account = mAggregateBaseUri
                    val warehouseStaging:String= "WHStagingFullLoad"
                    val warehouseDaily:String = "/WHReports/StoreItemDay/"
                    val warehouseWeekly:String = "/WHReports/StoreItemWeek/"
                    val vSavingLocationFullLoad=s"${mBlob_Account}${mRetailer}/${warehouseStaging}/"   
                    val vReadDailyAggLocation=s"${mBlob_Account}${mRetailer}${warehouseDaily}"
                    val vReadWeeklyAggLocation=s"${mBlob_Account}${mRetailer}${warehouseWeekly}"
                    val mVendors = List(pVendor.substring(13, pVendor.length-1 ))
                    val mPeriodKeys = genfunc.cleanup( mPeriodKey ).split(",")
                    
                    f.log("Success getting all parameters will now go to data loading to staging area ...")
                    mainLog1=Seq( Row( mJobId, "Step 2", s"JSON Parameter Values Checked : LOAD_TYPE = ${mLoadType}",  RC_GenFunctions.todaywTime, "Success" ) )
                    val LogDF = f.spSession.createDataFrame(  f.spContext.parallelize( mainLog1 ), StructType( (f.ApplogSchema)  ) )
                    LogDF.show(20,false)
                    
                    var nValidPeriods = List[String]()
                    f.info("WARNING :  LOAD TYPE -> ${mLoadType}")
                    /* check what type of loading is required */
                    mLoadType match { 
                        case "FL" => { f.info( "Full Load - Start from scratch. Insert/Append Both Staging/Aggregated Sets." )
                                    nValidPeriods = data_loading( mJobId, mRetailer, mVendors, mPeriodKeys, mSubject, SourceStorage, vSavingLocationFullLoad, mLoadType )
                                    nValidPeriods.foreach( x => println( "This is the nValidPeriods -> " + x ) )
                                    mainLog2=Seq( Row( mJobId,"Step 3", "Incremental Load to Staging Completed", RC_GenFunctions.todaywTime, "Success" ) )
                                    /* call the daily */
                                    readyForDaily=true
                                    }
                        case "IL" => { 
                                    f.info("Incremental Load - Will target UpSert partition keys in staging and aggregated sets.")
                                    nValidPeriods = data_loading( mJobId, mRetailer, mVendors, mPeriodKeys, mSubject, SourceStorage, vSavingLocationFullLoad,mLoadType )
                                    mainLog2=Seq( Row( mJobId,"Step 3", "Incremental Load to Staging Completed", RC_GenFunctions.todaywTime, "Success" ) )
                                    /* call the daily */
                                    nValidPeriods.foreach( x => println( "This is the nValidPeriods -> " + x ) )
                                    readyForDaily=true
                                    }
                        case "FR" => { println( "Full Refresh - This will Insert/Append aggregated Datasets, will not do staging." )
                                    f.log("Load to staging not required will go refresh daily and weekly aggregate datasets")
                                    var mainLog2 =  Seq( Row( mJobId, "Step 3","Load to staging not required will refresh daily/weekly aggregate datasets", RC_GenFunctions.todaywTime,"In-Progress" ) )
                                    readyForDaily=true 
                                    }
                        case "IR" => { println( "Incremental Refresh - This will do an UpSert on aggregated Datasets will bypass staging." )
                                    f.log("Load to staging not required will go refresh daily and weekly aggregate datasets")
                                    mainLog2 =  Seq( Row( mJobId, "Step 3","Load to staging not required will refresh daily/weekly aggregate datasets", RC_GenFunctions.todaywTime,"In-Progress" ) )
                                    readyForDaily=true 
                                    }
                        case _ => { f.info("Unknown Load Type!!")
                                    mainLog2 =  Seq(Row( mJobId, "Step 3", s"Unknown Load Type : => ${mLoadType}", RC_GenFunctions.todaywTime, "In-Progress" ))
                                  }
                        }
                    
                    /* If load to staging area is successful .. it will pass the readyForDaily 
                       Note:  Spark Context shuffling values and other params can be dynamically created based on data size calculations  
                              of from passed parameter                    
                    */
                            
                    if( readyForDaily==true ){
                            import java.util.Calendar
                            import org.apache.spark.sql.functions._
                            import org.apache.spark.sql.types._
                            import org.apache.spark.sql._
                            val spSession = SparkCommonUtils.spSession
                            import spSession.implicits._ 
                            spSession.conf.set("spark.sql.shuffle.partitions","80")
                            spSession.conf.set("spark.dynamicAllocation.enabled","true")
                            val StagingLocation =  vSavingLocationFullLoad
                            val DATECodeFilter = ($"PERIOD_KEY" < date_format(current_date(), "yyyyMMdd" ) && ( $"YEAR" > NoOfYears ) )
                            val TRANSACTIONFilter = ( $"PERIOD_KEY" >= mPeriodKeys(1) && $"PERIOD_KEY" <= mPeriodKeys(0) )
                            mainLog2 =  mainLog2 ++ Seq(Row( mJobId, "Step 4", s"Ready For Daily : => ${ mPeriodKeys(1) } to ${ mPeriodKeys(0) }", RC_GenFunctions.todaywTime, "In-Progress" ))
                            val dfCalendar = f.open_DimCalendar(  SourceStorage, mRetailer ).select($"PERIOD_KEY",$"LY_PERIOD_KEY",$"WEEK_ENDED",$"MONTH",$"YEAR").
                                                where( DATECodeFilter )
                            val dfDailyTYAggFnl = spSession.read.parquet( StagingLocation ).where( TRANSACTIONFilter ) 
                            f.info("RC_DailyFinalSet.AggregateDailyJob( dfIRISDailyTYAggFnl:DataFrame, dfCalendar:DataFrame ) ")
                            /* function that populates daily */    
                            readyForWeekly = RC_DailyFinalSet.AggregateDailyJob( dfDailyTYAggFnl, dfCalendar, nValidPeriods, mLoadType )
                                    /* if successful */
                                    if( readyForWeekly==true ){
                                        mainLog2 =  mainLog2 ++ Seq(Row( mJobId, "Step 5", s"Daily Aggregate Process : Done !!!", RC_GenFunctions.todaywTime, "Success" ))
                                        f.info( s"Should be same as below : ${vReadDailyAggLocation}" )
                                        f.info( "wasbs://pacific@totalstore.blob.core.windows.net/Walgreens/WHReports/StoreItemDay/" )
                                        /* should only read what is needed with the Transaction Filter*/
                                        val dfIRISDailyAggFnl_Filtered = spSession.read.parquet( vReadDailyAggLocation ).where( TRANSACTIONFilter )
                                        f.info("This is the weekly read after the daily process .. ")
                                        /* function that populates weekly */
                                        mainLog2 =  mainLog2 ++ Seq(Row( mJobId, "Step 6", s"Ready for Weekly Aggregates", RC_GenFunctions.todaywTime, "In-Progress" ))
                                        var xRes:String="Failed"                            
                                        val mResult = RC_WeeklyFinalSet.AggregateWeeklyJob( dfIRISDailyAggFnl_Filtered, dfCalendar, mLoadType, vReadWeeklyAggLocation )
                                        if( mResult==true ){ xRes = "Success" }
                                        mainLog2 =  mainLog2 ++ Seq(Row( mJobId, "Step 7", s"Weekly Aggregate Done !!!", RC_GenFunctions.todaywTime, xRes ))
                            
                                        f.info( "RC_WeeklyFinalSet.AggregateWeeklyJob( dfDailyTYAggFnl, dfCalendar" )
                                    }
                    }
                }else{
                   f.info(s"Error getting the parameter values from JSON File : ${mResults}")
                   mainLog1 =  Seq( Row( mJobId, "Step 2",s"WARNING :  ${mResults}", RC_GenFunctions.todaywTime, "Failed" ) )
                   val LogDF = f.spSession.createDataFrame(  f.spContext.parallelize( mainLog1 ), StructType( (f.ApplogSchema)  ) )
                   LogDF.show( 20, false )
                }
                
                
                f.log("Save the logs here .. ")
                val allLogs = StartLog ++ mainLog1 ++ mainLog2
              
                f.info( mainLog1.mkString(" ** ") )
                f.info( mainLog2.mkString(" ** ") )
             
                
                val LogDF = f.spSession.createDataFrame(  f.spContext.parallelize( allLogs ), StructType( (f.ApplogSchema)  ) )
                LogDF.show(20,false)
                
                val vLogLocation ="wasbs://pacific@totalstore.blob.core.windows.net/Walgreens/JobLogs/"
                LogDF.write.format( "orc" ).mode( "append" ).save( vLogLocation )
                
                /*
                INFO : [888,Step 1,Program Starts .. read and parse the JSON File.  JSON Location if passed -> null,2019-06-24 08:06:504,Success] Mon Jun 24 08:44:32 UTC 2019
                +-----+-------+--------------------------------------------------------------------------------+--------------------+-------+
                |JobId|JobStep|JobDescription                                                                  |StartTime           |Status |
                +-----+-------+--------------------------------------------------------------------------------+--------------------+-------+
                |888  |Step 1 |Program Starts .. read and parse the JSON File.  JSON Location if passed -> null|2019-06-24 08:06:504|Success|
                +-----+-------+--------------------------------------------------------------------------------+--------------------+-------+

                val vLogLocation ="wasbs://pacific@totalstore.blob.core.windows.net/Walgreens/JobLogs/"
                LogDF.write.format( "orc" ).mode( "append" ).save( vLogLocation )
                */
         }
        
        
        def data_loading( mJobId:Int, mRetailer:String, mValidVendor:List[String], mPeriodKeys:Array[String], 
                      subject:String, SourceStorage:String, vSavingLocationFullLoad:String, mLoadType:String ):List[String] = {
                val f = new RC_Constants()
                var mStatus="Success"
                var aLog =  Seq( Row( mJobId,"Step 1", "prepare rdd sources read from blob", RC_GenFunctions.todaywTime, mStatus ) )
                val xsize = mValidVendor.size
                var y = 0
                var iterNo = 3
                var bLog = Seq(  Row( mJobId, "Step 2", "Kick off from blob", RC_GenFunctions.todaywTime, mStatus)  )
                var FinalLogs1 = aLog.union( bLog ) 
              
                var newVendors = List[String]()
                
                f.info( "Period Key to process => ${mPeriodKeys(1)} - ${mPeriodKeys(0)}" )
                
                while( y < xsize ){
                
                        f.info( "Retailer " + mRetailer )
                        f.info( "SourceStorage " + SourceStorage )
                        f.info( "Subject " + subject ) 
                        f.info( "mValidVendor " +  mValidVendor(y) )
                        f.info( "Start Date -> " + mPeriodKeys(1) )
                        f.info( "End Date -> " + mPeriodKeys(0) )
                         
                        val mValidPaths = f.getVendors( mRetailer,  SourceStorage, subject, mValidVendor(y) )
                        val nStartPeriodKey = genfunc.convertStringToDate( mPeriodKeys(1) )
                        val nEndPeriodKey = genfunc.convertStringToDate( mPeriodKeys(0) ) 
                         
                        newVendors = mValidPaths.filter( nPeriodKeys => genfunc.checkDateRange( nPeriodKeys, nStartPeriodKey, nEndPeriodKey ) == true  )
                        
                        f.info("Make sure you do the application log her to see if there are invalid vendors on list")
                
                        /* meaning it found the directory location of each vendor, it will now do the iteration */
                        /* should only process the valid period keys based on the passed parameter */
            
                        var cLog = Seq( Row( mJobId, "Step "+iterNo, s"Iterations for : ${newVendors}", RC_GenFunctions.todaywTime, mStatus ) )
                        iterNo+=1
                        FinalLogs1 = FinalLogs1.union( cLog )
                        
                        newVendors.foreach{ yPeriodKey  => 
                            val ReadLocation = s"${SourceStorage}${mRetailer}/${subject}/${yPeriodKey}";
                            println( ReadLocation )
                            println( "Vendor Key Being Process .. " + mValidVendor(y) )
                            println( "Period Key : " + yPeriodKey )
                            f.info("Inside RC_DataPipe_Aggregate.ReadAndProcess_DataToStaging")
                            /* Load Type is a key factor in saving the data */
                            RC_DataPipe_Aggregate.MainFunc( SourceStorage, mRetailer, mValidVendor(y), yPeriodKey, ReadLocation, vSavingLocationFullLoad, mLoadType )
                        }
                    y+=1 
                }
                ( newVendors )
        }
  
}  


