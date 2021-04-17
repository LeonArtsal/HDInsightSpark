/** 

Object Name :  RC_DataPipe_Aggregate - Populates WarehouseStaging 
last Modified Date :  06/13/2019 - Nklastra

Note :  How to avoid declaring nulls ? 
 
By using ->  None: Option[String], Some(), getOrElse()

class paramList{
   var SourceStorage=None:Option[String]
   var Retailer=None: Option[String]
   var Vendor=None: Option[String]
   var PeriodKey=None: Option[String] 
   var ReadStaging=None: Option[String] 
   var SavingLocationFullLoad=None: Option[String]
}

val u = new paramList()

.. then assign other values 
.. setter
u.SourceStorage = Some(" ... ")
u.Retailer = Some(" ... ")

.. gettter
val vSourceStorage = u.SourceStorage.getOrElse("<no storage assigned>")
vSourceStorage: String = wasb:// ... 


class User(email: String, password: String) {
    var firstName = None: Option[String]
    var lastName = None: Option[String]
}

**/


object RC_DataPipe_Aggregate{
        import java.io.IOException
        import java.io.FileNotFoundException
        import org.apache.spark.sql.DataFrame 
        import java.util.Calendar
        import org.apache.spark.sql.functions.{lit, max, row_number, coalesce, sum, count, trim, round, bround,when }
        import org.apache.spark.sql.types._
        import org.apache.spark.sql._

        val F = new RC_Constants()
        val genfuncs = RC_GenFunctions
        
        var SourceStorage:String=_
        var vRetailer:String=_
        var vVendor:String=_
        var vPeriodKey:String=_ 
        var vReadStaging:String=_ 
        var vSavingLocationFullLoad:String=_
        var vLoadType:String=_
            
        val spSession = SparkCommonUtils.spSession  
        spSession.conf.set("spark.sql.shuffle.partitions","12")
        spSession.conf.set("spark.sql.orc.enabled","true")
        spSession.conf.set("spark.dynamicAllocation.executorIdleTimeout","2m")
        spSession.conf.set("spark.dynamicAllocation.enabled","true")
        spSession.conf.set("spark.dynamicAllocation.maxExecutors","2000" )
                        
        import F.spSession.implicits._
   
        def MainFunc( sParams: String* )={
                if( sParams.size != 7 ) { F.info("Missing Parameters Passed" ) }
                else{
                    SourceStorage=sParams(0) 
                    vRetailer=sParams(1) 
                    vVendor=sParams(2)
                    vPeriodKey=sParams(3)
                    vReadStaging=sParams(4)
                    vSavingLocationFullLoad=sParams(5)
                    vLoadType=sParams(6)
                }        
                
                F.info("Step 1 -> Read Data From Sources, ( Source:( Transaction/Dim)  To WHStaging )" )
                
                val dfCalendar =F.open_DimCalendar( SourceStorage, vRetailer )
                 
                F.info( "Read Staging" + vReadStaging ) 
                // work on the group key making it a SEQ()
                val dfIRISMainSrcRaw = spSession.read.parquet( vReadStaging ).select(
                    $"RETAILER_KEY",
                    $"VENDOR_KEY",
                    $"PERIOD_KEY",
                    $"ITEM_KEY",
                    $"STORE_KEY",
                    $"OSA_TYPE_KEY",
                    round( $"TOTAL_UNITS",2 ).alias("TotalUnits"),
                    round( $"TOTAL_SALES",2 ).alias("TotalSales"),
                    round( $"LOST_UNITS",2 ).alias("LostUnits"),
                    round( $"LOST_SALES",2 ).alias("LostSales"),
                    round( $"RETAIL_ON_HAND",2 ).alias("RetailOnHand"),
                    round( $"RS_DEMAND",2 ).alias("RSDemand"),
                    round( $"RS_INVENTORY",2 ).alias("RSInventory"),
                    round( $"OSA_DURATION",2 ).alias("OSADuration")  )
                 
                   /*  Requirements : 
                
                    Retail On Hand	LastNonEmpty  // to discuss tomorrow 05/08/2019, sum ON spd last date on weekly
                    RS Inventory	LastNonEmpty  // to discuss tomorrow 05/08/2019  SUM ON sPD last date on weekly
                    OSA Duration	Average  // for phase 2

                    */
                
                                    
                    F.info("Step 2 --> Aggregate The Daily USING sum()" )
   
                    val GroupByList:List[String]=List("RETAILER_KEY","VENDOR_KEY","PERIOD_KEY","STORE_KEY","ITEM_KEY","OSA_TYPE_KEY")
                    val AVGColumns:List[String]=List("OSADuration")
                
                    /* filter the for SUM columns */
                    val DailyMetricSumList = dfIRISMainSrcRaw.columns.filter(  x => !GroupByList.contains( x )).filter( y => !AVGColumns.contains( y ) )
                
                
                    var DailySumExprs = DailyMetricSumList.map( ( _ -> "sum") ).toMap
                    val DailyAvgExprs = AVGColumns.map( ( _ -> "avg") ).toMap
                    val MetricsMap = DailySumExprs ++ DailyAvgExprs
                
                    F.info("Step 3 -->  Execute the Aggregation - This is the final daily all we need now is the TY and LY from the calendar")
                
                    val dfIRISDailyAggRaw = genfuncs.groupN_Agg( dfIRISMainSrcRaw,  MetricsMap, GroupByList)
                    
                    F.info("Step 4 -->  This is the IRISDaily AggRAW ")
                    F.info("Step 5 --> Include the Calendar, YearWeek and Monthly")

                              
                    val dfIRISDailyTYAggFnl = dfIRISDailyAggRaw.join( dfCalendar, Seq("PERIOD_KEY") ).select( 
                    $"RETAILER_KEY",
                    $"VENDOR_KEY",
                    $"PERIOD_KEY".cast("String"),
                    $"ITEM_KEY",
                    $"STORE_KEY",
                    $"OSA_TYPE_KEY",
                    $"sum(TotalUnits)".alias("TOTAL_UNITS_TY"),
                    $"sum(RSDemand)".alias("RS_DEMAND_TY"),
                    $"sum(LostSales)".alias("LOST_SALES_TY"),
                    $"sum(LostUnits)".alias("LOST_UNITS_TY"),
                    $"avg(OSADuration)".alias("OSA_DURATION_TY"),
                    $"sum(TotalSales)".alias("TOTAL_SALES_TY"),
                    $"sum(RetailOnHand)".alias("RETAIL_ON_HAND_TY"),
                    $"sum(RSInventory)".alias("RS_INVENTORY_TY"),
                    $"LY_PERIOD_KEY".cast("String"),
                    $"YEAR_WEEK".alias("TY_YEAR_WEEK"),
                    $"WEEK_ENDED" )       

                   
                    F.info("Step 6 Save the Daily File" )
                    
                    import java.io.IOException
         
                    println("Save here : " + vSavingLocationFullLoad )
                    println("yPeriodKey : " + vPeriodKey )
                    
                    // val jak = vPeriodKey.substring( vPeriodKey.indexOf("/")+1, vPeriodKey.length )

                    val upSertPath = s"${vSavingLocationFullLoad}${vPeriodKey}"
                    println( s"This is the path for saving : ${upSertPath}" )
                      
                    // spSession.conf.set("parquet.enable.summary-metadata", "false" )
                    val ComboCols = GroupByList ++ F.FinalMetrics
                    val GroupPartition:List[String]=List("VENDOR_KEY","PERIOD_KEY")
                    
                    import java.io.IOException
                    var mSuccess = "True" 
                    try{             
                        if( vLoadType == "FL" ){
                             /* Note:  Full Load .. a fresh write keeps appending if not will create the partition */
                             dfIRISDailyTYAggFnl.select( ComboCols.head, ComboCols.tail: _* ).write.format( "parquet" ).
                                  partitionBy( F.GroupPartitionD: _* ).mode("append").save( vSavingLocationFullLoad )
                        }else if( vLoadType=="IL"){     
                            /* Note: Incremental .. will do an Upsert bsed on partitionkey, Insert If New, Overwrite if exists */
                            F.info(s"Path To Save : ${upSertPath}")
                            spSession.conf.set("parquet.enable.summary-metadata", "false" )
                            dfIRISDailyTYAggFnl.select( ComboCols.head, ComboCols.tail: _* ).write.format("parquet").mode("overwrite").save( s"${upSertPath}" )
                        }
                       }catch{
                                case e: Exception => "Error ==> " + e
                                mSuccess = "False"
                       }finally{
                                spSession.sqlContext.clearCache()
                                F.info("Step 7 .. clear all cache and open memory " )   
                        }
                       ( mSuccess )             
                  }    
                    
    }

          