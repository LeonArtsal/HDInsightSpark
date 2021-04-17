/** Date Last Modifed :  06/07/2019 at 5:39pm
    NKlastra
**/


object RC_WeeklyFinalSet{
       
        import java.io.IOException
        import java.io.FileNotFoundException
        import org.apache.spark.sql.DataFrame 
        import java.util.Calendar
        import org.apache.spark.sql.functions.{col, lit, max, row_number, coalesce, sum, count, trim, round, bround,when }
        import org.apache.spark.sql.types._
        import org.apache.spark.sql._
        val F = new RC_Constants()
        val genfuncs = RC_GenFunctions
                    
        val spSession = SparkCommonUtils.spSession
        import spSession.implicits._   
                    
        spSession.conf.set("spark.sql.shuffle.partitions","80")
        spSession.conf.set("spark.sql.orc.enabled","true")
        spSession.conf.set("spark.dynamicAllocation.enabled","true")

        /* Get Inventory Records */
        def getLastAvailableDVal( dfIRISDailyAggFnl:DataFrame, nMetric_Name:List[String] ):DataFrame ={
                  F.info("Step 1 -> get only records that are not zero" ) 
                  val dfDailyRecordsWithInventory = dfIRISDailyAggFnl.select( 
                    $"RETAILER_KEY",
                    $"VENDOR_KEY",
                    $"ITEM_KEY",
                    $"STORE_KEY",
                    $"OSA_TYPE_KEY",
                    $"WEEK_ENDED",
                    $"PERIOD_KEY",
                    col( nMetric_Name(0) ),
                    col( nMetric_Name(1) ) ).filter( col(nMetric_Name(0)) > 0 ).sort( $"ITEM_KEY", $"STORE_KEY", $"OSA_TYPE_KEY", $"WEEK_ENDED", $"PERIOD_KEY") 
                    F.info("Step 2 -->  now you can get the max period since you are sure it got a value not zero, make the group by list a list type later on ")
                    val dfMaxPeriodKeyWithInventory = dfDailyRecordsWithInventory.groupBy( 
                    $"RETAILER_KEY",
                    $"VENDOR_KEY",
                    $"ITEM_KEY",
                    $"STORE_KEY",
                    $"OSA_TYPE_KEY",
                    $"WEEK_ENDED" ).agg( max( $"PERIOD_KEY" ).as("PERIOD_KEY") ) 
                    F.info("Step 3 -->  now i can get back and get the value of this max period key which could NOT be a zero") 
                    val dfWeeklyInventory = dfMaxPeriodKeyWithInventory.join( dfDailyRecordsWithInventory, Seq( "RETAILER_KEY",
                    "VENDOR_KEY",
                    "ITEM_KEY",
                    "STORE_KEY",
                    "OSA_TYPE_KEY",
                    "WEEK_ENDED",
                    "PERIOD_KEY")).select( $"RETAILER_KEY", $"VENDOR_KEY", $"STORE_KEY", $"ITEM_KEY", $"OSA_TYPE_KEY", $"WEEK_ENDED", col(nMetric_Name(0)), col(nMetric_Name(1)) )
                    ( dfWeeklyInventory )          
        }
      

        /* starts weekly aggregate */
        def AggregateWeeklyJob( dfIRISDailyAggFnl: DataFrame, dfCalendar:DataFrame, mLoadType:String, vSavingReportWeekly:String ):Boolean={

                    F.info("Step 1 ---> Declare the variables ")
                    var mSuccess:Boolean=true
                    val GroupByWeekList:List[String]=List("RETAILER_KEY","VENDOR_KEY","WEEK_ENDED","STORE_KEY","ITEM_KEY","OSA_TYPE_KEY")
                    val AddedItem:List[String]=List("X_WEEK_ENDED")
                    val NotNeededAnymore:List[String]=List("PERIOD_KEY","LY_PERIOD_KEY")
                    val AVGColumns:List[String]=List("OSA_DURATION_TY","OSA_DURATION_LY")
                    val nMetric_Name1:List[String] = List("RS_INVENTORY_TY","RS_INVENTORY_LY")
                    val nMetric_Name2:List[String] = List("RETAIL_ON_HAND_TY","RETAIL_ON_HAND_LY")
                   
                    val UDFColumns:List[String]= nMetric_Name1 ++ nMetric_Name2
                    
                    F.info("Step 2 --> Aggregate The Daily USING sum()" )
   
                    val WeeklyMetricSumList = dfIRISDailyAggFnl.columns.filter(  x => !GroupByWeekList.contains( x )).filter( y =>  (!AVGColumns.contains( y ) 
                             && !NotNeededAnymore.contains( y ) && !UDFColumns.contains( y )  ) )
                             
                    var WeeklySumExprs = WeeklyMetricSumList.map( ( _ -> "sum") ).toMap
                    val WeeklyAvgExprs = AVGColumns.map( ( _ -> "avg") ).toMap
                    val MetricsMap = WeeklySumExprs ++ WeeklyAvgExprs
                
                    F.info("Step 3 -->  Execute the Aggregation - This is the first stage weekly just for SUM() and AVG() ")
                    val dfIRISWeeklyyAggRaw = genfuncs.groupN_Agg( dfIRISDailyAggFnl,  MetricsMap, GroupByWeekList)
                 
                    F.info("Step 4 --> needs to get formatted, if needed put the cast() here ..  ")
                    
                    val dfIRISWeeklyFormatted = dfIRISWeeklyyAggRaw.select(
                                                        $"RETAILER_KEY",
                                                        $"VENDOR_KEY",
                                                        $"WEEK_ENDED",
                                                        $"STORE_KEY",
                                                        $"ITEM_KEY",
                                                        $"OSA_TYPE_KEY",
                                                        $"sum(LOST_SALES_TY)".as("LOST_SALES_TY"),
                                                        $"sum(RS_DEMAND_TY)".as("RS_DEMAND_TY"),
                                                        $"avg(OSA_DURATION_LY)".as("OSA_DURATION_LY"),
                                                        $"sum(TOTAL_SALES_TY)".as("TOTAL_SALES_TY"),
                                                        $"sum(LOST_SALES_LY)".as("LOST_SALES_LY"),
                                                        $"avg(OSA_DURATION_TY)".as("OSA_DURATION_TY"),
                                                        $"sum(TOTAL_UNITS_TY)".as("TOTAL_UNITS_TY"),
                                                        $"sum(TOTAL_UNITS_LY)".as("TOTAL_UNITS_LY"),
                                                        $"sum(LOST_UNITS_TY)".as("LOST_UNITS_TY"),
                                                        $"sum(LOST_UNITS_LY)".as("LOST_UNITS_LY"),
                                                        $"sum(TOTAL_SALES_LY)".as("TOTAL_SALES_LY"),
                                                        $"sum(RS_DEMAND_LY)".as("RS_DEMAND_LY") )
                                                          
             
                    F.info("Step 4 -->  You can now call the UDF for getting the Lastvalue of Inventory with the period key range, passing the Raw Daily not the weekly formatted") 
             
                      
                    val dfIrisWeekly_One =  getLastAvailableDVal( dfIRISDailyAggFnl, nMetric_Name1 )
                    val dfIrisWeekly_Two =  getLastAvailableDVal( dfIRISDailyAggFnl, nMetric_Name2 )
                    
                    F.info("Step 5 -->  now join the three tables dfIRISWeeklyFormatted, dfIrisWeekly_One and DFIrisWeek_two")
                                   
                    val dfFinalWeekly = dfIRISWeeklyFormatted.join( dfIrisWeekly_One, Seq( "RETAILER_KEY",
                                                        "VENDOR_KEY",
                                                        "WEEK_ENDED",
                                                        "STORE_KEY",
                                                        "ITEM_KEY",
                                                        "OSA_TYPE_KEY"),"left").join( dfIrisWeekly_Two, Seq( "RETAILER_KEY",
                                                        "VENDOR_KEY",
                                                        "WEEK_ENDED",
                                                        "STORE_KEY",
                                                        "ITEM_KEY",
                                                        "OSA_TYPE_KEY"),"left").select(
                                                        $"RETAILER_KEY",
                                                        $"VENDOR_KEY",
                                                        $"WEEK_ENDED", 
                                                        $"WEEK_ENDED".alias("X_WEEK_ENDED"),
                                                        $"STORE_KEY",
                                                        $"ITEM_KEY",
                                                        $"OSA_TYPE_KEY",
                                                        $"LOST_SALES_TY", 
                                                        $"RS_DEMAND_TY", 
                                                        $"OSA_DURATION_LY", 
                                                        $"TOTAL_SALES_TY",
                                                        $"LOST_SALES_LY", 
                                                        $"OSA_DURATION_TY", 
                                                        $"TOTAL_UNITS_TY", 
                                                        $"TOTAL_UNITS_LY",
                                                        $"LOST_UNITS_TY", 
                                                        $"LOST_UNITS_LY", 
                                                        $"TOTAL_SALES_LY", 
                                                        $"RS_DEMAND_LY", 
                                                        $"RS_INVENTORY_TY",
                                                        $"RS_INVENTORY_LY", 
                                                        $"RETAIL_ON_HAND_TY",
                                                        $"RETAIL_ON_HAND_LY") 
                                                        
                    
                    F.info( "Step 6 -->  If all is good .. save them to the Weekly Storage " )

                    val ComboCols = GroupByWeekList ++ F.FinalCommonMetrics ++ AddedItem
                    
                    F.info("Step 6 Save the Weekly File" )
                    /** if cast is needed -> .cast("double").as("RETAIL_ON_HAND_TY") **/
                    mLoadType match{
                            case "FL" | "FR"  => { 
                                    import java.io.IOException
                                    try{
                                        dfFinalWeekly.select ( ComboCols.head, ComboCols.tail: _* ).write.format( "parquet" )
                                            .partitionBy( F.GroupPartitionW: _* ).mode( "overwrite" ).save( vSavingReportWeekly )
                                            
                                        F.log("Success Saving of Weekly TY and LY Data with Running Inventory")
                                       }catch{
                                                case e: Exception => "Error ==> " + e  
                                                mSuccess=false
                                       }finally{
                                                spSession.sqlContext.clearCache()
                                                F.info("Step 10 .. clear all cache and open memory " )   
                                       }
                            }
                            case "IL" | "IR" => { 
                                F.info("Incremental Refresh - This will do an UpSert on aggregated weekly Datasets")
                                /* GET THE VENDOR KEY AND WEEK ENDED */
                                val xWeekEnded = dfFinalWeekly.select($"VENDOR_KEY",$"WEEK_ENDED").distinct.rdd
                                xWeekEnded.take( xWeekEnded.count.toInt  ).foreach{ x => println( s"Vendor =>  ${x(0)} WeekEnded => ${x(1)}" ) 
                                        val newPath = s"${vSavingReportWeekly}VENDOR_KEY=${x(0)}/WEEK_ENDED=${x(1)}"
                                        println( "Saving Directory -> " + newPath )
                                        try{
                                             spSession.conf.set("parquet.enable.summary-metadata", "false" )
                                            dfFinalWeekly.select ( ComboCols.head, ComboCols.tail: _* ).write.format( "parquet" ).mode( "overwrite" ).save( newPath )
                                            F.log("Success Saving of Weekly Aggregate UpSert")
                                        }catch{
                                                case e: Exception => "Error ==> " + e  
                                                mSuccess=false
                                        }finally{
                                                spSession.sqlContext.clearCache()
                                                spSession.conf.set("parquet.enable.summary-metadata", "true" )
                                                F.info("Step 10 .. clear all cache and open memory " )   
                                        }
                                }
                                mSuccess=true    
                            }
                            case _ => { F.info("Unknown Load Type")
                                     mSuccess=false   
                            }  
                    }
                    ( mSuccess ) 
         }                               
    }     
                   
                   
           