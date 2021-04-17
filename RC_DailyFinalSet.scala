/** daily aggregates - nklastra  
    last modified date :  06/15/2019 at4am
**/


object RC_DailyFinalSet{
       
        import java.io.IOException
        import java.io.FileNotFoundException
        import org.apache.spark.sql.DataFrame 
        import java.util.Calendar
        import org.apache.spark.sql.functions.{lit, max, row_number, coalesce, sum, count, trim, round, bround,when }
        import org.apache.spark.sql.types._
        import org.apache.spark.sql._
        
        /* should change the logic, should only pick the mperiod passed iterate on that period key should filter only that specific 
           period passing the dfIRISDailyTYAggFnl  filtered records 
           06/06/2019
        */
   
        def AggregateDailyJob( dfIRISDailyTYAggFnl:DataFrame, dfCalendar:DataFrame, vPeriodKeys:List[String], mLoadType:String ):Boolean={
               
                    val F = new RC_Constants()
                    val genfuncs = RC_GenFunctions
                    val spSession = SparkCommonUtils.spSession
                    val GroupDailyByList:List[String]=List("RETAILER_KEY","VENDOR_KEY","PERIOD_KEY","WEEK_ENDED","ITEM_KEY","STORE_KEY","OSA_TYPE_KEY")
                    val AddedItem:List[String]=List("LY_PERIOD_KEY")
    
                    
                    import spSession.implicits._        
                    spSession.conf.set("spark.sql.shuffle.partitions","80")
                    spSession.conf.set("spark.sql.orc.enabled","true")
                    spSession.conf.set("spark.dynamicAllocation.enabled","true")

                    val nCalendar = dfCalendar.select( $"LY_PERIOD_KEY", $"PERIOD_KEY".as("X_PERIOD_KEY"), $"WEEK_ENDED".as("X_WEEK_ENDED"))
                    
                    val dfIRISDailyLYBridge = dfIRISDailyTYAggFnl.select(                      
                        $"RETAILER_KEY",
                        $"VENDOR_KEY",
                        $"LY_PERIOD_KEY".cast("int").alias("PERIOD_KEY"),
                        $"ITEM_KEY",
                        $"STORE_KEY",
                        $"OSA_TYPE_KEY" )
                        
                    
                   F.info("Bridge Table" )
                   
                    val dfIRISDailyLYAggFnl1 = dfIRISDailyLYBridge.join( dfIRISDailyTYAggFnl, Seq(
                        "PERIOD_KEY", 
                        "RETAILER_KEY",
                        "VENDOR_KEY",
                        "ITEM_KEY",
                        "STORE_KEY",
                        "OSA_TYPE_KEY"),"full").
                        select(
                        $"PERIOD_KEY".alias("LY_PERIOD_KEY"),
                        $"RETAILER_KEY",
                        $"VENDOR_KEY",
                        $"ITEM_KEY",
                        $"STORE_KEY",
                        $"OSA_TYPE_KEY",
                        $"TOTAL_UNITS_TY".alias("TOTAL_UNITS_LY"),
                        $"RS_DEMAND_TY".alias("RS_DEMAND_LY"),
                        $"LOST_SALES_TY".alias("LOST_SALES_LY"),
                        $"LOST_UNITS_TY".alias("LOST_UNITS_LY"),
                        $"OSA_DURATION_TY".alias("OSA_DURATION_LY"),
                        $"TOTAL_SALES_TY".alias("TOTAL_SALES_LY"),
                        $"RETAIL_ON_HAND_TY".alias("RETAIL_ON_HAND_LY"),
                        $"RS_INVENTORY_TY".alias("RS_INVENTORY_LY") )
            
                    val dfIRISDailyLYAggFnl = dfIRISDailyLYAggFnl1.join( nCalendar, Seq( "LY_PERIOD_KEY" ) )
                    
                    F.info( "Now doing a full Outer " )
                    
                    import org.apache.spark.sql.functions._
                    
                    val dfIRISDailyAggFnl =  dfIRISDailyTYAggFnl.join( dfIRISDailyLYAggFnl, Seq(
                        "LY_PERIOD_KEY", 
                        "RETAILER_KEY",
                        "VENDOR_KEY",
                        "ITEM_KEY",
                        "STORE_KEY",
                        "OSA_TYPE_KEY"),"full").select (
                        $"RETAILER_KEY",
                        $"VENDOR_KEY",
                        $"PERIOD_KEY",
                        $"ITEM_KEY",
                        $"STORE_KEY",
                        $"OSA_TYPE_KEY",
                        $"WEEK_ENDED",
                        $"X_WEEK_ENDED",
                        $"X_PERIOD_KEY",
                        coalesce( $"TOTAL_UNITS_TY",lit(0.00) ).cast("double").alias("TOTAL_UNITS_TY"),
                        coalesce( $"TOTAL_SALES_TY",lit(0.00) ).cast("double").alias("TOTAL_SALES_TY"),
                        coalesce( $"LOST_UNITS_TY",lit(0.00) ).cast("double").alias("LOST_UNITS_TY"),
                        coalesce( $"LOST_SALES_TY",lit(0.00) ).cast("double").alias("LOST_SALES_TY"),
                        coalesce( $"RS_DEMAND_TY",lit(0.00) ).cast("double").alias("RS_DEMAND_TY"),
                        coalesce( $"RETAIL_ON_HAND_TY",lit(0.00) ).cast("double").alias("RETAIL_ON_HAND_TY"),
                        coalesce( $"RS_INVENTORY_TY",lit(0.00) ).cast("double").alias("RS_INVENTORY_TY"),
                        coalesce( $"OSA_DURATION_TY",lit(0.00) ).cast("double").alias("OSA_DURATION_TY"),
                        $"LY_PERIOD_KEY".cast("int").alias("LY_PERIOD_KEY"), 
                        coalesce($"TOTAL_UNITS_LY", lit(0.00) ).cast("double").alias("TOTAL_UNITS_LY"),
                        coalesce($"TOTAL_SALES_LY", lit(0.00) ).cast("double").alias("TOTAL_SALES_LY"),
                        coalesce($"LOST_UNITS_LY", lit(0.00) ).cast("double").alias("LOST_UNITS_LY"),
                        coalesce($"LOST_SALES_LY", lit(0.00) ).cast("double").alias("LOST_SALES_LY"),
                        coalesce($"RS_DEMAND_LY", lit(0.00) ).cast("double").alias("RS_DEMAND_LY"),
                        coalesce($"RETAIL_ON_HAND_LY", lit(0.00) ).cast("double").alias("RETAIL_ON_HAND_LY"),
                        coalesce($"RS_INVENTORY_LY", lit(0.00) ).cast("double").alias("RS_INVENTORY_LY"),
                        coalesce($"OSA_DURATION_LY", lit(0.00) ).cast("double").alias("OSA_DURATION_LY")).
                        withColumn("PERIOD_KEY", when( ($"PERIOD_KEY" === "" or $"PERIOD_KEY".isNull), $"X_PERIOD_KEY" ).
                        otherwise( $"PERIOD_KEY")  ).withColumn("WEEK_ENDED",when( ($"WEEK_ENDED" === "" or $"WEEK_ENDED".isNull), $"X_WEEK_ENDED" ).
                        otherwise( $"WEEK_ENDED")  ) 

                    F.log("Will now do the final call .. matching TY with LY and do the final daily, should remove the hardcoded part")
                    val vSavingReportDaily="wasbs://pacific@totalstore.blob.core.windows.net/Walgreens/WHReports/StoreItemDay/"
                    
                    val ComboCols = GroupDailyByList ++ F.FinalCommonMetrics ++ AddedItem
                    var mSuccess = true 
                    mLoadType match{
                        case "FL" | "FR"  => { 
                                    F.info("Step 15 Insert/Append the Daily Aggregate File.  NOTE:  This will overwrite/delete any existing data." )
                                    import java.io.IOException
                                
                                    try{               
                                        dfIRISDailyAggFnl.select ( ComboCols.head, ComboCols.tail: _* ).show(5)
                                        /**  NOTE : Uncomment the below if ready for another full load of full refresh of staging 
                                        dfIRISDailyAggFnl.select ( ComboCols.head, ComboCols.tail: _* ).write.format( "parquet" ).
                                            partitionBy( F.GroupPartitionD: _* ).mode( "overwrite" ).save( vSavingReportDaily )
                                        F.log("Success Saving of Daily TY and LY Data")
                                        **/
                                       }catch{
                                                case e: Exception => "Error ==> " + e
                                                mSuccess = false
                                       }finally{
                                                spSession.sqlContext.clearCache()
                                                F.info("Step 16 .. clear all cache and open memory " )   
                                       }
                        } 
                        case "IL" | "IR" => { 
                                println( "Incremental Refresh - This will do an UpSert on aggregated Datasets will bypass staging." )
                                vPeriodKeys.foreach{ X => println( X );
                                    val jak = X.substring( X.indexOf("/")+12, X.length )
                                    val newPath = s"${vSavingReportDaily}${X}"
                                    F.info(s"This is where it will save the records : ${newPath}")
                                    spSession.conf.set("parquet.enable.summary-metadata", "false" )
                                    dfIRISDailyAggFnl.select( ComboCols.head, ComboCols.tail: _* ).where( $"PERIOD_KEY"===jak ).show(2)
                                    dfIRISDailyAggFnl.select( ComboCols.head, ComboCols.tail: _* ).where( $"PERIOD_KEY"===jak ).write.format( "parquet" ).mode("overwrite" ).save(newPath) 
                                }
                                mSuccess=true    
                        }
                        case _ =>  { F.info("Unknown Load Type")
                                     mSuccess=false   
                                   }  
                    }
                    spSession.conf.set("parquet.enable.summary-metadata", "true" )
                    ( mSuccess ) 
        }    
}        

