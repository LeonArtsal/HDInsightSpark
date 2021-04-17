/*
Last Modified Date : 06/04/2019  
nklastra
*/ 



trait Logger{
    val dDate = new java.util.Date()
    def log( msg: String )
    def info( msg: String ){ log("INFO : " + msg + " "+dDate) } 
    def warning( msg: String ){ log("WARN : " + msg  +" "+ dDate ) } 
    def severe( msg: String ){ log("SEVERE : " + msg +" "+ dDate ) }
}

trait FileOpening{
    import java.io.IOException
    import org.apache.spark.sql.types._
    import org.apache.spark.sql._
    import org.apache.spark.sql.DataFrame 
    val spContext = SparkCommonUtils.spContext
    val spSession = SparkCommonUtils.spSession  
    import spSession.implicits._
    
     def open_DimItem( blob_storage:String, mRetailer:String  ):DataFrame={
            val dfItem = spSession.read.format("parquet").load(s"${blob_storage}${mRetailer}/DIM_DATA/ITEM/")
            ( dfItem )         
     }
        
     def open_DimCalendar( blob_storage:String, mRetailer:String  ):DataFrame={
            val dfCalendar = spSession.read.format("parquet").load(s"${blob_storage}${mRetailer}/DIM_DATA/CALENDAR/")
            ( dfCalendar )         
     }

     def open_DimStore( blob_storage:String, mRetailer:String  ):DataFrame={
          val dfStore = spSession.read.format("parquet").load(s"${blob_storage}${mRetailer}/DIM_DATA/STORE/")
            ( dfStore )         
     }
        
     def open_DimOSAClass( blob_storage:String, mRetailer:String  ):DataFrame={
            val dfOSA_Class = spSession.read.format("parquet").load(s"${blob_storage}${mRetailer}/DIM_DATA/OSA_CLASSIFICATION/")
            ( dfOSA_Class )         
     }
     
    def SaveLogToFile( args_loc:String, arg_df:DataFrame, modetype:Boolean ){
            var modewrite="overwrite"
            val filetype="orc"
            if( modetype==false){ modewrite = "append" }
            try {
                arg_df.write.format( filetype ).mode( modewrite ).save(s"${args_loc}")
            } catch { 
                        case e: Exception => println( s"Error${e}" )
                    }
            finally { println( "Close all here .. " )  }
    }
}


trait ColumnListExpr{
    val FinalMetrics:List[String]=List("TOTAL_UNITS_TY","RS_DEMAND_TY", "LOST_SALES_TY","LOST_UNITS_TY","OSA_DURATION_TY","TOTAL_SALES_TY", "RETAIL_ON_HAND_TY","RS_INVENTORY_TY","LY_PERIOD_KEY","TY_YEAR_WEEK","WEEK_ENDED" )
    val FinalCommonMetrics:List[String]=List("TOTAL_UNITS_TY","TOTAL_SALES_TY","LOST_UNITS_TY","LOST_SALES_TY","RS_DEMAND_TY","RETAIL_ON_HAND_TY","RS_INVENTORY_TY","OSA_DURATION_TY","TOTAL_UNITS_LY","TOTAL_SALES_LY","LOST_UNITS_LY","LOST_SALES_LY","RS_DEMAND_LY","RETAIL_ON_HAND_LY","RS_INVENTORY_LY","OSA_DURATION_LY" )
    val DailyTYCols = Seq( "RETAILER_KEY","VENDOR_KEY","PERIOD_KEY","ITEM_KEY","STORE_KEY","OSA_TYPE_KEY","TOTAL_UNITS_TY","RS_DEMAND_TY","LOST_SALES_TY","LOST_UNITS_TY","OSA_DURATION_TY","TOTAL_SALES_TY","RETAIL_ON_HAND_TY","RS_INVENTORY_TY","LY_PERIOD_KEY","TY_YEAR_WEEK","WEEK_ENDED" )  
    val GroupPartitionD:List[String]=List("VENDOR_KEY","PERIOD_KEY")
    val GroupPartitionW:List[String]=List("VENDOR_KEY","WEEK_ENDED")
}


trait ColStructure{ 
    import org.apache.spark.sql.types._
    val ApplogSchema = List(
        StructField("JobId", IntegerType,true ),
        StructField("JobStep", StringType, true),
        StructField("JobDescription", StringType, true),
        StructField("StartTime", StringType, true),
        StructField("Status", StringType, true)
    )
    val someSchema = List(
        StructField("number", IntegerType, true),
        StructField("word", StringType, true)   
        )
}


class RC_Constants extends Logger with FileOpening with ColumnListExpr with ColStructure {
       import java.io.File
       def getVendors( vRetailer:String, vLocation:String, vType:String, vValidVendor:String  ):List[String]={
            import scala.sys.process._ 
            val lsResult = Seq("hadoop","fs","-ls",s"${vLocation}/${vRetailer}/${vType}/VENDOR_KEY=${vValidVendor}/").!!
            val lsSplitted= lsResult.split("\n").toList
            val removeHeaders = lsSplitted.filter( x => x.length > 30 )
            // = meaning vendor_key= where partition starts 
            val vendor_n_period_keys=  removeHeaders.map( x =>  x.substring( x.indexOf( "V") ) ).filter( x => x.contains("PERIOD_KEY") )
            return vendor_n_period_keys
       } 
       
      def getListOfSubDirectories(directoryName: String): Array[String] = {
            (new File(directoryName))
            .listFiles
            .filter(_.isDirectory)
            .map(_.getName)
        }
        
        override def log( msg:String ) { println( msg ) }
        
}


