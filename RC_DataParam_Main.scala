/** should be saved in side the current
   last Modified Date :  06/26/2019 ( added job master log )
   Artsal
   What it does ?  Read the Json File Parameter, return each required variable  

Driver : 
startParam.main_read( myPath )

**/

class paramRead { 

  /** setter 
     How to avoid setting nulls ..  
     class paramList{
           var SourceStorage=None:Option[String]
           var Retailer=None: Option[String]
           var Vendor=None: Option[String]
           var PeriodKey=None: Option[String] 
           var ReadStaging=None: Option[String] 
           var SavingLocationFullLoad:None:Option[String]
            }

    val u = new paramList()
    u.SourceStorage = Some(" ... ")
    u.Retailer = Some(" ... ")
    ...
    .. Extract the value into string 
    val vSourceStorage = u.SourceStorage.getOrElse("<no storage assigned>")
    06/13/2019 -NKlastra

  **/
  
   val x = RC_GenFunctions
   val MAX_PARAMS=15
   var jSonPath:String = _
   var vRetailer:String = _
   var vVendors:String =_
   var vJobId:Int =_
   var vDimBaseUri:String = _
   var vFactBaseUri:String=_
   var vOSAFactBaseUri:String= _
   var vAggregateBaseUri:String=_
   var vLoadType:String=_
   var vSubject:String=_
   var vPeriodKeys:String=_
   var vCalendarId:String=_
   var vAggregationType:String=_
   var vAggregationLevel:String=_
   var vKafkaServer:String=_
   var vKafkaTopic:String=_
  
   /* need to add the kafka info   
    "kafkaServer": "DevZ1KAFKA001:9092,DevZ1KAFKA002:9092,DevZ1KAFKA003:9092",
    "kafkaTopic": "pacific.aggregate.status"
   */

   def setPath(p: String ) { jSonPath = p }
   def setRetailer( p: String ) { vRetailer = x.cleanup( p ) }
   def setVendors( p: String ) { vVendors =  x.cleanup( p ) }
   def setJobId( p:String ) { vJobId =x.cleanup( p ).toInt }
   def setFactBaseUri( p: String ) { vFactBaseUri = x.cleanup( p )  }
   def setOSAFactBaseUri( p: String ) { vOSAFactBaseUri = x.cleanup( p ) }
   def setDimBaseUri( p: String ) { vDimBaseUri = p }
   def setAggregateBaseUri( p:String ) { vAggregateBaseUri = x.cleanup( p ) }
   def setLoadType( p:String ) { vLoadType = x.cleanup(p) }
   def setSubject( p:String ) { vSubject = x.cleanup( p ) }
   def setPeriodKeys( p:String ) { vPeriodKeys =  p  }
   def setCalendarID( p:String ) { vCalendarId = x.cleanup( p ) }
   def setAggregationLevel( p:String ) { vAggregationLevel = x.cleanup( p ) }
   def setAggregationType( p:String ) { vAggregationType = x.cleanup( p ) }
   def setKafkaServer( p:String ) { vKafkaServer = x.cleanup( p ) }
   def setKafkaTopic( p:String ) { vKafkaTopic = x.cleanup( p ) }
  
}


object RC_DataParam_Main extends paramRead with Logger {
    import org.apache.spark.sql.DataFrame 
    import org.apache.spark.sql.types._
    import org.apache.spark.sql._
    
    def Read( jSonPath:String )={ 
        var mSuccess:Boolean=true    
        var dPath=jSonPath;
        val e:String=""
        var koleror:String=""
        val x = new paramRead()
        if( (jSonPath=="") || (jSonPath==null) ){
           dPath = "wasb://pnstoreauto-fs@pnstoreauto.blob.core.windows.net/Walgreens/retail_pacific.json"
        }

        info(s"Config file: ${dPath}")
        x.setPath( dPath ) 
        val spSession = SparkCommonUtils.spSession  
        import spSession.implicits._ 
        spSession.conf.set("spark.sql.shuffle.partitions","12")
        val paramDF = spSession.read.option("multiline","true").json( x.jSonPath )
        info( s"Numbers of passed parameters : ${paramDF.columns.size} => Max Expected ${MAX_PARAMS}")
               
        if( ( paramDF.count == 1 ) && ( paramDF.columns.size == x.MAX_PARAMS) ){
                    try{
                            x.setAggregateBaseUri( paramDF.select( $"aggregateBaseUri").rdd.take(1)(0).toString )
                            x.setAggregationLevel( paramDF.select( $"aggregationLevel").rdd.take(1)(0).toString )
                            x.setAggregationType( paramDF.select( $"aggregationType").rdd.take(1)(0).toString )
                            x.setRetailer( paramDF.select( $"retailer").rdd.take(1)(0).toString )
                            x.setVendors( paramDF.select( $"vendors").rdd.take(1)(0).toString )
                            x.setJobId( paramDF.select($"jobId").rdd.take(1)(0).toString )
                            x.setFactBaseUri( paramDF.select( $"factBaseUri" ).rdd.take(1)(0).toString )
                            x.setDimBaseUri(  paramDF.select( $"dimBaseUri" ).rdd.take(1)(0).toString )
                            x.setCalendarID( paramDF.select( $"calendarId" ).rdd.take(1)(0).toString )
                            x.setOSAFactBaseUri( paramDF.select( $"osaFactBaseURI" ).rdd.take(1)(0).toString )
                            x.setSubject( paramDF.select( $"subject" ).rdd.take(1)(0).toString )
                            x.setLoadType( paramDF.select( $"loadType" ).rdd.take(1)(0).toString )
                            x.setPeriodKeys( paramDF.select( $"dateRange" ).rdd.take(1)(0).toString )
                            x.setKafkaServer( paramDF.select( $"kafkaServer").take(1)(0).toString )
                            x.setKafkaTopic( paramDF.select( $"kafkaTopic").take(1)(0).toString )
                            
                    }catch{
                            /** save the logs here .. artsal 06072019 **/
                            case e: Exception => "Error ==> " + e  
                            warning("Error in extracting parameter " + e )
                            koleror = "Error in extracting parameter " + e
                            mSuccess=false
               
                    }
        } else { 
            info( "Nothing to get!")
            mSuccess=false            
        }
                
         
        // call a function that will pass these params 
       
        info( "Retailer to process : " +  x.vRetailer )
        info( "Vendors to process : " + x.vVendors )
        info( "Period Dates : " + x.vPeriodKeys )
        info( "Job ID : " + x.vJobId )
        info( "Aggregation Type : " + x.vAggregationType )
        info( "Aggregation Level : " + x.vAggregationLevel  )
        info( "Calendar Id: " + x.vCalendarId  )
        info( "DIM Base URI : " + x.vDimBaseUri  )
        info( "Fact Base URI : " + x.vFactBaseUri  )
        info( "OSA Fact Base URI : " + x.vOSAFactBaseUri  )
        info( "Aggregation Base URI : " + x.vAggregateBaseUri )
        info( "Subject Type : " + x.vSubject )
        info( "Load Type : " + x.vLoadType )
        info( "Kafka Server(s)" + x.vKafkaServer )
        info( "Kafka Topic" + x.vKafkaTopic )
        log( "End of process .. returns the value ... " + x.vRetailer )
       
        
        var mResults=""
        if( mSuccess==true){
              mResults = s"> ${x.vJobId},${x.vRetailer},${x.vVendors},${x.vLoadType} <"
        }else if( mSuccess==false ){
              mResults = koleror.substring( 0,100  ) 
        }
        /* is getter better ? */ 
        
        /** use the try{}catch{}  
            ** This is the Job Master Log ***          
            val vLogLocation ="wasbs://pacific@totalstore.blob.core.windows.net/Walgreens/JobMaster/"
            paramDF.write.format( "orc" ).mode( "append" ).save( vLogLocation )
        **/        
        
         
        ( x.vRetailer,  x.vVendors, x.vJobId, x.vFactBaseUri, x.vOSAFactBaseUri, x.vPeriodKeys, x.vSubject, x.vLoadType, x.vAggregateBaseUri, mSuccess, mResults )

    }
    
    override def log( msg:String ) { println( "RC_DataParam_Read ->" + msg ) }

}
