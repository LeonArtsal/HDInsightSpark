/**
   Supported Module:  OSA 
	Number of machines in the cluster :  20 
        Number of Cores per Machine : 8 
        Memory Per Machine : 56 GB  
        Available Executors in the Cluster :   160
        Overhead for the OS/Machine - Minus 1 Core  =  140
        Net No. of Cores Available = 140 / 20 = 7 
        Number of Executor ( Fix to Max 3 to 5 )  = 7 / 3 => 2.5
        --number of executor 3  ( 3 executors per machine ) 
	Memory ( Heap ) 
        --executor-memory 
	56 GB -- Minus 1 for the OS Making it 55 Per Machine 
        56 / 3 = 18 
        -- we minus 2 for the yarn 
        18 - 2 = 16 Executor Memory 

    spark-shell --driver-memory 20G --executor-memory 16G --executor-cores 4  --num-executors 3 --master yarn --deploy-mode client
    .option("compression", "snappy")
    
**/


object SparkCommonUtils{
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.SparkContext
    import org.apache.spark.SparkConf
      
    val appName="Data Synthesizer Ver. 1"
    val sparkMasterURL = "--master client"
    val tempDir="file:///c:/temp/spark-warehouse/RC/"
    
    var spSession:SparkSession = null
    var spContext:SparkContext = null
  
    val conf = new SparkConf()
            .setAppName(appName)
            .set("spark.sql.shuffle.partitions","50")
            .set("spark.sql.orc.enabled","true")
    
    spContext =  SparkContext.getOrCreate(conf)
  
  
    spSession = SparkSession
        .builder()
        .appName(appName)
        .master(sparkMasterURL)
        .config("spark.sql.warehouse.dir", tempDir)
        .getOrCreate()
} 

