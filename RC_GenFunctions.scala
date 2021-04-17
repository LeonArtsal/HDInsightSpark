/**       Last Modified :  06/04/2019
          Nklastra
          Available Functions Created 
          def toUcaseThenLower( x:String ) :String ={  x.toLowerCase.capitalize  }
          def toUcaseAll( x:String ) :String={ x.toUpperCase }
          def toWhatCase( args:String, func:String =>String ) : String ={
          def today(): String = {
          def yesterday(): String = {
          def getDateAsString(d: Date): String = {
          def convertStringToDate(s: String): Date = {
          def daysAgo(days: Int): String = {
          def todaywTime(): String = {
          def getDayOfWeek( pDateKey: String ): String = {
          def checkDateRange( nPeriodKeys:String, nStartPeriodKey:Date, nEndPeriodKey:Date ):Boolean={
          def groupN_Agg(df: DataFrame,  dMetrics: Map[String, String], cols: List[String] ): DataFrame ={
          def KeepSearch( A:List[Int], value:Int ) : Int = {
          def KeepSort(xs: Array[Int]): Array[Int] = {
**/
      
      


object RC_GenFunctions {

    /**  String Manipulation **/
    def toUcaseThenLower( x:String ) :String ={  x.toLowerCase.capitalize  }
    def toUcaseAll( x:String ) :String={ x.toUpperCase }
    def cleanup( s:String ):String={ s.replaceAll("[\\[\\]]", "") }
  
    def toWhatCase( args:String, func:String =>String ) : String ={
        if( args.length > 0 ){ func(args) }
        else{ "Please check parameter pass" }
    }
    
    import java.text.SimpleDateFormat
    import java.util.{Calendar, Date}
    import java.time.LocalDate
    import java.time.format.DateTimeFormatter
    
    private val dateFmt = "yyyy-MM-dd"
    private val dateFmtTime = "yyyy-MM-dd HH:MM:SS"

    /** Date Manipulation **/
    
    def today(): String = {
        val date = new Date
        val sdf = new SimpleDateFormat(dateFmt)
        sdf.format(date)
    }
 
    def yesterday(): String = {
        val calender = Calendar.getInstance()
        calender.roll(Calendar.DAY_OF_YEAR, -1)
        val sdf = new SimpleDateFormat(dateFmt)
        sdf.format(calender.getTime())
    }
  
    def CurrentYear() = LocalDate.now.getYear
    
    /* convert date to string */
    
    val DATE_FORMAT = "yyyyMMdd"
    def getDateAsString(d: Date): String = {
        val dateFormat = new SimpleDateFormat(DATE_FORMAT)
        dateFormat.format(d)
        // Sample Result :  Sat Mar 31 00:00:00 UTC 2018 ased on Date Format Passed 20180331

    }

    /* convert string to date  */
    
    def convertStringToDate(s: String): Date = {
        val dateFormat = new SimpleDateFormat(DATE_FORMAT)
        dateFormat.parse(s)
    }
    

    def daysAgo(days: Int): String = {
        val calender = Calendar.getInstance()
        calender.roll(Calendar.DAY_OF_YEAR, -days)
        val sdf = new SimpleDateFormat(dateFmt)
        sdf.format(calender.getTime())
    }
  
    def todaywTime(): String = {
        val date = new Date
        val sdf = new SimpleDateFormat(dateFmtTime)
        sdf.format(date)
    }
   
    def getDayOfWeek( pDateKey: String ): String = {
        var mDate = pDateKey
        if( mDate == null ){  mDate=today() }
        try{
                mDate = mDate.replace('-','/') 
                val df = DateTimeFormatter.ofPattern("yyyy/MM/dd")
                val dayOfWeek = LocalDate.parse( mDate, df).getDayOfWeek
                toWhatCase( dayOfWeek.toString, toUcaseThenLower  )
        } catch{
                case e: Exception => "Error ==> " + e  
        }
    }
    
    def checkDateRange( nPeriodKeys:String, nStartPeriodKey:Date, nEndPeriodKey:Date ):Boolean={
        val tLength = nPeriodKeys.length
        val xDate = nPeriodKeys.slice( ( tLength - 8 ), tLength )
        var retval:Boolean=false
        if( xDate.length==8 ){
            var y = convertStringToDate( xDate  )
            if( y.compareTo( nStartPeriodKey ) >= 0 && y.compareTo( nEndPeriodKey ) <= 0 ){
                        retval=true 
            }
        }
        return retval     
    }
    
    import org.apache.spark.sql._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._

     
    def groupN_Agg(df: DataFrame,  dMetrics: Map[String, String], cols: List[String] ): DataFrame ={
            val grouped = df.groupBy(cols.head, cols.tail: _*)
            val aggregated = grouped.agg(dMetrics)
            /* aggregated.show() */
            aggregated
    }
 
    def removeChar(s: String, T:Char) = s.map(c => if( c == T ) ' ' else c)
  
    def KeepSearch( A:List[Int], value:Int ) : Int = {
        var low:Int = 0
        var high:Int = A.length - 1
        var mid = -1
        while( low <= high ){
            mid = (low + high)/2
            if(  A(mid) == value ){
                return mid 
                }
            else if( value < A(mid) ) { //search the left half
                    high = mid - 1
                    }
            else{ 
                low = mid+1
            }
        }
       ( -1 )
   }
   
   def cleanArr( args_p:String ):Array[String]={
            args_p.replaceAll("[\\[\\]]","").replaceAll("\\s","").split(",")
    }
   
}



/* 
    def KeepSort(xs: Seq[Int]): Seq[Int] = {
            if (xs.length <= 1) xs
            else {
                    val pivot = xs(xs.length / 2)
                    Seq.concat(KeepSort(xs filter (pivot >)),  xs filter (pivot ==), KeepSort(xs filter (pivot <)))
                 }
    }

*/    

