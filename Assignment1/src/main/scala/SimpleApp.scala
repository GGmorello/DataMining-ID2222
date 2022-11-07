import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {
    val logFile = "docs" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").master("local[4]").getOrCreate()
    
    val logData = spark.sparkContext.wholeTextFiles(logFile).cache()
    println(logData)
    spark.stop()
  }
}