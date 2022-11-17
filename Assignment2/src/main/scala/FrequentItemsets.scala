import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer
import scala.sys.process.processInternal
import org.apache.spark.rdd.RDD 
import scala.util.control.Breaks._

import scala.collection.mutable.HashMap
import scala.util.hashing.MurmurHash3
import org.apache.spark.sql.catalyst.expressions.In
import scala.util.Random

// 1. sbt package
// 2. spark-submit --class "Main" --master local target/scala-2.12/discovery-of-frequent-itemsets-project_2.12-1.0.jar

object Main {
  def main(args: Array[String]): Unit = {
    val dir = "data" // Should be some file on your system
    println("hello world")
  }
}