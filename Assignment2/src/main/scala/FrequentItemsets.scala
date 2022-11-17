import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import scala.sys.process.processInternal
import org.apache.spark.rdd.RDD 
import scala.util.control.Breaks._

import org.apache.spark.sql.catalyst.expressions.In
import scala.util.Random

// run the following commands in order
// sbt package
// spark-submit --class "Main" --master local target/scala-2.12/discovery-of-frequent-itemsets-project_2.12-1.0.jar
object Main {
  def main(args: Array[String]): Unit = {
    val dir = "data" // Should be some file on your system

    // how many tuple sizes we construct (passes)
    val k = 5
    // support required for a tuple to be counted as a candidate pair
    // in each pass (occurrence support) - dataset contains 100k transactions
    val support = 100

    val spark = SparkSession.builder.appName("Frequent Itemsets Application").master("local[*]").getOrCreate()
    val sparkContext = spark.sparkContext
    val transactionItemsetRdd = sparkContext.textFile(dir + "/dataset.dat")

    // TODO Refactor code to separate class?
    // (got serialization exception if using separate class)
    // val aPriori = new APriori()

    // transaction items ID's are always ordered
    val res: ArrayBuffer[(ArrayBuffer[String], Integer)] = performKPasses(sparkContext, transactionItemsetRdd, support, k)

    println("A-Priori result:")
    res.sortBy(x => (x._2)).foreach(println)
  }
  
  def performKPasses(
    sparkContext: SparkContext, // TODO Might not be needed
    transactionItemsetRdd: RDD[String],
    support: Integer, k: Integer): ArrayBuffer[(ArrayBuffer[String], Integer)] = {
      var lastCandidatePairs = new ArrayBuffer[(ArrayBuffer[String], Integer)]()
      for (ki <- Range(1, k)) {
        // we need to keep track of the single elements
        // from the last pass (ki - 1) to construct
        // new candidate pairs for the next iteration
        var singleItems: ArrayBuffer[String] = lastCandidatePairs.map({ case (k, sup) => {
          k
        }}).flatten

        println("single items: " + singleItems.size)
        println("last candidate pairs: " + lastCandidatePairs.size)

        // var parallel: RDD[String] = sparkContext.parallelize(singleItems)

        // this will create an entry for each row (transaction) in the RDD
        // entries: (itemset, supportOfItemset)
        val allTuplesCk: RDD[(ArrayBuffer[String], Integer)]= transactionItemsetRdd.flatMap(row => {
            createKItemsets(row, singleItems, ki)
        }).reduceByKey({ case(a, b) => {
          a + b
        }})

        // we already know support as an input parameter,
        // so no need to check for "allTuples.size" here
        val filteredTuplesLk: ArrayBuffer[(ArrayBuffer[String], Integer)] = allTuplesCk.filter({
          case(tuple, sup) => { 
            sup >= support
          }
        }).collect().to[ArrayBuffer]

        // we might run into a scenario where we no longer find
        // any candidate pairs, so we can just return the "greatest result"
        if (filteredTuplesLk.size == 0) {
          val max = allTuplesCk.collect().maxBy(a => a._2)
          println("stopping at iteration k = " + ki + "; no more candidate pairs found with support s = " + support)
          println("pair with highest support:")
          println(max)
          return lastCandidatePairs
        }
        lastCandidatePairs = filteredTuplesLk
      }
      return lastCandidatePairs
  }

  def createKItemsets(
      transactionItems: String,
      lastCandidatePairs: ArrayBuffer[String], // RDD[String]
      k: Integer): ArrayBuffer[(ArrayBuffer[String], Integer)] = {
        val items: ArrayBuffer[String] = transactionItems.split(" ").to[ArrayBuffer] // separate individual ID's

        val allTuples = new ArrayBuffer[(ArrayBuffer[String], Integer)]()

        // if this transaction doesn't contain >= k items,
        // we cannot construct k-tuples, so we just return
        if (items.size < k) {
          return allTuples
        }

        // we use arrays instead of tuples
        // since constructing tuples of arbitrary size
        // proved to be difficult (more straight forward with arrays)
        for (i <- Range(0, items.size - k)) {
            // TODO: Create itemsets based on last candidate pairs
            var itemset = new ArrayBuffer[String]()
            itemset += items(i)

            val recursed: ArrayBuffer[String] = recurseItems(items, i + 1, k - 1)
            itemset = itemset ++ recursed
            allTuples += ((itemset, 1)) 
        }

        return allTuples
    }

    // TODO: need to take into account recursive pairs
    // ex. [1, 2, 3, 4] for 2-tuples only makes pairs
    // (1, 2), (2, 3), (3, 4) - and not pairs such as (1, 3), (1, 4) and (2, 4)
    def recurseItems(items: ArrayBuffer[String], idx: Integer, rec: Integer): ArrayBuffer[String] = {
      var recursedItems: ArrayBuffer[String] = new ArrayBuffer[String]()
      if (rec == 0) {
        return recursedItems
      }
      recursedItems += items(idx)
      val recursed: ArrayBuffer[String] = recurseItems(items, idx + 1, rec - 1)
      recursedItems = recursedItems ++ recursed
      return recursedItems
    }
}

// class APriori {
//  TODO Move implementations into this class
// }