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
    val k = 3
    // support required for a tuple to be counted as a candidate pair
    // in each pass (occurrence support) - dataset contains 100k transactions
    val support = 1000
    val confidence = 0.8

    val spark = SparkSession.builder.appName("Frequent Itemsets Application").master("local[*]").getOrCreate()
    val sparkContext = spark.sparkContext
    val transactionItemsetRdd = sparkContext.textFile(dir + "/dataset.dat")

    // TODO Refactor code to separate class?
    // (got serialization exception if using separate class)
    // val aPriori = new APriori()

    // transaction items ID's are always ordered
    val candidates: ArrayBuffer[(ArrayBuffer[String], Integer)] = performKPasses(transactionItemsetRdd, support, k)

    println("A-Priori result: " + candidates.size)
    candidates.sortBy(x => (x._2)).foreach(println)

    val allBaskets = transactionItemsetRdd.map(_.split(" ").to[ArrayBuffer]).cache()

    val confident = associationRules(candidates, allBaskets, support, confidence)
    
    println("Confident pairs: ")
    confident.foreach(println)
  }
  
  def performKPasses(
    transactionItemsetRdd: RDD[String],
    support: Integer, k: Integer): ArrayBuffer[(ArrayBuffer[String], Integer)] = {
      var lastCandidatePairs = new ArrayBuffer[(ArrayBuffer[String], Integer)]()
      for (ki <- 1 to k) {
        // we need to keep track of the single elements
        // from the last pass (ki - 1) to construct
        // new candidate pairs for the next iteration
        var singleItems: ArrayBuffer[String] = lastCandidatePairs.map({ case (k, sup) => {
          k
        }}).flatten

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

        println("k pass = " + ki + " finished with Lk = " + filteredTuplesLk.size + " candidate pairs")
        println("candidate pairs/single items from previous pass: " + lastCandidatePairs.size + "/" + singleItems.size)

        // we might run into a scenario where we no longer find
        // any candidate pairs, so we can just return the "greatest result"
        if (filteredTuplesLk.size == 0) {
          val res = allTuplesCk.collect()
          println("stopping at iteration k = " + ki + "; no more candidate pairs found with support s = " + support)
          println("pair with highest support: ")
          val max = res.maxBy(a => a._2)
          println(max)
          return lastCandidatePairs
        }
        lastCandidatePairs = filteredTuplesLk
      }
      return lastCandidatePairs
  }

  def createKItemsets(
      transactionItems: String,
      lastCandidatePairs: ArrayBuffer[String],
      k: Integer): ArrayBuffer[(ArrayBuffer[String], Integer)] = {
        var items: ArrayBuffer[String] = transactionItems.split(" ").to[ArrayBuffer] // separate individual ID's

        // only use ID's from the previous candidate pairs
        // to improve memory usage
        var filteredItems = items.filter(item => {
          if (lastCandidatePairs.size > 0) {
            lastCandidatePairs contains item
          } else {
            true
          }
        })

        val allTuples = new ArrayBuffer[(ArrayBuffer[String], Integer)]()

        // if this transaction doesn't contain >= k items,
        // we cannot construct k-tuples, so we just return
        if (filteredItems.size < k) {
          return allTuples
        }

        // we use arrays instead of tuples
        // since constructing tuples of arbitrary size
        // proved to be difficult (more straight forward with arrays)
        val itemsets: ArrayBuffer[ArrayBuffer[String]] = createItemsets(filteredItems, k)
        itemsets.foreach(itemset => {
          allTuples += ((itemset, 1)) 
        })

        return allTuples
    }

    def createItemsets(items: ArrayBuffer[String], k: Integer): ArrayBuffer[ArrayBuffer[String]] = {
      return createItemsetsRec(items, 0, k)
    }

    // creates tuple pairs from items
    // ex. [1, 2, 3, 4] for 2-tuples makes pairs
    // (1, 2), (1, 3), (1, 4), (2, 3), (2, 4) and (3, 4)
    def createItemsetsRec(items: ArrayBuffer[String], idx: Integer, rem: Integer): ArrayBuffer[ArrayBuffer[String]] = {
      var allTuples: ArrayBuffer[ArrayBuffer[String]] = new ArrayBuffer[ArrayBuffer[String]]()
      if (rem == 0 || items.size < idx + rem) {
        return allTuples
      }

      // create pairs excluding current element
      // note that if idx + 1 + rem will be larger than the size
      // of our items, we will just append an empty array here
      val tuplesExcludingElement: ArrayBuffer[ArrayBuffer[String]] = createItemsetsRec(items, idx + 1, rem)
      allTuples = allTuples ++ tuplesExcludingElement

      if (rem == 1) {
        var arr = new ArrayBuffer[String]()
        arr += items(idx)
        allTuples += arr
        return allTuples
      }

      // create pairs including current element
      val tuplesIncludingElement: ArrayBuffer[ArrayBuffer[String]] = createItemsetsRec(items, idx + 1, rem - 1)
      // each entry in "recursed" will include a permutation for rem - 1 pairs,
      // so we construct a result for each of the entries here (including idx)
      tuplesIncludingElement.foreach(otherItems => {
        var tuple: ArrayBuffer[String] = new ArrayBuffer[String]()
        tuple += items(idx)
        tuple = tuple ++ otherItems
        allTuples += tuple
      })

      return allTuples
    }

  def associationRules(candidates: ArrayBuffer[(ArrayBuffer[String], Integer)], sets: RDD[ArrayBuffer[String]], s: Int, c: Double): ArrayBuffer[(ArrayBuffer[String], (ArrayBuffer[String], ArrayBuffer[String]), Double)] = {
    
    val canBaskets = candidates.map(_._1)

    val conf = new ArrayBuffer[(ArrayBuffer[String], (ArrayBuffer[String], ArrayBuffer[String]), Double)]

    for (tuple <- canBaskets) {

      var combs = new ArrayBuffer[ArrayBuffer[String]]
      for (l <- Range(1, tuple.length)){
        combs ++= tuple.combinations(l)
      }
      var pairs = new ArrayBuffer[(ArrayBuffer[String], ArrayBuffer[String])]
      for (x <- combs; y <- combs) {
        if (x.intersect(y).length == 0){
          pairs.append((x, y))
        }
      }
      for (pair <- pairs){
        var num = sets.filter( s => (pair._1.toSet.subsetOf(s.toSet) &&  pair._2.toSet.subsetOf(s.toSet))).count
        var den = sets.filter( s => (pair._1.toSet.subsetOf(s.toSet))).count
        conf.append((tuple, pair, num.toDouble / den))
      }
    }
    return conf.filter(_._3 > c)
  }
  
}
 