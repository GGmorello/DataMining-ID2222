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
// spark-submit --class "Main" --master local target/scala-2.12/discovery-of-frequent-itemsets-project_2.12-1.0.jar 3 1000 0.5
object Main {
  def main(args: Array[String]): Unit = {
    val dir = "data" // Should be some file on your system

    if (args.size != 3) {
      throw new Exception("Expected 3 arguments: 'K_PASSES SUPPORT CONFIDENCE'");
    }

    // how many tuple sizes we construct (passes)
    val tupleSize: Integer = args(0).toInt
    // support required for a tuple to be counted as a candidate pair
    // in each pass (occurrence support) - dataset contains 100k transactions
    val support: Integer = args(1).toInt
    val confidence: Double = args(2).toDouble

    val spark = SparkSession.builder.appName("Frequent Itemsets Application").master("local[*]").getOrCreate()
    val sparkContext = spark.sparkContext
    val transactionItemsetRdd = sparkContext.textFile(dir + "/dataset.dat")

    // TODO Refactor code to separate class?
    // (got serialization exception if using separate class)
    // val aPriori = new APriori()

    var lastCandidatePairs = new ArrayBuffer[(ArrayBuffer[String], Integer)]()
    for (k <- 1 to tupleSize) {
      val candidates: ArrayBuffer[(ArrayBuffer[String], Integer)] = performKPasses(lastCandidatePairs, transactionItemsetRdd, support, k)

      lastCandidatePairs = candidates

      if (k == 1) {
        println("Frequent itemsets - k: " + k + " support: " + support + " confidence: " + confidence);
        println("A-Priori result: " + candidates.size)
        println("Not printing all candidates or calculating association rules for pass k = 1");
      } else {
        val allBaskets = transactionItemsetRdd.map(_.split(" ").to[ArrayBuffer]).cache()

        val rules: ArrayBuffer[(ArrayBuffer[String], (ArrayBuffer[String], ArrayBuffer[String]), Double)] = associationRules(candidates, allBaskets, confidence)      

        println("Frequent itemsets - k: " + k + " support: " + support + " confidence: " + confidence);
        println("A-Priori result: " + candidates.size)
        candidates.sortBy(x => (x._2)).foreach(candidate => {
          println("tuple: (" + candidate._1.mkString(",") + "), support: " + candidate._2);
        });
        println();
        println("Association rules: ")
        rules.foreach(item => {
          val tuple = "tuple: (" + item._1.mkString(",") + ") ";
          print(tuple); // frequent itemset I
          val subsetA = item._2._1.mkString(",");
          print("rule: [" + subsetA + "] -> "); // subset A of I
          val subsetIA = item._2._2.mkString(",");
          print("[" + subsetIA + "]"); // subset A w/o items in I
          print("; confidence: " + item._3);
          println();
        });
      }
      
    }
    
  }
  
  def performKPasses(
    lastCandidatePairs: ArrayBuffer[(ArrayBuffer[String], Integer)],
    transactionItemsetRdd: RDD[String],
    support: Integer, ki: Integer): ArrayBuffer[(ArrayBuffer[String], Integer)] = {
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
        return filteredTuplesLk
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

  def associationRules(candidates: ArrayBuffer[(ArrayBuffer[String], Integer)], sets: RDD[ArrayBuffer[String]], c: Double): ArrayBuffer[(ArrayBuffer[String], (ArrayBuffer[String], ArrayBuffer[String]), Double)] = {
    
    val conf = new ArrayBuffer[(ArrayBuffer[String], (ArrayBuffer[String], ArrayBuffer[String]), Double)]

    for ((tuple, support) <- candidates) {

      var combs = new ArrayBuffer[ArrayBuffer[String]]
      for (l <- Range(1, tuple.length)){
        combs ++= tuple.combinations(l)
      }

      var pairs = new ArrayBuffer[(ArrayBuffer[String], ArrayBuffer[String])]
      for (x <- combs; y <- combs) {
        if ((x.intersect(y).length == 0 && x.union(y).length == tuple.length)){
          pairs.append((x, y))
        }
      }

      for (pair <- pairs){
        var num = support
        var den = sets.filter(s => (pair._1.toSet.subsetOf(s.toSet))).count
        conf.append((tuple, pair, num.toDouble / den))
      }
    }
    return conf.filter(_._3 > c)
  }
  
}
 