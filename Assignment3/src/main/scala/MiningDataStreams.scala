import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.sys.process.processInternal
import org.apache.spark.rdd.RDD 
import scala.util.control.Breaks._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

// import org.apache.spark.implicits._

import org.apache.spark.sql.catalyst.expressions.In
import scala.util.Random

// run the following commands in order
// sbt package
// spark-submit --class "Main" --master local target/scala-2.12/mining-data-streams-project_2.12-1.0.jar 1000
object Main {
  def main(args: Array[String]): Unit = {
    val rand = new scala.util.Random
    val dir = "data" // Should be some file on your system
    val spark = SparkSession.builder.appName("Frequent Itemsets Application").master("local[*]").getOrCreate()
    val sparkContext = spark.sparkContext
    val rawGraph = sparkContext.textFile(dir + "/edges_and_op.txt").cache()

    val graph = rawGraph.map(_.split(" ")).map{case Array(f1,f2,f3) => (f1.toInt, f2.toInt, f3)}.collect.toArray
    val M = args(0).toInt
    var t: Long = 0
    var s = 0
    var di = 0
    var d0 = 0
    val S = new ArrayBuffer[(Int, Int)]

    var adjList = scala.collection.mutable.Map[Int, Set[Int]]()

    var globalCounter = 0
    var localCounters = scala.collection.mutable.Map[Int, Int]()
    
    var estimation = BigDecimal(0.0)

    for ((u,v,op) <- graph) {
      t += 1
      if (op == "+") {
        s += 1
        if (sampleEdge((u,v), op, S, adjList)) {
          updateCounters(op, (u,v), S, adjList)
        }
      } else if (op == "-"){
        s -= 1
        if (S.contains((u,v))) {
          updateCounters("+", (u,v), S, adjList)
          removeEdge((u,v), S, adjList)
          di += 1
        } else {
          d0 += 1
        }
      }
    }

    println(globalCounter)
    estimation = estimate(globalCounter, s, S, M, di, d0)
    println("Estimation:")
    println(estimation)


    def sampleEdge(edge: (Int, Int), op: String, S: ArrayBuffer[(Int, Int)], adjList: Map[Int, Set[Int]]): Boolean = {
      var ret = false
      if (d0 + di == 0) {
        if (S.size < M) {
          addEdge(edge, S, adjList)
          ret = true
        } else if (rand.nextDouble() < (M.toDouble/t)) {
          var chosen = S(rand.nextInt(S.length))
          updateCounters("-", chosen, S, adjList)
          removeEdge(chosen, S, adjList)
          addEdge(edge, S, adjList)
          ret = true
        }

      } else if (rand.nextDouble() < di.toDouble/(di+d0)) {
        addEdge(edge, S, adjList)
        di -= 1
        ret = true
      } else {
        d0 -= 1
        ret = false
      }
      return ret
    }

    def updateCounters(op: String, edge: (Int, Int), S: ArrayBuffer[(Int, Int)], adjList: Map[Int, Set[Int]]) = {
      val u = edge._1
      val v = edge._2
      val shared = adjList(u) & adjList(v)
      for (common <- shared){
        if (op == "+") {
          globalCounter += 1
          if (localCounters.contains(u)) {
            localCounters(u) += 1
          } else {
            localCounters += (u -> 1)
          }
          if (localCounters.contains(v)) {
            localCounters(v) += 1
          } else {
            localCounters += (v -> 1)
          }         
          if (localCounters.contains(common)) {
            localCounters(common) += 1
          } else {
            localCounters += (common -> 1)
          }
        } else {
          globalCounter -= 1
          if (localCounters(u) == 1){
            localCounters -= u
          } else {
            localCounters(u) -= 1
          }
          if (localCounters(v) == 1){
            localCounters -= v
          } else {
            localCounters(v) -= 1
          }
          if (localCounters(common) == 1){
            localCounters -= common
          } else {
            localCounters(common) -= 1
          }
        }
      }
      // combined neighborhood NSuv = neighborhood of pair._1 in S; union neighborhood of pair._2 in S ??? I think it's not union
      // for all c in NSu,v do
      // 19: t <- t op 1
      // 20: tc <- tc op 1
      // 21: tu <- tu op 1
      // 22: tv <- tv op 1
    }

    def addEdge(edge: (Int, Int), S: ArrayBuffer[(Int, Int)], adjList: Map[Int, Set[Int]]) = {
      val u = edge._1
      val v = edge._2
      if (adjList contains u){
        adjList(u) += v
      } else {
        adjList += (u -> Set(v))
      }
      if (adjList contains v){
        adjList(v) += u
      } else {
        adjList += (v -> Set(u))
      }
      S += edge
    }

    def removeEdge(edge: (Int, Int), S: ArrayBuffer[(Int, Int)], adjList: Map[Int, Set[Int]]) = {
      val u = edge._1
      val v = edge._2
      adjList(u) -= v
      adjList(v) -= u
      if(adjList(u).isEmpty) {
        adjList -= u
      }
      if(adjList(v).isEmpty) {
        adjList -= v
      }
      S -= edge
    }
  }

  def estimate(globalCounter: Int, s: Int, S: ArrayBuffer[(Int, Int)], M: Int, di: Int, d0: Int): BigDecimal = {
    val tau = globalCounter
    val Mt = S.size.toDouble
    val kappa = findKappa(M, s, di, d0)
    var ret = BigDecimal(0)
    var p = 0.0
    if (Mt < 3) {
      ret = 0
    } else {
      p = (Mt/s) * ((Mt-1) / (s-1)) * ((Mt-2) / (s-2))
      //ret = (tau / kappa) * (s * (s-1) * (s-2)) / ( Mt * (Mt-1) * (Mt-2))
    }
    println(Mt, s, p)
    tau / p
  }

  def findKappa(M: Int, s: Int, di: Int, d0: Int): BigDecimal = {
    val omega = scala.math.min(M, s + di + d0)
    var p = BigDecimal(0.0)
    var hyper = BigDecimal(0)
    for (j <- 0 to 2) {
      if (j >= scala.math.max(0, omega - di - d0)  && j <= scala.math.min(omega, s)) {
        hyper = hypergeometric(s+di+d0, s, omega, j)
        p += hyper
      }
    }
    println(p)
    return 1 - p
  }

  def binCoef(n: Int, k: Int): BigDecimal = {

    def permutations(n: Int): BigDecimal =
      (1 to n).map(BigDecimal(_)).foldLeft(BigDecimal(1))(_ * _)

    def combinations(n: Int, k: Int): BigDecimal =
      permutations(n) / (permutations(k) * permutations(n - k))
    
    def fact(n: Int) : Int = (1 to n).product

    println(combinations(n, k))
    combinations(n, k)
  }
  
  def hypergeometric(N: Int, K: Int, n: Int, k: Int): BigDecimal = {
    println(N, K, n, k)
    binCoef(K, k) * binCoef(N - K, n - k) / binCoef(N, n)
  }



  
  // TODO include graph S, M, 

      /*
    val dataSchema = new StructType().add("u", "integer").add("v", "integer")
    var text = spark.readStream
      .option("sep", " ")
      .schema(dataSchema)
      .csv(dir)

    val M = 10
    val S = new ArrayBuffer[(Int, Int)]
    var t = 0
    val rdd = spark.sparkContext.parallelize(S)

    import spark.implicits._

    var arr = new ArrayBuffer[String]

    val query = text.writeStream.foreach( // https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#foreach
        new ForeachWriter[Row] {
          override def process(value: Row): Unit = {
            val u = value.getAs[Int]("u")
            val v = value.getAs[Int]("v")
            rdd.append((u,v))
          }
          override def open(partitionId: Long, epochId: Long): Boolean = {
            true
          }
          override def close(errorOrNull: Throwable): Unit = {

          }
        }
      )
      .trigger(Trigger.Once())
      .start()

    query.awaitTermination()
    println(S)
        */  
}
