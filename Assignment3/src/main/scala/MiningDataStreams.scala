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
// spark-submit --class "Main" --master local target/scala-2.12/mining-data-streams-project_2.12-1.0.jar
object Main {
  def main(args: Array[String]): Unit = {
    val dir = "data" // Should be some file on your system
    val spark = SparkSession.builder.appName("Frequent Itemsets Application").master("local[*]").getOrCreate()
    val sparkContext = spark.sparkContext
    val rawGraph = sparkContext.textFile(dir + "/edges_and_op.txt").cache()

    val graph = rawGraph.map(_.split(" ")).map{case Array(f1,f2,f3) => (f1.toInt, f2.toInt, f3)}.collect.toArray
    
    val M = 10
    var t = 0
    var s = 0
    var di = 0
    var d0 = 0
    val S = new ArrayBuffer[(Int, Int)]

    var adjList = scala.collection.mutable.Map[Int, Set[Int]]()

    var globalCounter = 0
    var localCounters = scala.collection.mutable.Map[Int, Int]()
    
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
          updateCounters(op, (u,v), S, adjList)
          di += 1
        } else {
          updateCounters(op, (u,v), S, adjList)
          S -= ((u,v))
          d0 += 1
        }
      }
    }


    def sampleEdge(edge: (Int, Int), op: String, S: ArrayBuffer[(Int, Int)], adjList: Map[Int, Set[Int]]): Boolean = {
      if (d0 + di == 0) {
        if (S.size < M) {
          addEdge(edge, S, adjList)
          return true
        } else if (scala.util.Random.nextDouble < M/t.toDouble){
          var chosen = S(scala.util.Random.nextInt(S.length))
          updateCounters(op, chosen, S, adjList)
          removeEdge(chosen, S, adjList)
          addEdge(edge, S, adjList)
          return true
        }
      } else if (scala.util.Random.nextDouble < M/t.toDouble) {
        addEdge(edge, S, adjList)
        di -= 1
        return true
      } else {
        d0 -= 1
        return false
      }
    }

    // operation = + or -
    // TODO include graph S
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
            localCounters += (u -> 0)
          }
          if (localCounters.contains(v)) {
            localCounters(v) += 1
          } else {
            localCounters += (v -> 0)
          }         
          if (localCounters.contains(common)) {
            localCounters(common) += 1
          } else {
            localCounters += (common -> 0)
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
  }

  def addEdge(edge: (Int, Int), S: ArrayBuffer[(Int, Int)], adjList: Map[Int, Set[Int]]) = {
    val u = edge._1
    val v = edge._2
    if (S contains edge){
      adjList(u) += v
      adjList(v) += u
    } else {
      adjList += (u -> Set(v), v -> Set(u))
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
    if(adjList(u).isEmpty) {
      adjList -= v
    }
    S -= edge
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