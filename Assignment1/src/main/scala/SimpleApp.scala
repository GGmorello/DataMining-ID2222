import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer
import scala.sys.process.processInternal
import org.apache.spark.rdd.RDD 
import scala.util.control.Breaks._

import scala.collection.mutable.HashMap
import scala.util.hashing.MurmurHash3
import org.apache.spark.sql.catalyst.expressions.In
import scala.util.Random

object Main {
  def main(args: Array[String]): Unit = {

    val dir = "docs/wikipedia" // Should be some file on your system
    val lshThreshold: Double = 0.8
    
    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()

    val rddWhole = spark.sparkContext.wholeTextFiles(dir)

    val shingles = new Shingling()
    
    val allShingles = shingles.makeShingles(rddWhole, 5)
    
    val sims = new CompareSets()

    val similarities = sims.computeJaccard(allShingles)

    println("Jaccard Similiarities: ")

    similarities.foreach(println)

    val hash = new MinHashing()

    val minHashed = hash.buildMinHash(allShingles)

    val compareMinHash = sims.compareSigMatrix(minHashed)

    var lsh = new LSH()

    var matchingPairs = lsh.createLSH(minHashed, lshThreshold)

    println("candidate pair amount: " + matchingPairs.length)

    matchingPairs.foreach({ case (doc1, doc2) => { println("candidate pair " + doc1 + " & " + doc2) }})

    spark.stop()

  }
}

class Shingling {

  def makeShingles(rddWhole: RDD[(String, String)], shingleSize: Int): RDD[(String, Set[Int])] = {

    var allShingles = new ArrayBuffer[String]()  

    return rddWhole.map({ case (filePath, fileContent) => {
        val set = fileContent.replace(" ", "")
          .sliding(shingleSize)
          .map(s1 => MurmurHash3.stringHash(s1)) 
          .toSet
        (filePath.split("/").last, set)
    }})
  }
}


class CompareSets {

  def computeJaccard(data: RDD[(String, Set[Int])]): ArrayBuffer[(String, String, Double)] =  {
    val arr = data.collect()
    val sol = ArrayBuffer[(String, String, Double)]()
    for (i <- Range(0, arr.size)) {
      for (j <- Range(i+1, arr.size)) {
        sol += ((arr(i)._1, arr(j)._1, compare(arr(i)._2, arr(j)._2)))
      }
    }
    return sol
  }

  def compare(s1: Set[Int], s2: Set[Int]): Double = {
    val inter = s1 & s2
    val union = s1 | s2
    val sol: Double = (inter.size.toDouble / union.size)
    return sol
  }

  def compareSigMatrix(sigMatrix: Array[Array[Int]]): ArrayBuffer[(Int, Int, Double)] = {
    var pairs = ArrayBuffer[(Int, Int, Double)]()
    for (i <- Range(0, sigMatrix(0).size)) {
      for (j <- Range(0, sigMatrix(0).size)) {
        var sim = 0
        for (k <- Range(0, sigMatrix.size)) {
          if (sigMatrix(k)(i) == sigMatrix(k)(j)) {
            sim += 1
          }
        }
        var pair = (i, j, sim.toDouble / sigMatrix.size)
        pairs += pair
      }
    }
    return pairs
  }
}

class MinHashing {

  def buildMinHash( shingles: RDD[(String, Set[Int])] ): Array[Array[Int]] = {

    val charMatrix = createCharMatrix(shingles)
    
    val sigMatrix = createSigMatrix(charMatrix)
    return sigMatrix
  }

  def createCharMatrix(shingles: RDD[(String, Set[Int])]): Array[Array[Int]] = {
    var i = 0
    var j = 0
    val arr = shingles.collect()
    val shingleSet = ArrayBuffer[Int]()
    for (sh <- arr){
      shingleSet ++= sh._2.toArray
    }

    val allShingles = shingleSet.distinct

    val matrix = Array.ofDim[Int](shingleSet.size, arr.size)

    for (sh <- allShingles) {

      for (file <- arr){

        if (file._2 contains sh) matrix(i)(j) = 1
        j += 1

      }
      i += 1
      j = 0
    }

    return matrix
  }

  def createSigMatrix(charMatrix: Array[Array[Int]]): Array[Array[Int]] = {
    
    val k = 1000
    val sigMatrix = Array.ofDim[Int](k, charMatrix(0).size)
    var auxArray = Array(charMatrix(0).size)

    for(k<-Range(0, k)) {
      
      val shuffle = Random.shuffle(charMatrix.toSeq)
      auxArray = Array.fill(charMatrix(0).size)(-1)

      for(i <- Range(0, charMatrix.size)){
        breakable {

            for (j <- Range(0, charMatrix(0).size)){
              if (shuffle(i)(j) == 1 && auxArray(j) == -1) {
                auxArray(j) = i
              }
            }
            if (!(auxArray contains -1)) {
              break
            }
        }
      }
      sigMatrix(k) = auxArray
    }
    return sigMatrix
  }
}

class LSH {
  def createLSH(sigMatrix: Array[Array[Int]], threshold: Double): ArrayBuffer[(Int, Int)] = {
    val b = 200
    val r = sigMatrix.size / b

    var candidatePairs = ArrayBuffer[(Int, Int, Int)]()
    
    for (bandIdx <- Range(0, b)){
      var bucketMap = HashMap[Int, ArrayBuffer[Int]]()
      val offset = r * bandIdx
      val band = sigMatrix.slice(offset, offset + r)

      for (i <- Range(0, sigMatrix(0).size)) {
        var colSig = ""
        for (j <- Range(0, band.size)) {
          colSig += band(j)(i) 
        }
        val hash = MurmurHash3.stringHash(colSig)
        val existing = bucketMap get hash
        if (existing.isDefined) {
          bucketMap(hash) = existing.get += i
        } else {
          var instance = ArrayBuffer[Int]()
          instance = instance += i
          bucketMap(hash) = instance 
        }
      }
      bucketMap.foreach({ case (k, v) => {
          if (v.size > 1) {
            for (i <- Range(0, v.size)) {
              for (j <- Range(i+1, v.size)) {
                val tuple = (v(i), v(j), bandIdx)
                candidatePairs += tuple
              }
            }
          }
        }
      })
    }
    
    val res = candidatePairs.map({ case (a, b, c) => { (a, b) } }).groupBy(identity).mapValues(_.size)

    var finalCandidates = ArrayBuffer[(Int, Int)]()

    res.foreach({ case ((doc1, doc2), amt) => {
      val fraction = amt.toDouble / b
      println("doc1/doc2 fraction: " + fraction)
      if (fraction >= threshold) {
        val tuple = (doc1, doc2)
        finalCandidates += tuple
      }
    }})

    return finalCandidates
  }
}


// spark-submit --class "Main" --master local target/scala-2.12/simple-project_2.12-1.0.jar