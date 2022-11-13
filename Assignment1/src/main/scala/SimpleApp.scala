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

    val dir = "docs" // Should be some file on your system
    
    val spark = SparkSession.builder.appName("Simple Application").master("local[4]").getOrCreate()

    val rddWhole = spark.sparkContext.wholeTextFiles(dir)

    val shingles = new Shingling()
    
    val allShingles = shingles.makeShingles(rddWhole, 5)
    
    val sims = new CompareSets()

    val similarities = sims.computeJaccard(allShingles)

    val hash = new MinHashing()

    val minHashed = hash.buildMinHash(allShingles)

    val compareMinHash = sims.compareSigMatrix(minHashed)


    compareMinHash.foreach(e => {
      println(e)
    })

    spark.stop()

  }
}

class Shingling {

  def makeShingles(rddWhole: RDD[(String, String)], shingleSize: Int): RDD[(String, Set[Int])] = {

    var allShingles = new ArrayBuffer[String]()  

    return rddWhole.map({ case (filePath, fileContent) => {
        val set = fileContent.replace(" ", "")
          .sliding(shingleSize)
          .map(s1 => MurmurHash3.stringHash(s1)) // [(hash), (hash)]
          .toSet
        (filePath, set)
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
  // - a b c 
  // a 1
  // b   1
  // c     1
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
    // historyOfPermutations = []
    // 4, 3, 1, 8, 5, ...

    
    for(k<-Range(0, k)) {
      // val newOrder = []
      // for (i <- Range(0, charMatrix.size)) {
      //   Random.nextInt(i, charMatrix.size)
      //   newOrder.push(charMatrix[i])
      // }

      // charmatrix = [a = [...], b = [...], ...]
      // shuffle = [b = [...], a = [...]]
      
      
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
