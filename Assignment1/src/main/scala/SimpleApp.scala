import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer
import scala.sys.process.processInternal
import org.apache.spark.rdd.RDD 

import scala.collection.mutable.HashMap
import scala.util.hashing.MurmurHash3
import org.apache.spark.sql.catalyst.expressions.In

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

}

class MinHashing {

  def createCharMatrix(shingles: RDD[(String, Set[Int])]): Array[Array[Int]]= {
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

      i += 1

      for (file <- arr){
        j += 1

        if (file._2 contains sh) matrix(i)(j) = 1

      }
      j = 0
    }

    return matrix
  }

  def buildMinHash( shingles: RDD[(String, Set[Int])] ) = {
    val matrix = createCharMatrix(shingles)
    print(matrix)
  }
}