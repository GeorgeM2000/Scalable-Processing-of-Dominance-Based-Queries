import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.log4j._

import java.io.{BufferedWriter, FileWriter}
import scala.math._
import scala.collection.mutable.ListBuffer

case class Point(dimensionValues: Array[Double], var dominance_score: Int = 0) {
  override def toString: String = s"Point(${dimensionValues.mkString(",")}), Dominance Score: $dominance_score"
}


object Skyline {

  def main(args: Array[String]): Unit = {

    println("***********************************************************************************************")
    println("***********************************************************************************************")

    println("Skyline Computation")



    Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)

    // Create spark configuration
    val sparkConf = new SparkConf()
      //.setMaster("local[2]")
      .setMaster("local")
      .setAppName("Skyline")

    // Create spark context
    val sc = new SparkContext(sparkConf) // create spark context


    val inputFile = "Distribution Datasets/Correlated_Data.txt"

    // Input file path in HDFS
    //val inputFile = "hdfs://localhost:9000/user/ozzy/data/leonardo/leonardo.txt"

    // Output directory path to store the results
    //val outputDir = "/home/georgematlis/IdeaProjects/Scalable Processing of Dominance-Based Queries/Output"

    val numberOfPartitions = 5
    val k = 1

    val txtFile = sc.textFile(inputFile, minPartitions = numberOfPartitions)

    val localSkylines = txtFile.map(line => line.split(","))
      .map(line => line.map(elem => elem.toDouble))
      .map(array => Point(array))
      .mapPartitions(Skyline_Calculation.computeLocalSkyline)

    val globalSkylines = Skyline_Calculation.computeGlobalSkyline(localSkylines.collect().iterator)

    // Broadcast the Global Skyline Set to workers
    val globalSkylinesBroadcast = sc.broadcast(globalSkylines.toList)

    val results = txtFile.mapPartitions { _ =>
      val localDominatedPoints = Skyline_Calculation.loadDominatedPointsFromLocalFileSystem()

      // Capture the global skyline iterator in the closure
      val globalSkylineIteratorOnWorker = globalSkylinesBroadcast.value

      for (s <- globalSkylineIteratorOnWorker) {
        for (d <- localDominatedPoints) {
          if (Skyline_Calculation.dominates(s, d)) {
            s.dominance_score += 1
          }
        }
      }

      globalSkylineIteratorOnWorker.iterator
    }

    // Assuming results is an RDD[Point]
    val combinedResults = results
      .map(point => (point, point.dominance_score)) // Convert Point to (Point, dominance_score) pair
      .aggregateByKey(0)(_ + _, _ + _) // Aggregate dominance scores by key (Point)

    // Extract the top dominating skyline points
    val topDominatingGlobalSkylinePoints = combinedResults
      .sortBy({ case (_, totalDominance) => totalDominance }, ascending = false)
      .map({ case (point, _) => point })

    //val topDominatingGlobalSkylinePoints = results.collect().distinct().sortBy(point => point.dominance_score, ascending = false)

    topDominatingGlobalSkylinePoints.foreach(println)

    sc.stop()

    println("***********************************************************************************************")
    println("***********************************************************************************************")



  }
}

object DominatingPoints {
  def main(args: Array[String]): Unit = {

    println("***********************************************************************************************")
    println("***********************************************************************************************")

    println("Skyline Computation")

    Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)

    // Create spark configuration
    val sparkConf = new SparkConf()
      //.setMaster("local[2]")
      .setMaster("local")
      .setAppName("Skyline")

    // Create spark context
    val sc = new SparkContext(sparkConf) // create spark context


    val inputFile = "Distribution Datasets/Correlated_Data.txt"

    // Input file path in HDFS
    //val inputFile = "hdfs://localhost:9000/user/ozzy/data/leonardo/leonardo.txt"

    // Output directory path to store the results
    //val outputDir = "/home/georgematlis/IdeaProjects/Scalable Processing of Dominance-Based Queries/Output"


    val txtFile = sc.textFile(inputFile)



    sc.stop()

    println("***********************************************************************************************")
    println("***********************************************************************************************")


  }

}
