import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.log4j._
import org.apache.spark.storage.StorageLevel

import java.io.{BufferedWriter, FileWriter}
import scala.math._
import scala.collection.mutable.ListBuffer


case class Point(dimensionValues: Array[Double], var dominance_score: Int = 0, var distance_score: Double = 0.0, var localIndex: Int = -1) {
  override def toString: String = s"Point(${dimensionValues.mkString(",")}), Dominance Score: $dominance_score"
}


object Skyline {


  def euclideanDistance(coordinates: Array[Double]): Double = {
    val squaredSum = coordinates.map(value => value * value).sum
    Math.sqrt(squaredSum)
  }

  def main(args: Array[String]): Unit = {

    println("***********************************************************************************************")
    println("***********************************************************************************************")

    println("Skyline Computation")

    /*

    val start = System.nanoTime()
    val D = Iterator(
      Point(Array(1.0, 1.0)),
      Point(Array(2.0, 2.0)),
      Point(Array(3.0, 3.0)),
      Point(Array(0.5, 2.0)),
      Point(Array(4.0, 4.0)),
      Point(Array(5.0, 5.0)),
      Point(Array(6.0, 6.0)),
      Point(Array(0.2, 0.9)),
      Point(Array(8.0, 8.0)),
      Point(Array(9.0, 9.0))
      //Point(Array(0.1, 0.1))
    )

    val skylineSet = Skyline_Calculation.computeFastSkyline(D)

    println((System.nanoTime() - start).asInstanceOf[Double] / 1000000000.0)
    println("Skyline Set:")
    skylineSet.foreach(println)

     */





    Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)

    // Create spark configuration
    val sparkConf = new SparkConf()
      //.setMaster("local[2]")
      .setMaster("local")
      .setAppName("Skyline")

    // Create spark context
    val sc = new SparkContext(sparkConf) // create spark context

    val inputFile = "/home/georgematlis/IdeaProjects/Scalable Processing of Dominance-Based Queries/Distribution Datasets/Correlated_Data.txt"

    // Input file path in HDFS
    //val inputFile = "hdfs://localhost:9000/user/ozzy/data/leonardo/leonardo.txt"

    // Output directory path to store the results
    //val outputDir = "/home/georgematlis/IdeaProjects/Scalable Processing of Dominance-Based Queries/Output"

    val numberOfPartitions = 4

    val start = System.nanoTime()

    val txtFile = sc.textFile(inputFile, minPartitions = numberOfPartitions)


    val localSkylines = txtFile.map(line => line.split(","))
      .map(line => line.map(elem => elem.toDouble))
      .map(array => Point(array))
      .mapPartitions(Skyline_Calculation.computeFastSkyline)



    /*
    val localSkylines = txtFile.map(line => line.split(","))
      .map(line => line.map(elem => elem.toDouble))
      .map(array => Point(array, distance_score = euclideanDistance(array)))
      .sortBy(point => point.distance_score)
      .mapPartitions(Skyline_Calculation.computeFastSkyline)

     */

    println((System.nanoTime() - start).asInstanceOf[Double] / 1000000000.0)


    val datasetPreprocessing = txtFile.map(line => line.split(","))
      .map(line => line.map(elem => elem.toDouble))
      .map(array => Point(array, distance_score = euclideanDistance(array)))
      .sortBy(point => point.distance_score)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val globalSkylines = Skyline_Calculation.computeGlobalSkyline(datasetPreprocessing
      .mapPartitions(Skyline_Calculation.computeLocalSkylineBaseline).collect().iterator)


    // Broadcast the Global Skyline Set to workers
    val globalSkylinesBroadcast = sc.broadcast(globalSkylines.toList)

    val results = datasetPreprocessing.mapPartitions {Data: Iterator[Point] =>
      val points = Data.toList
      val localSkylineIndices = Skyline_Calculation.loadLocalSkylineIndices()

      // Capture the global skyline iterator in the closure
      val globalSkylineIteratorOnWorker = globalSkylinesBroadcast.value

      val localDominatedPoints = Skyline_Calculation.findLocalDominatedPoints(localSkylineIndices.localPartitionLength, localSkylineIndices.localSkylineIndices)

      for (s <- globalSkylineIteratorOnWorker) {
        for (i <- localDominatedPoints) {
          if (Skyline_Calculation.dominates(s, points(i))) {
            s.dominance_score += 1
          }
        }
      }

      globalSkylineIteratorOnWorker.iterator
    }

    results.foreach(println)


    sc.stop()

    println("***********************************************************************************************")
    println("***********************************************************************************************")



  }
}
