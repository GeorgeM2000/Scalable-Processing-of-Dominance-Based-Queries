import Skyline_Calculation.dominates
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import java.io.PrintWriter
import java.nio.file.{Files, Paths, StandardOpenOption}



/*
case class Point(dimensionValues: Array[Double], var dominance_score: Int = 0, var distance_score: Double = 0.0, var localIndex: Int = -1) {
  override def toString: String = s"Point(${dimensionValues.mkString(",")}), Dominance Score: $dominance_score"
  def sum_dominance(p:Point): Point = {
    this.dominance_score += p.dominance_score
    this
  }
}
 */

case class Point(dimensionValues: Array[Double]) {
  override def toString: String = s"Point(${dimensionValues.mkString(",")})"
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

    Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)

    // Create spark configuration
    val sparkConf = new SparkConf()
      .setMaster("local[6]")
      //.setMaster("local")
      .setAppName("Skyline")

    // Create spark context
    val sc = new SparkContext(sparkConf) // create spark context

    //val inputFile = "/home/georgematlis/IdeaProjects/Scalable Processing of Dominance-Based Queries/Distribution Datasets/Anticorrelated_Data.txt"

    // Input file path in HDFS
    //val inputFile = "hdfs://localhost:9000/user/ozzy/data/leonardo/leonardo.txt"

    // Output directory path to store the results
    //val outputDir = "/home/georgematlis/IdeaProjects/Scalable Processing of Dominance-Based Queries/Output"

    val numberOfPartitions = 6

    val outputFile = "Results/Output.txt"  // Specify the name of the output file
    val writer = new PrintWriter(Files.newBufferedWriter(Paths.get(outputFile), StandardOpenOption.APPEND))

    try{
      val fileNames = List(
        "Distribution Datasets/100000/Uniform_Data.txt",
        "Distribution Datasets/100000/Uniform_Data_4D.txt",
        "Distribution Datasets/100000/Uniform_Data_6D.txt",
        "Distribution Datasets/100000/Normal_Data.txt",
        "Distribution Datasets/100000/Normal_Data_4D.txt",
        "Distribution Datasets/100000/Normal_Data_6D.txt",
        "Distribution Datasets/100000/Correlated_Data.txt",
        "Distribution Datasets/100000/Correlated_Data_4D.txt",
        "Distribution Datasets/100000/Correlated_Data_6D.txt",
        "Distribution Datasets/100000/Anticorrelated_Data.txt",
        "Distribution Datasets/100000/Anticorrelated_Data_4D.txt",
        "Distribution Datasets/100000/Anticorrelated_Data_6D.txt",
      )

      fileNames.foreach { inputFile =>
        val start = System.nanoTime()

        val txtFile = sc.textFile(inputFile, minPartitions = numberOfPartitions)
        val localSkylines = txtFile.map(line => line.split(","))
          .map(line => line.map(elem => elem.toDouble))
          .map(array => Point(array))
          .mapPartitions(Skyline_Calculation.computeLocalSkylineBaseline)

        val globalSkylines = Skyline_Calculation.computeLocalSkylineBaseline(localSkylines.collect().iterator)

        val elapsedTime = (System.nanoTime() - start).asInstanceOf[Double] / 1000000000.0

        // Write the result to the file
        writer.println(s"Elapsed time for $inputFile: $elapsedTime seconds")
      }
    } finally {
      writer.close()
    }



    //val start = System.nanoTime()

    //val txtFile = sc.textFile(inputFile, minPartitions = numberOfPartitions)


    /*
    // Task 2
    val dominanceScoreCalcForPoints:RDD[(Point, Int)] = txtFile.map(line => line.split(","))
      .map(line => line.map(elem => elem.toDouble))
      .map(array => Point(array))
      .cartesian(dominanceScoreCalcForPoints)
      .filter { case (point1, point2) =>
      point1 != point2 && dominates(point1, point2)
    }.map(x => (x._1, 1))
      .reduceByKey(_+_)
      .sortBy(_._2,ascending = false)

     */

    /*
    val localSkylines = txtFile.map(line => line.split(","))
      .map(line => line.map(elem => elem.toDouble))
      .map(array => Point(array))
      .mapPartitions(Skyline_Calculation.computeLocalSkylineBaseline)

    //println((System.nanoTime() - start).asInstanceOf[Double] / 1000000000.0)

    val globalSkylines = Skyline_Calculation.computeLocalSkylineBaseline(localSkylines.collect().iterator)

    println((System.nanoTime() - start).asInstanceOf[Double] / 1000000000.0)

     */

    //globalSkylines.foreach(println)



    /*
    // Local skyline computation using the euclidean distance as a scoring function to sort the d-dimensional points
    val localSkylines = txtFile.map(line => line.split(","))
      .map(line => line.map(elem => elem.toDouble))
      .map(array => Point(array, distance_score = euclideanDistance(array)))
      .sortBy(point => point.distance_score)
      .mapPartitions(Skyline_Calculation.computeFastSkyline)

     */

    //println((System.nanoTime() - start).asInstanceOf[Double] / 1000000000.0)


    /*
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

     */


    sc.stop()

    println("***********************************************************************************************")
    println("***********************************************************************************************")



  }
}
