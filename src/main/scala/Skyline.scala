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


/*
case class Point(dimensionValues: Array[Double]) {
  override def toString: String = s"Point(${dimensionValues.mkString(",")})"
}
 */


case class Point(dimensionValues: Array[Double], var dominance_score: Int = 0, var localIndex: Int = -1) {
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

    Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)

    // Create spark configuration
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      //.setMaster("local")
      .setAppName("Skyline")

    // Create spark context
    val sc = new SparkContext(sparkConf) // create spark context

    //val inputFile = "Distribution Datasets/10000/Anticorrelated_Data.txt"

    // Input file path in HDFS
    //val inputFile = "hdfs://localhost:8000/user/Anticorrelated_Data.txt"

    val numberOfPartitions = 2
    val k = 3

    val outputFile = "Results/Output.txt"  // Specify the name of the output file
    val writer = new PrintWriter(Files.newBufferedWriter(Paths.get(outputFile), StandardOpenOption.APPEND))


    try{
      val fileNames = List(
        //"Distribution Datasets/Testing/Correlated_Data_Testing.txt"
        //"Distribution Datasets/50000/Uniform_Data.txt",
        //"Distribution Datasets/50000/Uniform_Data_4D.txt",
        //"Distribution Datasets/50000/Uniform_Data_6D.txt",
        //"Distribution Datasets/50000/Normal_Data.txt",
        //"Distribution Datasets/50000/Normal_Data_4D.txt",
        //"Distribution Datasets/50000/Normal_Data_6D.txt",
        //"Distribution Datasets/50000/Correlated_Data.txt",
        //"Distribution Datasets/50000/Correlated_Data_4D.txt",
        //"Distribution Datasets/50000/Correlated_Data_6D.txt",
        //"Distribution Datasets/50000/Anticorrelated_Data.txt",
        //"Distribution Datasets/50000/Anticorrelated_Data_4D.txt",
        //"Distribution Datasets/50000/Anticorrelated_Data_6D.txt",
      )

      fileNames.foreach {inputFile =>
        val start = System.nanoTime()

        val txtFile = sc.textFile(inputFile, minPartitions = numberOfPartitions)

        /*
        val localSkylines = txtFile.map(line => line.split(","))
          .map(line => line.map(elem => elem.toDouble))
          .map(array => Point(array))
          .mapPartitions(Skyline_Calculation.computeLocalSkylineBaseline)

        val globalSkylines = Skyline_Calculation.computeLocalSkylineBaseline(localSkylines.collect().iterator)
         */


        val datasetPreprocessing = txtFile.map(line => line.split(","))
          .map(line => line.map(elem => elem.toDouble))
          .map(array => Point(array))
          .persist(StorageLevel.DISK_ONLY)

        val globalSkylines = Skyline_Calculation.computeGlobalSkylineBaseline(datasetPreprocessing.mapPartitions(Skyline_Calculation.computeLocalSkylineBaseline).collect().iterator)

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

        }.collect().sortBy(_.dominance_score)

        results.foreach(println)

        val elapsedTime = (System.nanoTime() - start).asInstanceOf[Double] / 1000000000.0

        writer.println(s"Elapsed time for $inputFile: $elapsedTime seconds")

      }
    } finally {
      writer.close()
    }


    sc.stop()

    println("***********************************************************************************************")
    println("***********************************************************************************************")



  }
}
