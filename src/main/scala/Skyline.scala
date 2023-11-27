import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.log4j._

import java.io.{BufferedWriter, FileWriter}
import scala.math._
import scala.collection.mutable.ListBuffer

case class Point(dimensionValues: Array[Double], var dominance_score: Int = 0) {
  override def toString: String = s"Point(${dimensionValues.mkString(",")}), Dominance Score: $dominance_score"
}


object Skyline {


  import scala.math._

  def createVariablePartitions(lines: Int, numberOfPartitions: Int): Vector[Vector[Int]] = {
    val partitionSize: Int = floor(lines.toDouble / numberOfPartitions).toInt
    var partitions: Vector[Vector[Int]] = Vector()

    // Add variables 0 - N-1 to partitions
    var lastIndex = 0
    for (i <- 1 until numberOfPartitions) {
      var partition: Vector[Int] = Vector()

      var j: Int = partitionSize * (i - 1)
      partition :+= j + 1
      partition :+= j + partitionSize

      partitions :+= partition
      lastIndex = i
    }

    partitions :+= Vector[Int](partitionSize * lastIndex + 1, (((partitionSize * lastIndex) + (partitionSize - 1)) + (lines - (partitionSize * numberOfPartitions))) + 1)

    partitions
  }




  def main(args: Array[String]): Unit = {

    println("***********************************************************************************************")
    println("***********************************************************************************************")

    println("Skyline Computation")

    /*
    val D = Iterator(
      Point(Array(1.0, 1.0)),
      Point(Array(2.0, 2.0)),
      Point(Array(3.0, 3.0)),
      Point(Array(0.5, 2.0)),
      Point(Array(4.0, 4.0)),
      Point(Array(5.0, 5.0)),
      Point(Array(6.0, 6.0)),
      Point(Array(0.2, 1.3)),
      Point(Array(8.0, 8.0)),
      Point(Array(9.0, 9.0))
    )

    val skylineSet = Skyline_Calculation.computeSkyline(D)
    println("Skyline Set:")
    skylineSet.foreach(println)

     */


    /*
    // Set up Spark configuration
    val conf = new SparkConf().setAppName("Skyline").setMaster("local")
    val sc = new SparkContext(conf)

    // Example usage:
    val points = sc.parallelize(Seq(
      Point(Array(1.0, 1.0)),
      Point(Array(2.0, 2.0)),
      Point(Array(3.0, 3.0)),
      Point(Array(0.5, 2.0)),
      Point(Array(4.0, 4.0)),
      Point(Array(5.0, 5.0)),
      Point(Array(6.0, 6.0)),
      Point(Array(0.2, 1.3)),
      Point(Array(8.0, 8.0)),
      Point(Array(9.0, 9.0))
    ))


    // Compute local skylines
    val localSkylines = points.mapPartitions(Skyline_Calculation.computeLocalSkyline).collect()

    // Compute global skylines
    val globalSkylines = Skyline_Calculation.computeGlobalSkyline(localSkylines.iterator)

    // Broadcast the Global Skyline Set to workers
    val globalSkylinesBroadcast = sc.broadcast(globalSkylines.toList)

    val results = points.mapPartitions { iter =>

      val localDominatedPoints = Skyline_Calculation.loadDominatedPointsFromLocalFileSystem()

      // Capture the global skyline iterator in the closure
      val globalSkylineIteratorOnWorker = globalSkylinesBroadcast.value

      for(s <- globalSkylineIteratorOnWorker) {
        for(d <- localDominatedPoints) {
          if(Skyline_Calculation.dominates(s, d)) {
            s.dominance_score += 1
          }
        }
      }

      globalSkylineIteratorOnWorker.iterator
    }


    results.foreach(println)

    // Print the top-k skyline points with the highest dominance score
    //val topK = 5 // Change this value to the desired top-k
    //val topKSkyline = globalDominanceScores.take(topK)

    //println(s"\nTop-$topK Skyline Points with Highest Dominance Score:")
    //topKSkyline.foreach(println)

    // Stop the SparkContext
    sc.stop()

     */

    /*
    val D = Iterator(
      Point(List(1.0, 1.0)),
      Point(List(2.0, 2.0)),
      Point(List(3.0, 3.0)),
      Point(List(0.5, 2.0)),
      Point(List(4.0, 4.0)),
      Point(List(5.0, 5.0)),
      Point(List(6.0, 6.0)),
      Point(List(0.2, 1.3)),
      Point(List(8.0, 8.0)),
      Point(List(9.0, 9.0))
    )

    val skylineSet = Skyline_Calculation.computeSkyline(D)
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


    val inputFile = "/home/georgematlis/IdeaProjects/Scalable Processing of Dominance-Based Queries/Distribution Datasets/Correlated_Data_Testing.txt"
    //val inputFile = "hdfs://localhost:9000/user/ozzy/data/leonardo/leonardo.txt"
    val outputDir = "/home/georgematlis/IdeaProjects/Scalable Processing of Dominance-Based Queries/Output"


    val numberOfPartitions = 4
    val fileSizeinLines = 500
    val partitions: Vector[Vector[Int]] = createVariablePartitions(fileSizeinLines, numberOfPartitions)
    println(partitions)

    partitions.zipWithIndex.foreach { case (bounds, index) =>
      val outputFile = s"/home/georgematlis/IdeaProjects/Scalable Processing of Dominance-Based Queries/Input/partition_$index.txt"

      val writer = new BufferedWriter(new FileWriter(outputFile))
      try {
        val lines = scala.io.Source.fromFile(inputFile).getLines().toVector
        val partitionLines = lines.slice(bounds(0), bounds(1))
        println(partitionLines)
        partitionLines.foreach { line =>
          writer.write(line)
          writer.newLine()
        }
      } finally {
        writer.close()
      }
    }

    println("Reading from input file: " + inputFile)

    val txtFile = sc.wholeTextFiles(s"/home/georgematlis/IdeaProjects/Scalable Processing of Dominance-Based Queries/Input/*")

    txtFile.collect().foreach(println)

    println("Folder used for output: " + outputDir)

    /*
    val localSkylines = txtFile.map(line => line.split(","))
      .map(line => line.map(elem => elem.toDouble))
      .map(array => Point(array))
      .mapPartitions(Skyline_Calculation.computeLocalSkyline)

     */
    // Extracting points from the textFile


    val localSkylines = txtFile.flatMap { case (filePath, content) =>
      content.split("\n").map(_.split(",")).map { line =>
        val coordinates = line.map(_.toDouble)
        Point(coordinates)
      }
    }.mapPartitions(Skyline_Calculation.computeLocalSkyline)

    val globalSkylines = Skyline_Calculation.computeGlobalSkyline(localSkylines.collect().iterator)

    // Broadcast the Global Skyline Set to workers
    val globalSkylinesBroadcast = sc.broadcast(globalSkylines.toList)

    val results = txtFile.mapPartitions { _ =>

      val localDominatedPoints = Skyline_Calculation.loadDominatedPointsFromLocalFileSystem()
      println("Partition ID: ", TaskContext.getPartitionId())

      // Capture the global skyline iterator in the closure
      val globalSkylineIteratorOnWorker = globalSkylinesBroadcast.value

      globalSkylineIteratorOnWorker.foreach(println)

      for (s <- globalSkylineIteratorOnWorker) {
        for (d <- localDominatedPoints) {
          if (Skyline_Calculation.dominates(s, d)) {
            s.dominance_score += 1
          }
        }
        println("Dominanace Score: " + s.dominance_score + " partition ID: " + TaskContext.getPartitionId())
      }

      globalSkylineIteratorOnWorker.iterator
    }

    results.foreach(println)

    sc.stop()



    println("***********************************************************************************************")
    println("***********************************************************************************************")



  }
}
