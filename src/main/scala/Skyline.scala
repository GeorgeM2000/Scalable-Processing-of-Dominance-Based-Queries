import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._

import scala.annotation.unused

case class Point(dimensionValues: Array[Double], var dominance_score: Int = 0)

object Skyline {

  def main(args: Array[String]): Unit = {

    println("***********************************************************************************************")
    println("***********************************************************************************************")

    println("Skyline Computation")

    /*
    val D = Iterator(
      Point(List(1.0, 1.0)),
      Point(List(2.0, 2.0)),
      Point(List(3.0, 3.0)),
      Point(List(0.5, 2.0)),
      Point(List(4.0, 4.0)),
      Point(List(5.0, 5.0)),
      Point(List(6.0, 6.0)),
      Point(List(0.5, 2.0)),
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
    val outputDir = "/home/georgematlis/IdeaProjects/Scalable Processing of Dominance-Based Queries/Skyline_Output"

    println("Reading from input file: " + inputFile)

    val txtFile = sc.textFile(inputFile, 4)

    println("Folder used for output: " + outputDir)

    val rdd = txtFile.map(line => line.split(","))
      .map(line => line.map(elem => elem.toDouble))
      .map(array => Point(array))
      .mapPartitions(Skyline_Calculation.computeSkyline)
      .saveAsTextFile(outputDir)


    sc.stop()



    println("***********************************************************************************************")
    println("***********************************************************************************************")

  }
}
