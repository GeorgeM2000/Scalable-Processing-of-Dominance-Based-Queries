import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import java.io.{BufferedReader, File, FileReader, PrintWriter}
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable.ListBuffer
import scala.util.hashing.MurmurHash3
import scala.math._

class Top_K_Dominating_Points_Calculation (sc: SparkContext, points_set: org.apache.spark.rdd.RDD[Point], val strategy: String = "Uniform Grid") {
  // points_set: set of points for which dominance scores are to be calculated
  // Strategy options are "Uniform Grid" and "Adaptive Grid"
  // Uniform Grid creates a uniform grid based on the minimum and maximum coordinates in each dimension
  // Adaptive Grid creates a variable grid based on the mean and standard deviation of a sample of the points

  private val count = points_set.count()
  private val dimensionality = points_set.take(1)(0).dimensionValues.length

  private def sample_points(sample_ratio: Double = 0.1, min_sample: Int = 10000): org.apache.spark.rdd.RDD[Point] = {

    val sample_size = max(floor(count*sample_ratio),min(count,min_sample))
    val final_sample_ratio = sample_size/count

    return points_set.sample(false,final_sample_ratio)
  }

  private def make_grid(cell_size: Int = 1000): (Map[Int,List[BigDecimal]],Int) = {

    val cell_num_per_dimension = ceil(count/(cell_size*dimensionality))

    var grid = Map.empty[Int, List[BigDecimal]]

    if (strategy.equals("Adaptive Grid")) {

      val sample = sample_points()
      val sample_count = sample.count()

      for (i <- 0 until dimensionality) {

        val coords = sample.map(p => p.dimensionValues(i)).persist()

        // TODO: Implement adaptive grid

        //grid + (i -> cells)

      }

    } else {

      for(i <- 0 until dimensionality){

        val coords = points_set.map(p => p.dimensionValues(i)).persist()
        val min = BigDecimal(coords.min())
        val max =  BigDecimal(coords.max())

        val cell_size = (max-min)/cell_num_per_dimension

        val cells = min to max by cell_size

        grid + (i -> cells)

      }

    }

    (grid, (cell_num_per_dimension*dimensionality).toInt)

  }

  private def locate_point(grid: Map[Int,List[BigDecimal]], point: Point): (List[Int],Int) = {

    var pos = 0

    var coords = Array[Int](dimensionality)

    for (i <- 0 until dimensionality) {

      for (j <- 0 until grid(i).length-1){

        if ( point.dimensionValues(i) >= grid(i)(j) && point.dimensionValues(i) < grid(i)(j+1)){

          pos = (pos + pow(j,i+1)).toInt

          coords(i) = j

        }

      }

    }

    point.grid_pos = pos

    (coords.toList,pos)

  }

  def calculate(k:Int): org.apache.spark.rdd.RDD[Point] = {

    val grid = make_grid()



    sc.parallelize(List.empty[Point]) //placeholder

  }
}

object Top_K_Dominating_Points {

  def main(args: Array[String]): Unit = {

    // File location and number of top dominant points to return are passed as args to the main function
    val inputFile = args(0)
    val dominant_points = args(1).toInt
    val executors = args(2)

    val sparkConf = new SparkConf()
      //.setMaster("local[2]")
      .setMaster("local")
      .setAppName("DominanceQuery")
      .set("spark.executor.cores", executors)

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val start = System.nanoTime()

    val txtFile = sc.textFile(inputFile)

    val allPoints = txtFile.map(line => line.split(","))
      .map(line => line.map(elem => elem.toDouble))
      .map(array => Point(array))
      .persist()

    val calc = new Top_K_Dominating_Points_Calculation(sc,allPoints)

    val top_points = calc.calculate(dominant_points)

    val dur = (System.nanoTime().toDouble-start.toDouble)/1000000000
    val result = top_points.map(f=>f.toString).collect()

    println(s"Top Points: $result\n")
    println(s"Workers: $executors")
    println(s"Input File: $inputFile")
    println(s"Time Elapsed: $dur seconds")

    sc.stop(0)

  }

}
