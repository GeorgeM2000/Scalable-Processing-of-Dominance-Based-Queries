import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import java.io.{BufferedReader, File, FileReader, PrintWriter}
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable.ListBuffer
import scala.util.hashing.MurmurHash3
import scala.math._

class Top_K_Dominating_Points_Calculation (points_set: org.apache.spark.rdd.RDD[Point], val strategy: String = "Uniform Grid") {
  // points_set: set of points for which dominance scores are to be calculated
  // Strategy options are "Uniform Grid" and "Adaptive Grid"
  // Uniform Grid creates a uniform grid based on the minimum and maximum coordinates in each dimension
  // Adaptive Grid creates a variable grid based on the mean and standard deviation of a sample of the points

  private val count = points_set.count()
  private def sample_points(sample_ratio: Double = 0.1, min_sample: Int = 10000): org.apache.spark.rdd.RDD[Point] = {

    val sample_size = max(floor(count*sample_ratio),min(count,min_sample))
    val final_sample_ratio = sample_size/count

    return points_set.sample(false,final_sample_ratio)
  }

  private def make_grid(cell_size: Int = 1000): Map[Int,List[BigDecimal]]  = {

    val example_point = points_set.take(1)(0)
    val dimensionality = example_point.dimensionValues.length

    val cell_num_per_dimension = ceil(count/(cell_size*dimensionality))

    var grid = Map.empty[Int, List[BigDecimal]]

    if (strategy.equals("Adaptive Grid")) {

      val sample = sample_points()
      val sample_count = sample.count()

      for (i <- 0 until dimensionality) {

        val coords = sample.map(p => p.dimensionValues(i)).persist()



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

    grid

  }

  def calculate(k:Int): List[Point] = {

    return List[Point]

  }
}

object Top_K_Dominating_Points {

  def main(args: Array[String]): Unit = {

    // File location and number of top dominant points to return are passed as args to the main function
    val inputFile = args(0)
    val dominant_points = args(1).toInt

    val sparkConf = new SparkConf()
      //.setMaster("local[2]")
      .setMaster("local")
      .setAppName("DominanceQuery")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val txtFile = sc.textFile(inputFile)

    val allPoints = txtFile.map(line => line.split(","))
      .map(line => line.map(elem => elem.toDouble))
      .map(array => Point(array))
      .persist()

    println(allPoints.takeSample(false,1)(0).toString)// temp thing until i write the code that actually gives me the dominance query

  }

}
