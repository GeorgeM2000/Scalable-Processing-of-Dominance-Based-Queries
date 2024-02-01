import org.apache.commons.collections.functors.TruePredicate
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

import java.io.{BufferedReader, File, FileReader, PrintWriter}
import org.apache.log4j.{Level, Logger}

import scala.:+
import scala.collection.mutable.ListBuffer
import scala.util.hashing.MurmurHash3
import scala.math._

case class Point(dimensionValues: Array[Double], var dominance_score: Int = 0, var distance_score: Double = 0.0, var localIndex: Int = -1, var grid_pos: Int = 0) {
  override def toString: String = s"Point(${dimensionValues.mkString(",")}), Dominance Score: $dominance_score"
  def sum_dominance(p:Point): Point = {
    this.dominance_score += p.dominance_score
    this
  }
}

class Top_K_Dominating_Points_Calculation (points_set: org.apache.spark.rdd.RDD[Point], val strategy: String = "Uniform Grid") extends Serializable {
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

  // Checks if A dominates B
  def dominates_int(A: Seq[Int], B: Seq[Int]): Boolean = {
    var output = true

    for(i <- 0 until A.length){
      if (A(i) >= B(i)){
        output = false
      }
    }

    output
  }
  def dominates(A: Seq[Double], B: Seq[Double]): Boolean = {
    var output = true

    for (i <- 0 until A.length) {
      if (A(i) >= B(i)) {
        output = false
      }
    }

    output
  }
  def dominance_comparison(A: Point,B: Point): Boolean = {
    if (dominates(A.dimensionValues,B.dimensionValues)){
      true
    } else {
      false
    }
  }
  def dominance_count(A: Point,B: Point): Point = {
    if (dominance_comparison(A,B)) {
      A.dominance_score = A.dominance_score + 1
    }
    A
  }

  private def make_grid(cell_size: Int = 200): (Map[Int,List[BigDecimal]],Int) = {

    val cell_num_per_dimension = ceil(pow(count/cell_size,1f/dimensionality))

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
        val max =  BigDecimal(coords.max()+0.000001)

        val cell_size = (max-min)/cell_num_per_dimension

        val cells = (min to max by cell_size).toList

        grid = grid + (i -> cells)

      }

    }

    (grid, pow(cell_num_per_dimension,dimensionality).toInt)

  }

  private def calc_pos(grid: Map[Int,List[BigDecimal]], coords:List[Int]): Int = {

    var output = 0

    val dim = grid(1).length-1

    for (i <- 0 until coords.length) {

      output = (output + pow(dim,i)*coords(i)).toInt

    }

    output
  }
  private def locate_point(grid: Map[Int,List[BigDecimal]], point: Point): Point = {

    var pos = 0

    val dim = grid(1).length-1

    for (i <- 0 until dimensionality) {

      for (j <- 0 until dim){

        if ( point.dimensionValues(i) >= grid(i)(j) && point.dimensionValues(i) < grid(i)(j+1)){

          pos = (pos + pow(dim,i)*j).toInt

        }

      }

    }

    point.grid_pos = pos

    point

  }

  //combinations code was a helpful contribution courtesy of StackOverflow user Tesseract
  //https://stackoverflow.com/questions/25099802/combination-of-elements
  def combinations[T](seq: Seq[T], size: Int): Seq[Seq[T]] = {
    List(size).flatMap(i => seq.combinations(i).flatMap(_.permutations))
  }

  def dominance_skip(point:Point,counts:scala.collection.Map[Int,Long],skip_tree:Map[Int,List[Int]]): Point = {

    var dominated_cells = List.empty[Int]
    try{
      dominated_cells = skip_tree(point.grid_pos)
    }catch{
      case e: Exception => dominated_cells = List(count.toInt)
    }
    var sum = 0
    for((i,points) <- counts){
      if(dominated_cells.contains(i)){
        sum = sum + points.toInt
      }
    }
    point.dominance_score = point.dominance_score+sum

    point
  }
  private def grid_dominance(grid: Map[Int,List[BigDecimal]], cells_per_dimension: Int): Map[Int,List[Int]] = {

    var dominance = Map.empty[Int,List[Int]]
    val grid_length = pow(cells_per_dimension,dimensionality).toInt

    var possible_positions = List.fill(dimensionality)(0)
    for (i <- 1 until cells_per_dimension){
      possible_positions = possible_positions++List.fill(dimensionality)(i)
    }

    val all_cells = combinations(possible_positions,dimensionality)

    for (i <- 0 until grid_length) {

      for (j <- 0 until grid_length) {

        val pos = calc_pos(grid,all_cells(i).toList)
        val posj = calc_pos(grid,all_cells(j).toList)

        if (dominates_int(all_cells(i),all_cells(j))){

          if(dominance.contains(pos)){
            val new_dominance = dominance(pos)++List(posj)
            dominance = dominance + (pos -> new_dominance)
          }else{
            dominance = dominance + (pos -> List(posj))
          }

        }

      }

    }

    dominance

  }

  def hash_coords(p:Point): Double = {
    var sum = 0d
    for(i <- p.dimensionValues){
      sum += i
    }
    sum*pow(p.dimensionValues(0),2)
  }
  def calculate(k:Int,candidates:org.apache.spark.rdd.RDD[Point]): org.apache.spark.rdd.RDD[Point] = {

    val (grid,len) = make_grid() // construct the grid

    val cells_per_dimension = pow(len,1f/dimensionality).toInt // number of cells on each dimension, we'll need this

    val grid_shortcut = grid_dominance(grid,cells_per_dimension) // create a map of which grid cells dominate which

    val located_points = points_set.map(point => locate_point(grid,point)) // store which cell each point belongs to
    val located_candidates = candidates.map(point => locate_point(grid,point))

    val counts = located_points.map(point => point.grid_pos).countByValue() // store how many points are in each cell

    var tiers = Array.fill(cells_per_dimension)(0)
    for (i <- 0 until cells_per_dimension) {
      tiers(i) = ((len - 1) / (cells_per_dimension - 1)) * i
    } // Each "tier" represents a different set of dominated points we'll be looking at to find our top-k points
    val range = (0 until len).toArray

    // Loop takes a subset of dominant points to check, based on the mininum "tier" we need to reach to have enough points
    var totalpoints = 0
    var finaltier = 0
    var flag = true
    var i = 0
    while(flag){
      val tier_cells = range.diff(grid_shortcut(tiers(i)))
      val points_in_this_tier = located_candidates.filter(f => tier_cells.contains(f.grid_pos)).count()
      totalpoints += points_in_this_tier.toInt
      if(totalpoints >= k){
        finaltier = tiers(i)
        flag=false
      }
      i += 1
    }

    val tier_cells = range.diff(grid_shortcut(finaltier)) // cells involved in the "tier" we're checking

    val chosen_candidates = located_candidates.filter(f => tier_cells.contains(f.grid_pos)) // possibly dominant points we will investigate

    val to_check = located_points.filter(f => tier_cells.contains(f.grid_pos)) // dominated points we will have to perform dominance calculations for - the rest are found easily

    val pairs = chosen_candidates.cartesian(to_check) // dominant candidates and dominated points are zipped to tuples

    val split_counts = pairs.map(t => dominance_count(t._1, t._2)).map(point=>(hash_coords(point) -> point)).reduceByKey((a,b)=>a.sum_dominance(b)).map(k=>k._2)

    val final_counts = split_counts.map(point => dominance_skip(point, counts, grid_shortcut)).sortBy(point => point.dominance_score, ascending = false) // final counts are performed and tallied

    final_counts

  }
}

object Top_K_Dominating_Points {

  def main(args: Array[String]): Unit = {

    // File location and number of top dominant points to return are passed as args to the main function
    //val inputFile = args(0)
    val dominant_points = args(1).toInt
    val executors = args(2)

    val sparkConf = new SparkConf()
      .setMaster(s"local[$executors]")
      .setAppName("DominanceQuery")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val fileNames = List(
      "Distribution Datasets/Uniform_Data_2D.txt",
      "Distribution Datasets/Uniform_Data_2D.txt",
      "Distribution Datasets/Uniform_Data_2D.txt",
      "Distribution Datasets/Normal_Data_2D.txt",
      "Distribution Datasets/Normal_Data_4D.txt",
      "Distribution Datasets/Normal_Data_6D.txt",
      "Distribution Datasets/Uniform_Data_2D.txt",
      "Distribution Datasets/Uniform_Data_4D.txt",
      "Distribution Datasets/Uniform_Data_6D.txt",
      "Distribution Datasets/Correlated_Data_2D.txt",
      "Distribution Datasets/Correlated_Data_4D.txt",
      "Distribution Datasets/Correlated_Data_6D.txt",
      "Distribution Datasets/Anticorrelated_Data_2D.txt",
      "Distribution Datasets/Anticorrelated_Data_4D.txt",
      "Distribution Datasets/Anticorrelated_Data_6D.txt",
    )

    for (inputFile <- fileNames) {
      val start = System.nanoTime()

      val txtFile = sc.textFile(inputFile)

      val allPoints = txtFile.map(line => line.split(","))
        .map(line => line.map(elem => elem.toDouble))
        .map(array => Point(array))

      val calc = new Top_K_Dominating_Points_Calculation(allPoints)

      val top_points = calc.calculate(dominant_points, allPoints).map(f => f.toString).take(dominant_points)

      val dur = (System.nanoTime().toDouble - start.toDouble) / 1000000000

      println(s"\nThreads: $executors")
      println(s"Input File: $inputFile")
      println(s"Time Elapsed: $dur seconds")
    }

  }
}
