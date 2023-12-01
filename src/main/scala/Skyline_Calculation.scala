import org.apache.spark.TaskContext

import java.io.{BufferedReader, File, FileReader, PrintWriter}
import scala.collection.mutable.ListBuffer
import scala.util.hashing.MurmurHash3


object Skyline_Calculation {


  def hashMapping(dPoint: Array[Double]): Int = {
    // Convert the d-dimensional point to a string for hashing
    val pointStr = dPoint.mkString(",")

    // Calculate the hash value
    val originalHash = MurmurHash3.stringHash(pointStr)

    originalHash.toInt
  }

  // Check if p1 dominates p2
  def dominates(p1: Point, p2: Point): Boolean = {
    p1.dimensionValues.zip(p2.dimensionValues).forall { case (x, y) => x <= y } && p1.dimensionValues.exists(_ < p2.dimensionValues.head)
  }

  def saveDominatedPointsToLocalFileSystem(dominatedPoints: ListBuffer[Point]): Unit = {

    // Assuming each partition writes to a separate file
    val partitionId = TaskContext.getPartitionId()
    val outputFilePath = new File(s"Input/partition_$partitionId.txt")

    // Open a new PrintWriter for writing Dominated_Points
    val writer = new PrintWriter(outputFilePath)

    // Iterate over the ListBuffer and write each point to the file
    dominatedPoints.foreach { point =>
      // Convert point to a string representation as needed
      writer.println(point.dimensionValues.mkString(","))
    }

    // Close the writer
    writer.close()
  }

  def loadDominatedPointsFromLocalFileSystem(): List[Point] = {
    // Create an empty ListBuffer to store Dominated_Points
    val dominatedPoints = ListBuffer[Point]()

    // Assuming each partition wrote to a separate file
    val partitionId = TaskContext.getPartitionId()
    val inputFilePath = new File(s"Input/partition_$partitionId.txt")

    // Read Dominated_Points from the file and add them to the ListBuffer
    val reader = new BufferedReader(new FileReader(inputFilePath))
    var line: String = null


    while ( {
      line = reader.readLine(); line != null
    }) {
      val point: Point = {
        // Parse the line and split dimension values
        val values = line.split(",").map(_.trim.toDouble)

        // Create a Point object using the parsed dimension values
        Point(dimensionValues = values)
      }

      dominatedPoints += point
    }

    // Close the reader
    reader.close()

    // Return the ListBuffer containing Dominated_Points
    dominatedPoints.toList
  }

  def computeLocalSkylineBaseline(x: Iterator[Point]): Iterator[Point] = {
    var tempList = x.toList
    var i = 0
    var listLength = tempList.length

    while (i < listLength - 1) {
      var k = i + 1
      while (k < listLength) {
        if (dominates(tempList(i), tempList(k))) {
          tempList(i).dominance_score += 1
          tempList = tempList.take(k) ++ tempList.drop(k + 1)
          k = k - 1
          listLength = listLength - 1

        }
        else if (dominates(tempList(k), tempList(i))) {
          tempList(k).dominance_score += tempList(i).dominance_score + 1
          tempList = tempList.take(i) ++ tempList.drop(i + 1)
          listLength = listLength - 1
          i = i - 1
          k = listLength
        }
        k = k + 1
      }
      i = i + 1
    }
    tempList.iterator
  }


  // Compute the local skyline set S
  def computeLocalSkyline(Data: Iterator[Point]): Iterator[Point] = {


    val S = ListBuffer[Point]()
    val Dominated_Points = ListBuffer[Point]()
    var Points = Data

    while (Points.hasNext) {
      val p1 = Points.next()
      var dominated = false

      val iteratorClone = Points.filter { p2 =>
        if (dominates(p1, p2)) {
          if(!Dominated_Points.contains(p2)) {
            Dominated_Points += p2
          }
          dominated = true
          false
        } else if (dominates(p2, p1)) {
          if (!Dominated_Points.contains(p1)) {
            Dominated_Points += p1
          }
          S -= p1
          true
        } else {
          true
        }
      }

      if (!dominated) {
        S += p1
        Points = iteratorClone
      }
    }

    //saveDominatedPointsToLocalFileSystem(Dominated_Points)

    S.iterator
  }


  // Compute the local skyline set S
  def computeGlobalSkyline(Data: Iterator[Point]): Iterator[Point] = {

    val S = ListBuffer[Point]()
    val Dominated_Points = ListBuffer[Point]()
    var Points = Data

    while (Points.hasNext) {
      val p1 = Points.next()
      var dominated = false

      val iteratorClone = Points.filter { p2 =>
        if (dominates(p1, p2)) {
          if (!Dominated_Points.contains(p2)) {
            Dominated_Points += p2
          }
          dominated = true
          false
        } else if (dominates(p2, p1)) {
          if (!Dominated_Points.contains(p1)) {
            Dominated_Points += p1
          }
          S -= p1
          true
        } else {
          true
        }
      }

      if (!dominated) {
        S += p1
        Points = iteratorClone
      }
    }

    for(s <- S.toList) {
      for(d <- Dominated_Points.toList) {
        if(dominates(s, d)) {
          s.dominance_score += 1
        }
      }
    }

    S.iterator
  }



  // Function to compute dominance scores locally on each worker
  def computeLocalDominanceScores(iterator: Iterator[Point]): Iterator[Point] = {
    val points = iterator.toArray

    for {
      p1 <- points
      p2 <- points if p1 != p2
    } {
      if(dominates(p1, p2)) {
        p1.dominance_score += 1
      }
    }

    points.iterator
  }








}
