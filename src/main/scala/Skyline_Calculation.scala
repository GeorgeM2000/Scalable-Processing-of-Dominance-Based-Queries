import org.apache.spark.TaskContext

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import scala.util.control.Breaks.{break, breakable}


case class Skyline_Indices(localPartitionLength: Int, var localSkylineIndices: List[Int] = List())


object Skyline_Calculation {


  // Function to create the filtered list
  def findLocalDominatedPoints(length: Int, excludedIndices: List[Int]): List[Int] = {
    (0 until length).filterNot(excludedIndices.contains).toList
  }

  // Check if p1 dominates p2
  def dominates(p1: Point, p2: Point): Boolean = {
    var lessThanOrEqual = true
    var anyStrictlyLess = false

    for (i <- p1.dimensionValues.indices) {
      if (p1.dimensionValues(i) > p2.dimensionValues(i)) {
        lessThanOrEqual = false
      } else if(p1.dimensionValues(i) < p2.dimensionValues(i)) {
        anyStrictlyLess = true
      }
    }

    if(lessThanOrEqual & anyStrictlyLess) {
      true
    } else if(!lessThanOrEqual) {
      false
    } else {
      false
    }
  }


  def storeLocalSkylineIndices(localSkylineIndices: Skyline_Indices): Unit = {
    val partitionId = TaskContext.getPartitionId()
    val objectOutputStream = new ObjectOutputStream(new FileOutputStream(s"Input_Partitions/partition_$partitionId.ser"))
    objectOutputStream.writeObject(localSkylineIndices)
    objectOutputStream.close()
  }

  def loadLocalSkylineIndices(): Skyline_Indices = {
    val partitionId = TaskContext.getPartitionId()
    val objectInputStream = new ObjectInputStream(new FileInputStream(s"Input_Partitions/partition_$partitionId.ser"))
    val loadedObject = objectInputStream.readObject().asInstanceOf[Skyline_Indices]
    objectInputStream.close()

    loadedObject
  }




  def computeLocalSkylineBaseline(Data: Iterator[Point]): Iterator[Point] = {
    var points = Data.toList

    // For testing purposes
    var lIndex = 0
    for(point <- points) {
      point.localIndex = lIndex
      lIndex += 1
    }


    var i = 0
    var listLength = points.length
    //var lIndex = 0
    val localSkylineIndices = Skyline_Indices(listLength)

    //points.head.localIndex = lIndex
    while (i < listLength - 1) {
      var k = i + 1

      while (k < listLength) {

        /*
        if(points(k).localIndex == -1) {
          lIndex += 1
          points(k).localIndex = lIndex
        }
         */


        if (dominates(points(i), points(k))) {
          points = points.take(k) ++ points.drop(k + 1)
          k = k - 1
          listLength = listLength - 1
        }
        else if (dominates(points(k), points(i))) {
          points = points.take(i) ++ points.drop(i + 1)
          listLength = listLength - 1
          i = i - 1
          k = listLength
        }
        k = k + 1
      }
      i = i + 1
    }


    for(skylinePoint <- points) {
      localSkylineIndices.localSkylineIndices = localSkylineIndices.localSkylineIndices :+ skylinePoint.localIndex
    }
    storeLocalSkylineIndices(localSkylineIndices)


    points.iterator
  }


  def computeGlobalSkylineBaseline(Data: Iterator[Point]): Iterator[Point] = {
    val Dominated_Points = ListBuffer[Point]()
    var points = Data.toList
    var i = 0
    var listLength = points.length

    while (i < listLength - 1) {
      var k = i + 1

      while (k < listLength) {
        if (dominates(points(i), points(k))) {
          Dominated_Points += points(k)
          points = points.take(k) ++ points.drop(k + 1)
          k = k - 1
          listLength = listLength - 1
        }
        else if (dominates(points(k), points(i))) {
          Dominated_Points += points(i)
          points = points.take(i) ++ points.drop(i + 1)
          listLength = listLength - 1
          i = i - 1
          k = listLength
        }
        k = k + 1
      }
      i = i + 1
    }

    for(s <- points) {
      for(d <- Dominated_Points.toList) {
        if(dominates(s, d)) {
          s.dominance_score += 1
        }
      }
    }
    points.iterator
  }
}
