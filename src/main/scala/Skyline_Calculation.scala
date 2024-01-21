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
    p1.dimensionValues.zip(p2.dimensionValues).forall { case (x, y) => x <= y } && p1.dimensionValues.exists(_ < p2.dimensionValues.head)
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

  def computeFastSkyline(Data: Iterator[Point]): Iterator[Point] = {
    var arraybuffer = ArrayBuffer[Point]()
    val array = Data.toArray
    arraybuffer += array(0)
    for (i<-1 until array.length)
    {
      var j=0
      var breaked = false
      breakable
      {
        while (j < arraybuffer.length) {
          if (dominates(array(i), arraybuffer(j))) {
            arraybuffer.remove(j)
            j-=1
          }
          else if (dominates(arraybuffer(j), array(i))) {
            breaked = true
            break()
          }
          j += 1
        }
      }
      if(!breaked)
        arraybuffer+=array(i)
    }
    arraybuffer.iterator
  }

  def computeLocalSkylineBaseline(Data: Iterator[Point]): Iterator[Point] = {
    var points = Data.toList
    var i = 0
    var listLength = points.length
    //var lIndex = 0
    //val localSkylineIndices = Skyline_Indices(listLength)

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

    /*
    for(skylinePoint <- points) {
      localSkylineIndices.localSkylineIndices = localSkylineIndices.localSkylineIndices :+ skylinePoint.localIndex
    }

    storeLocalSkylineIndices(localSkylineIndices)

     */

    points.iterator
  }


  // Compute the local skyline set S
  def computeLocalSkylineAltBaseline(Data: Iterator[Point]): Iterator[Point] = {


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




  /*
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

   */

}
