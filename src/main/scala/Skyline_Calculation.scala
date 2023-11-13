import scala.collection.mutable.ListBuffer

object Skyline_Calculation {

  // Check if p1 dominates p2
  def dominates(p1: Point, p2: Point): Boolean = {
    p1.dimensionValues.zip(p2.dimensionValues).forall { case (x, y) => x <= y } && p1.dimensionValues.exists(_ < p2.dimensionValues.head)
  }

  // Compute the skyline set S
  def computeSkyline[T](Data: Iterator[Point]): Iterator[Point] = {

    val S = ListBuffer[Point]()
    val Dominated_Points = ListBuffer[Point]()
    var Points = Data

    while (Points.hasNext) {
      val p1 = Points.next()
      var dominated = false

      val iteratorClone = Points.filter { p2 =>
        if (dominates(p1, p2)) {
          Dominated_Points += p2
          dominated = true
          false
        } else if (dominates(p2, p1)) {
          Dominated_Points += p1
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

    /*
    // Calculate dominance score for each of the local skyline points
    for (s <- S.toList.distinct) {
      for (d <- Dominated_Points.toList) {
        if (dominates(s, d)) {
          s.dominance_score += 1
        }
      }
    }
     */

    S.iterator
  }

}
