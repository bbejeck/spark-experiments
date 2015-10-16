package bbejeck.implicits

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * Created by bbejeck on 10/12/15.
 *
 */

object GroupingRDDUtils {


  implicit class GroupingRDDFunctions[K: ClassTag, V: ClassTag](self: RDD[(K, V)]) extends Logging with Serializable {

    implicit def intToDouble(num: V): Double = {
        num match {
          case i: Int => i.toDouble
          case _ => num.asInstanceOf[Double]
        }
    }


    def groupByKeyToList(): RDD[(K, ListBuffer[V])] = {
      val initialSet = ListBuffer.empty[V]
      val addToSet = (s: ListBuffer[V], v: V) => s += v
      val mergePartitionSets = (p1: ListBuffer[V], p2: ListBuffer[V]) => p1 ++= p2

      self.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
    }

    def groupByKeyUnique(): RDD[(K, mutable.HashSet[V])] = {
      val initialSet = mutable.HashSet.empty[V]
      val addToSet = (s: mutable.HashSet[V], v: V) => s += v
      val mergePartitionSets = (p1: mutable.HashSet[V], p2: mutable.HashSet[V]) => p1 ++= p2

      self.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
    }

    def countByKey(): RDD[(K, Int)] = {
      val initialCount = 0
      val addToCounts = (n: Int, v: V) => n + 1
      val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2

      self.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
    }

    def sumWithTotal(): RDD[(K, (Int, Double))] = {

      self.aggregateByKey((0, 0.0))(takeTupleAndValue, sumTuples)
    }

    def averageByKey(): RDD[(K, Double)] = {

      self.sumWithTotal().map(t => averagingFunction(t))

    }


    private def averagingFunction(t: (K, (Int, Double))): (K, Double) = {
      val (name, (numberScores, totalScore)) = t
      (name, totalScore / numberScores)
    }

    private def takeTupleAndValue(t: (Int, Double), v: V): (Int, Double) = {
      (t._1 + 1, t._2 + v)
    }

    private def sumTuples(t: (Int, Double), t2: (Int, Double)): (Int, Double) = {
      val (numScores1, totalScore1) = t
      val (numScores2, totalScore2) = t2

      (numScores1 + numScores2, totalScore1 + totalScore2)
    }

  }

}
