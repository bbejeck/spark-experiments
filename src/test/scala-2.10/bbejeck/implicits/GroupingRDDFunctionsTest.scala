package bbejeck.implicits

import bbejeck.implicits.GroupingRDDUtils._
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FunSuite

import scala.collection.mutable

/**
 * Created by bbejeck on 10/16/15.
 *
 */
class GroupingRDDFunctionsTest extends FunSuite with SharedSparkContext {

  val input = List(("Fred", 80.0), ("Fred", 90), ("Fred", 90.0), ("Wilma", 100.0), ("Wilma", 100))


  test("testSumWithTotal") {
    val peopleScores = sc.parallelize(input)
    val scores = peopleScores.sumWithTotal().collect().toList
    val expected = List(("Fred", (3, 260.0)), ("Wilma", (2, 200.0)))
    assert(scores.contains(expected.head))
    assert(scores.contains(expected(1)))
    assert(scores.size === 2)
  }

  test("testGroupByKeyUnique") {
    val localInput = List(("Fred", 80.0), ("Fred", 90.0), ("Fred", 90.0), ("Wilma", 100.0), ("Wilma", 100.0))
    val peopleScores = sc.parallelize(localInput)
    val scores = peopleScores.groupByKeyUnique().collect().toList
    val expected = List(("Fred", mutable.HashSet(80.0, 90.0)), ("Wilma", mutable.HashSet(100.0)))
    assert(scores.contains(expected.head))
    assert(scores.contains(expected(1)))
    assert(scores.size === 2)
  }

  test("testGroupByKeyUniqueWithWords") {
    val localInput = List(("Fred", "Foo"), ("Fred", "Foo"), ("Fred", "Foo"), ("Wilma", "Bar"), ("Wilma", "Baz"))
    val peopleScores = sc.parallelize(localInput)
    val scores = peopleScores.groupByKeyUnique().collect().toList
    val expected = List(("Fred", mutable.HashSet("Foo")), ("Wilma", mutable.HashSet("Bar","Baz")))
    assert(scores.contains(expected.head))
    assert(scores.contains(expected(1)))
    assert(scores.size === 2)
  }

  test("testCountByKey") {
    val peopleScores = sc.parallelize(input)
    val scores = peopleScores.countByKey().collect().toList
    val expected = List(("Fred", 3), ("Wilma", 2))
    assert(scores.contains(expected.head))
    assert(scores.contains(expected(1)))
    assert(scores.size === 2)

  }

  test("testAverageByKey") {
    val peopleScores = sc.parallelize(input)
    val scores = peopleScores.averageByKey().collect().toList
    val expected = List(("Fred", (80.0 + 90.0 + 90.0) / 3), ("Wilma", 100.0))
    assert(scores.contains(expected.head))
    assert(scores.contains(expected(1)))
    assert(scores.size === 2)

  }

  test("testGroupByKeyToList") {
    val peopleScores = sc.parallelize(input)
    val scores = peopleScores.groupByKeyToList().collect().toList
    val expected = List(("Fred", List(80.0, 90.0, 90.0)), ("Wilma", List(100.0, 100.0)))
    assert(scores.contains(expected.head))
    assert(scores.contains(expected(1)))
    assert(scores.size === 2)
  }

}
