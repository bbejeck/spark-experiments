package bbejeck.broadcast

import com.google.common.collect.Table
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Created by bbejeck on 10/6/15.
 *
 * Test for loading guava tables
 */
class GuavaTableLoaderTest extends FunSuite with BeforeAndAfter {

  val basePath = "src/test/resources/guava_loader"
  val fileList = "foo.txt,numbers.txt"
  var refTable: Table[String,String,String] = _

  before {

   refTable = GuavaTableLoader.load(basePath,fileList)

  }

  test("Table is loaded with 2 rows") {
     assert(refTable.rowMap().size() === 2 )
  }

  test("Table contains row 'foo' with 3 entries"){
      assert(refTable.row("foo").size() === 3)
  }

  test("Table contains row 'numbers' with 4 entries"){
      assert(refTable.row("numbers").size() === 4)
  }

  test("Table contains entry foo:curly:moe") {
      assert(refTable.get("foo","curly") === "moe, leader")
  }

  test("Table contains entry numbers:two:double") {
     assert(refTable.get("numbers","two") === "double, means two")
  }

}
