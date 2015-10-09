package bbejeck.broadcast

import com.google.common.collect.{HashBasedTable, Table}

import scala.io.Source

/**
 * Created by bbejeck on 10/4/15.
 *
 */
object GuavaTableLoader {

  //custom type for convenience
  type RefTable = Table[String, String, String]

  def load(path: String, filenames: List[String]): RefTable = {
    val lookupTable = HashBasedTable.create[String, String, String]()
    for (filename <- filenames) {
      val lines = Source.fromFile(path + "/" + filename).getLines()
      val baseFilename = filename.substring(0, filename.indexOf('.'))
      loadFileInTable(lines, baseFilename, lookupTable)
    }

    lookupTable
  }

  def load(path: String, filenames: String): RefTable = {
    val fileList = filenames.split(",").toList
    load(path, fileList)
  }

  private def loadFileInTable(lines: Iterator[String], rowKey: String, tb: RefTable): Unit = {
    for (line <- lines) {
      if (!line.trim().isEmpty) {
        val keyValue = line.split("#")
        tb.put(rowKey, keyValue(0), keyValue(1))
      }
    }
  }
}
