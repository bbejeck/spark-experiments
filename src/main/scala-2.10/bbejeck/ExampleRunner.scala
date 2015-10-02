package bbejeck

import bbejeck.sorting.SecondarySort

/**
 * Created by bbejeck on 7/31/15.
 */
object ExampleRunner {

  def main(args: Array[String]) = {

      //Grouping.runGroupingExample(args)
      //AggregateByKey.runAggregateByKeyExample()
      //CombineByKey.runCombineByKeyExample()
      //StripFirstLines.stripLinesExample(args)
      //Splitting.runSplitExample()
     // MappingValues.runMappingValues()
      //AirlineFlightPerformance.runInitialFlightPerformanceDataFrame(args(0))
      SecondarySort.runSecondarySortExample(args)



  }

}
