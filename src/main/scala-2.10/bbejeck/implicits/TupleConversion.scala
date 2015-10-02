package bbejeck.implicits

/**
 * Created by bbejeck on 8/21/15.
 */
object TupleConversion {


  //case class IdText(numberId: Int, text:String)

  implicit def newTypeFromTuple[A,B,U](t:(A,B)) (implicit f:(A,B) => U) : U =  f(t._1,t._2)

  //implicit def convertTuple(t:(Int,String) ): IdText=  new IdText(t._1,t._2)


}
