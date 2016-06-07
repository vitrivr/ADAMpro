package ch.unibas.dmi.dbis.adam.utils

/**
  * Created by silvan on 06.06.16.
  */
trait AdamParUtils {

  /**
    * Jesper @ http://stackoverflow.com/questions/9160001/how-to-profile-methods-in-scala
    * use with time({ methodcall }, "methodName")
    *
    * @param block Block of code to profile
    * @param methodName Description for your code-block
    */
  def time[R](methodName:String) (block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    System.out.println("\nElapsed time for " + methodName + ": " + ((t1-t0)/1e6).toInt/1e3 + "s\n")
    result
  }

}
