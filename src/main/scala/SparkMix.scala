import java.io.{File, PrintWriter, FileInputStream}
import java.util.Properties

/**
 * Created by shrinidhihudli on 2/10/15.
 */

object SparkMix {

  def main (args: Array[String]) {

    val start = System.currentTimeMillis()

    L1.run("/output2/L1out")
    L2.run("/output2/L2out")
    L3.run("/output2/L3out")
    L4.run("/output2/L4out")
    L5.run("/output2/L5out")
    L6.run("/output2/L6out")
    L7.run("/output2/L7out")
    L8.run("/output2/L8out")
    L9.run("/output2/L9out")
    L10.run("/output2/L10out")
    L11.run("/output2/L11out")
    L12.run("/output2/L12out")
    L13.run("/output2/L13out")
    L14.run("/output2/L14out")
    L15.run("/output2/L15out")
    L16.run("/output2/L16out")

    val stop = System.currentTimeMillis()

    val pw = new PrintWriter(new File("/output2/time.txt"))
    pw.write(((start - stop)/1000.0).toString + "s")
  }
}
