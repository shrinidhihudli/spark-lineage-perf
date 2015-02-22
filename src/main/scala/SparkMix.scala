import java.io.{FileWriter, File, PrintWriter, FileInputStream}
import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by shrinidhihudli on 2/10/15.
 */

object SparkMix {

  def main (args: Array[String]) {

    val properties = SparkMixUtils.loadPropertiesFile()
    val datasize = args(0)
    val pigmixPath = properties.getProperty("pigMix") + "pigmix_" + datasize + "/"
    val outputRoot = properties.getProperty("output") + "pigmix_" + datasize + "_" + (System.currentTimeMillis()/100000 % 1000000) + "/"

    new File(outputRoot).mkdir()

    val conf = new SparkConf().setAppName("SparkMix").setMaster("local")
    val sc = new SparkContext(conf)

    val L1time = L1.run(sc,pigmixPath,outputRoot + "L1out")
    val L2time = L2.run(sc,pigmixPath,outputRoot + "L2out")
    val L3time = L3.run(sc,pigmixPath,outputRoot + "L3out")
    val L4time = L4.run(sc,pigmixPath,outputRoot + "L4out")
    val L5time = L5.run(sc,pigmixPath,outputRoot + "L5out")
    val L6time = L6.run(sc,pigmixPath,outputRoot + "L6out")
    val L7time = L7.run(sc,pigmixPath,outputRoot + "L7out")
    val L8time = L8.run(sc,pigmixPath,outputRoot + "L8out")
    val L9time = L9.run(sc,pigmixPath,outputRoot + "L9out")
    val L10time = L10.run(sc,pigmixPath,outputRoot + "L10out")
    val L11time = L11.run(sc,pigmixPath,outputRoot + "L11out")
    val L12time = L12.run(sc,pigmixPath,outputRoot + "L12out")
    val L13time = L13.run(sc,pigmixPath,outputRoot + "L13out")
    val L14time = L14.run(sc,pigmixPath,outputRoot + "L14out")
    val L15time = L15.run(sc,pigmixPath,outputRoot + "L15out")
    val L16time = L16.run(sc,pigmixPath,outputRoot + "L16out")
    val L17time = L17.run(sc,pigmixPath,outputRoot + "L17out")

    val pw = new PrintWriter(new File(outputRoot + "time.txt"))

    pw.append(datasize + "\t" + properties.getProperty("PARALLEL") + "\n")
    pw.append("L1: " + (L1time/1000.0).toString + " s\n")
    pw.append("L2: " + (L2time/1000.0).toString + " s\n")
    pw.append("L3: " + (L3time/1000.0).toString + " s\n")
    pw.append("L4: " + (L4time/1000.0).toString + " s\n")
    pw.append("L5: " + (L5time/1000.0).toString + " s\n")
    pw.append("L6: " + (L6time/1000.0).toString + " s\n")
    pw.append("L7: " + (L7time/1000.0).toString + " s\n")
    pw.append("L8: " + (L8time/1000.0).toString + " s\n")
    pw.append("L9: " + (L9time/1000.0).toString + " s\n")
    pw.append("L10: " + (L10time/1000.0).toString + " s\n")
    pw.append("L11: " + (L11time/1000.0).toString + " s\n")
    pw.append("L12: " + (L12time/1000.0).toString + " s\n")
    pw.append("L13: " + (L13time/1000.0).toString + " s\n")
    pw.append("L14: " + (L14time/1000.0).toString + " s\n")
    pw.append("L15: " + (L15time/1000.0).toString + " s\n")
    pw.append("L16: " + (L16time/1000.0).toString + " s\n")
    pw.append("L17: " + (L17time/1000.0).toString + " s\n")

    pw.close()
  }
}
