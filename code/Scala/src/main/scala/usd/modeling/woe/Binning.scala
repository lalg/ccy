package usd.modeling.woe


import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import usd.util.CcyLogging
import usd.models.SimpleLinearRegression
import usd.modeling.RegressionInput

private
case class Counts(label: Double, count: Long)

case class Bins(
  bin : Int,
  min : Double,
  max : Double,
  size : Long,
  bi  : Long,
  gi  : Long,
  b   : Long,
  g   : Long,
  woe : Double)


class Binning(ri: RegressionInput, numOfBins: Int=20)(implicit spark : SparkSession)
    extends CcyLogging {

  import spark.implicits._

  def equalSized(columnName: String) = {
    val modelingDf =
      ri.modelingDf
        .select(ri.labelName, columnName)
        .cache()

    val wspec = Window.orderBy(col(columnName).asc)
    val tiled = 
      modelingDf
        .withColumn("bin", ntile(numOfBins) over wspec)

    val minMax  =
      tiled
        .groupBy("bin")
        .agg(
          min(columnName) as "min",
          max(columnName) as "max",
          count("*") as "size")

    val split =
      tiled
        .groupBy("bin", ri.labelName)
        .count()

    val label0 =
      split.filter(col(ri.labelName) === 0)
        .select($"bin", col(ri.labelName), $"count" as "bi")


    val label1 =
      split.filter(col(ri.labelName) === 1)
        .select($"bin", col(ri.labelName), $"count" as "gi")

    val totals : Array[Counts] =
      modelingDf
        .groupBy("label")
        .count()
        .as[Counts]
        .collect()

    val b =
      totals
        .flatMap(cnt => if (cnt.label == 1) Some(cnt) else None)
        .head
        .count

    val g =
      totals
        .flatMap(cnt => if (cnt.label == 0) Some(cnt) else None)
        .head
        .count

    minMax
      .join(label0, Seq("bin"))
      .drop(ri.labelName)
      .join(label1, Seq("bin"))
      .drop(ri.labelName)
      .withColumn("b", lit(b))
      .withColumn("g", lit(g))
      .withColumn("woe", log(($"bi"/$"gi") / (b/g)))
      .as[Bins]
  }

  def linearFit(bins: Dataset[Bins]) = {
    val slr =
      new SimpleLinearRegression(
        designMatrix = bins.toDF(),
        yLabel = "woe",
        xLabels = Seq("bin"))
    slr.regression 
  }

  def featureFit = {
    val names = ri.featureColumns
    val rsquared = 
      names map {case name =>
        val bins = equalSized(name)
        val fit = linearFit(bins)
        (name, fit("rsquared"))
      }
    rsquared.toSeq.toDF("feature", "rsquared")
  }
}


