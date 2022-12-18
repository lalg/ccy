package usd.modeling

import java.sql.Date
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class RegressionInput(
  startDate : Date,
  endDate : Date,
  featureColumns : Seq[String],
  modelingDf : DataFrame,
  rowCount : Long) {

  def labelCount =
    modelingDf
      .groupBy("label")
      .agg(count("*"))

  def featuresDate =
    modelingDf.selectExpr(featureColumns: _*)
}



