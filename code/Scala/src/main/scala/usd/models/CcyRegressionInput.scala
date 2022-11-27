package usd.models

import java.sql.Date
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count

case class CcyRegressionInput (
  ccy : String,
  startDate : Date,
  endDate : Date,
  featureColumns : Seq[String],
  dmDf : DataFrame) {

  // assumes "label" is a column
  def labelCount =
    dmDf.groupBy("label")
      .agg(count("*"))

  def featureData : DataFrame =
    dmDf.selectExpr(featureColumns: _*)

  
}


