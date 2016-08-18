package CustomTransformers

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._

/**
 * Created by XiangChen1 on 8/17/16.
 */
class SentimentAdder(override val uid: String) extends UnaryTransformer[String, Double, SentimentAdder] {
  def this() = this(Identifiable.randomUID("sentimentadder"))

  override def createTransformFunc: String => Double = {
    def myTransform(str: String): Double = {
      str match {
        case "5" | "4" => 1.0
        case _ => 0.0  // classification labels must be between 0 to 1
      }
    }

    myTransform
  }

  override def outputDataType: DataType = DoubleType

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == StringType, s"Input type must be string type but got $inputType.")
  }

  override def copy(extra: ParamMap): SentimentAdder = defaultCopy(extra)
}
