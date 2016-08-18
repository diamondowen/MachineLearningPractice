package CustomTransformers

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._

/**
 * Created by XiangChen1 on 8/17/16.
 */
class WordCounter(override val uid: String) extends UnaryTransformer[Seq[String], Map[String, Int], WordCounter]{

  def this() = this(Identifiable.randomUID("wordcounter"))

  override def createTransformFunc: Seq[String] => Map[String, Int] = {
    _.map(_.toLowerCase).groupBy(x => x).map{case (str, seq) => (str, seq.size)}
  }

  override def outputDataType: DataType = new MapType(keyType = StringType, valueType = IntegerType, valueContainsNull = false)

  override protected def validateInputType(inputType: DataType): Unit = {
    require(inputType == ArrayType(StringType, true), s"Input type must be string type but got $inputType.")
  }

  override def copy(extra: ParamMap): WordCounter = defaultCopy(extra)
}
