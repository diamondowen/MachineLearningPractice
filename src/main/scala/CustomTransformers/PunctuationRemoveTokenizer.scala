package CustomTransformers

import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.util.Identifiable

/**
 * Created by XiangChen1 on 8/17/16.
 */
class PunctuationRemoveTokenizer(override val uid: String) extends Tokenizer {
  def this() = this(Identifiable.randomUID("prt"))

  override def createTransformFunc: String => Seq[String] = {
    _.toLowerCase.replaceAll("""[\p{Punct}]\b\p{IsLetter}{1}""", " ").split("\\s+") // remove punctuation and words with size <= 1  http://stackoverflow.com/questions/30074109/removing-punctuation-marks-form-text-in-scala-spark
  }
}
