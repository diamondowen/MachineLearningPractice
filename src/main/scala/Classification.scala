import CustomTransformers.{SentimentAdder, PunctuationRemoveTokenizer, WordCounter}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, BinaryClassificationEvaluator}
import org.apache.spark.ml.feature.{CountVectorizer, SQLTransformer, HashingTF, Tokenizer}
import org.apache.spark.sql.SQLContext

/**
 * Created by XiangChen1 on 8/17/16.
 */
object Classification extends SparkMLHelper{
  private def predictingSentimentFromProductReviews(sqlContext: SQLContext, datasetLocation: String) = {
    //1. Data preparation
    val products = loadCSVData(sqlContext, datasetLocation)

    products.show
    products.printSchema()

    //2. Build the word count vector for each review
    val tokenizer = new PunctuationRemoveTokenizer()
      .setInputCol("review")
      .setOutputCol("cleaned_review")

    val wordCounter = new CountVectorizer()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("word_count")


    //3. Extract sentiments
    // 3.1 we will ignore all the reviews with rating = 3, since they tend to have a neutral sentiment.
    val sqlRemoveRating3 = new SQLTransformer().setStatement("SELECT * from __THIS__ where rating != 3")
    val sentimentAdder = new SentimentAdder()
      .setInputCol("rating")
      .setOutputCol("sentiment")

    val dataPreparationPipeline = new Pipeline().setStages(Array(tokenizer, wordCounter, sqlRemoveRating3, sentimentAdder))
    val productWithSentiment = dataPreparationPipeline.fit(products).transform(products)

    val splits = productWithSentiment.randomSplit(Array(0.8, 0.2))
    val (train_data, test_data) = (splits(0), splits(1))

    val lr = new LogisticRegression()
      .setFitIntercept(true)
      .setFeaturesCol(wordCounter.getOutputCol) // note that features must be Vector
      .setLabelCol(sentimentAdder.getOutputCol) // must be double type

    val lrModel = lr.fit(train_data)
    val weights = lrModel.coefficients
    println(weights.toArray.count(_ > 0))
    println(weights.toArray.count(_ < 0))
    val predictions = lrModel.transform(test_data)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(sentimentAdder.getOutputCol)
      .setMetricName("precision")

    val accuracy = evaluator.evaluate(predictions)

    println("Accuracy: " + accuracy)
  }


  def main(args: Array[String]) {
    val (sc, sqlContext) = initializeSpark()

    predictingSentimentFromProductReviews(sqlContext, datasetLocation = "/Users/XiangChen1/Dropbox/Coursera/MachineLearningUW/Classification/Week1/amazon_baby.csv")

  }

  
}
