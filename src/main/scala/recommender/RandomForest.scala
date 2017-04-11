package recommender

import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}

/**
  * Created by chen on 2017/4/6.
  */
object RandomForest {
  def main(args: Array[String]): Unit = {
    // Load and parse the data file, converting it to a DataFrame.
    // val data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    val data = inputData.DataHolder.getFinalData()

    val formula = new RFormula()
      .setFormula("cnt ~ year + month + day + hour + temperature + humidity + windspeed + weatherType + holiday + weekday")
      .setFeaturesCol("features")
      .setLabelCol("label")

    val output = formula.fit(data).transform(data)
    output.select("features", "label").show

    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(output)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = output.randomSplit(Array(0.7, 0.3))

    // Train a RandomForest model.
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")

    // Chain indexer and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(featureIndexer, rf))


    // Train model. This also runs the indexer.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("prediction", "label", "features").show(5)
    predictions.select("prediction", "label", "features").write.save("prediction.parquet")

    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

    val rfModel = model.stages(1).asInstanceOf[RandomForestRegressionModel]
    println("Learned regression forest model:\n" + rfModel.toDebugString)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)
  }
}
