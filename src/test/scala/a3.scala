import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.{Matchers, FlatSpec}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
/**
  * Created by QQ on 2016/2/22.
  */
class a3 extends  FlatSpec with Matchers{
  "test " should "work" in{

//    val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))
//    println(pos)
//    val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))
//    println(neg)
    val conf = new SparkConf().setMaster("local").setAppName("BinaryClassificationMetricsExample")
    val sc = new SparkContext(conf)
    val data = MLUtils.loadLibSVMFile(sc, "D:\\QQ\\Desktop\\spark-1.6.0\\spark-1.6.0\\spark-1.6.0" +
        "\\data\\mllib\\sample_binary_classification_data.txt")
    val splits = data.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val test = splits(1).cache()
    val numIterations = 100
    val svmAlg = new SVMWithSGD()
    svmAlg.optimizer.
      setNumIterations(200).
      setRegParam(0.1) setUpdater new L1Updater
    val modelL1 = svmAlg.run(training)
    val model = SVMWithSGD.train(training, numIterations)
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = modelL1.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    }
}
