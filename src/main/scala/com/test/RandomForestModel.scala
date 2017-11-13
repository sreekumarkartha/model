/*
 * Gopinathan Munappy, 17/07/2017
 * Bala,			 17/07/2017  
 */
package com.test
import com.test.config.ConfigurationFactory
import com.test.config.objects.Config

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.tuning.{ ParamGridBuilder, CrossValidator }
import org.apache.spark.ml.{ Pipeline, PipelineStage }
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Estimator

import org.apache.log4j.Logger;
//---------------------------------------------------------------------------------------------------------//
// create a serializable RandomForestModel
@SerialVersionUID(123L)	
object RandomForestModel extends Serializable  {
	
			case class Credit(
				creditability: Double,
				balance: Double,
				duration: Double,
				history: Double,
				purpose: Double,
				amount:  Double,
				savings: Double,
				employment: Double,
				instPercent: Double,
				sexMarried: Double,
				guarantors: Double,
				residenceDuration: Double,
				assets: Double,
				age: Double,
				concCredit: Double,
				apartment: Double,
				credits: Double,
				occupation: Double,
				dependents: Double,
				hasPhone: Double,
				foreign: Double
				)
				private[this] lazy val logger = Logger.getLogger(getClass)
				private[this] val config = ConfigurationFactory.load()
				
				val spark = SparkSession.builder()
				.appName("Random Forest Model")
				.config("spark.master", "local[*]")
				.getOrCreate
				
				spark.sparkContext.setLogLevel("ERROR")
							
				val creditHistoryData: String =("file:///home/cloudera/Real-Time-Credit-Risk-Analysis/germancredit.csv")
				val modelSavePath: String = "/home/cloudera/Real-Time-Credit-Risk-Analysis/save/"
				
				val sqlContext= new org.apache.spark.sql.SQLContext(spark.sparkContext)
				import sqlContext._
				import sqlContext.implicits._
//-------------------------------------------------------------------------------------------------------------//
				// main method
				def main(args: Array[String]) {
				// History: Get a RDD from history data file
				val creditHistoryRDD = (spark.sparkContext).textFile(creditHistoryData)
				println("1")
				// Get a dataframe from a RDD to get trainingdata,testdata. 
				val creditHistoryDF: org.apache.spark.sql.Dataset[Row] = getDataFrameFromFile(creditHistoryRDD)
				println("2")
				// Get training and Testing data
				val data: Array[org.apache.spark.sql.Dataset[Row]] = getData(creditHistoryDF)
				println("3")
				val trainingData: org.apache.spark.sql.Dataset[Row] = data(0).cache
				println("4")
				val testData: org.apache.spark.sql.Dataset[Row] = data(1) 
				println("5")								
				// Generate a Random Forest model from existing historical data
				val model: org.apache.spark.ml.classification.RandomForestClassificationModel = getRandomForestModel(data)
				println("6")	
				model.write.overwrite().save(modelSavePath) 
				} // main
 //--------------------------------------------------------------------------------------------------------------------------//
			// Parsing Credit data
			def parseCredit(line: Array[Double]): Credit = {
				Credit(
				line(0),
				line(1) - 1,
				line(2),
				line(3),
				line(4),
				line(5),
				line(6) - 1,
				line(7) - 1,
				line(8),
				line(9) - 1,
				line(10) - 1,
				line(11) - 1,
				line(12) - 1,
				line(13),
				line(14) - 1,
				line(15) - 1,
				line(16) - 1,
				line(17) - 1,
				line(18) - 1,
				line(19) - 1,
				line(20) - 1)
				}
//----------------------------------------------------------------------------------------------------------//				
			// Parsing Credit RDD
			def parseRDD(rdd: RDD[String]): RDD[Array[Double]] = {
				rdd.map(_.split(",")).map(_.map(_.toDouble))
				}
//----------------------------------------------------------------------------------------------------------//				
			// Get a dataframe from a file location
			def getDataFrameFromFile(creditHistoryRDD: org.apache.spark.rdd.RDD[String]): org.apache.spark.sql.Dataset[Row] = {
				val creditHistoryDF = parseRDD(creditHistoryRDD).map(parseCredit).toDF().cache()
				return creditHistoryDF
				}
//----------------------------------------------------------------------------------------------------------//				
			//Function to get training and test data
			def getData(creditHistoryDF: org.apache.spark.sql.Dataset[Row]): Array[org.apache.spark.sql.Dataset[Row]] = {
			// Feature Engineering for credit history data
			println("5A")
				val featureCols = Array("balance", "duration", "history", "purpose", "amount",
					"savings", "employment", "instPercent", "sexMarried", "guarantors",
					"residenceDuration", "assets", "age", "concCredit", "apartment",
					"credits", "occupation", "dependents", "hasPhone", "foreign")
					println("5B")
				val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
				println("5C")
				val df2 = assembler.transform(creditHistoryDF)
				println("5D")
				df2.show

				val labelIndexer = new StringIndexer().setInputCol("creditability").setOutputCol("label")
				println("5E")
				val df3 = labelIndexer.fit(df2).transform(df2)
				println("5F")
				df3.show
				val splitSeed = 5043
				println("5G")
				//return  Array(trainingData, testData) = df3.randomSplit(Array(0.7, 0.3), splitSeed)
				val splits: Array[org.apache.spark.sql.Dataset[Row]] =  df3.randomSplit(Array(0.7, 0.3), splitSeed)
				println("5H")
				return splits
			}
//----------------------------------------------------------------------------------------------------------//				
			// Get a Random Forest model from a dataframe created from historical credit data
			def getRandomForestModel(data: Array[org.apache.spark.sql.Dataset[Row]]): org.apache.spark.ml.classification.	
				RandomForestClassificationModel = {

				val trainingData: org.apache.spark.sql.Dataset[Row] = data(0).cache()
				val testData: org.apache.spark.sql.Dataset[Row] = data(1) 

				val classifier = new RandomForestClassifier()
				.setImpurity("gini")
				.setMaxDepth(3).setNumTrees(20)
				.setFeatureSubsetStrategy("auto")
				.setSeed(5043)
				
				val model = classifier.fit(trainingData)
				model.toDebugString
				
				val predictions = model.transform(testData)
				
				val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
				val accuracy = evaluator.evaluate(predictions)
				
				println("Accuracy before pipeline fitting" + accuracy)
				
				val rm = new RegressionMetrics(predictions.select("prediction", "label").rdd.map(x =>
				(x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))
				)
				
				println("MSE: " + rm.meanSquaredError)
				println("MAE: " + rm.meanAbsoluteError)
				println("RMSE Squared: " + rm.rootMeanSquaredError)
				println("R Squared: " + rm.r2)
				println("Explained Variance: " + rm.explainedVariance + "\n")

				val paramGrid = new ParamGridBuilder()
				.addGrid(classifier.maxBins, Array(25, 31))
				.addGrid(classifier.maxDepth, Array(5, 10))
				.addGrid(classifier.numTrees, Array(20, 60))
				.addGrid(classifier.impurity, Array("entropy", "gini"))
				.build()

				val steps: Array[PipelineStage] = Array(classifier)
				val pipeline = new Pipeline().setStages(steps)

				val cv = new CrossValidator()
				.setEstimator(pipeline)
				.setEvaluator(evaluator)
				.setEstimatorParamMaps(paramGrid)
				.setNumFolds(10)

				val pipelineFittedModel = cv.fit(trainingData)

				val predictions2 = pipelineFittedModel.transform(testData)
				val accuracy2 = evaluator.evaluate(predictions2)
				println("Accuracy after pipeline fitting" + accuracy2)

				println(pipelineFittedModel.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages(0))

				pipelineFittedModel.bestModel
				.asInstanceOf[org.apache.spark.ml.PipelineModel]
				.stages(0)
				.extractParamMap

				val rm2 = new RegressionMetrics(predictions2.select("prediction", "label").rdd.map(x =>
				(x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))

				println("MSE: " + rm2.meanSquaredError)
				println("MAE: " + rm2.meanAbsoluteError)
				println("RMSE Squared: " + rm2.rootMeanSquaredError)
				println("R Squared: " + rm2.r2)
				println("Explained Variance: " + rm2.explainedVariance + "\n")
		
				// Calculate Binary Classification Metrics**
				val predictionAndLabels =predictions.select("prediction", "label")
				.rdd.map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))
	
				val metrics = new BinaryClassificationMetrics(predictionAndLabels)
				// A Precision-Recall curve plots (precision, recall) points for different threshold values, 
				// while a receiver operating 
				// characteristic, or ROC, curve plots (recall, false positive rate) points.
				println("Area under the precision-recall curve: " + metrics.areaUnderPR)
				println("Area under the receiver operating characteristic (ROC) curve : " + metrics.areaUnderROC)

				return model
				} //getaRandomForestModel
} // object

