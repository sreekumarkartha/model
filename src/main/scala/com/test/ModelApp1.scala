/*
 * Gopinathan Munappy, 17/07/2017
 * Bala,			 17/07/2017 
 */
package com.test

import com.test.config.ConfigurationFactory
import com.test.config.objects.Config

import com.test.config.ConfigurationFactory
import com.test.utils.JsonUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferBrokers
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
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

import java.io._

object ModelApp1 {
				case class kafkacredit(
					credibility: Double,
					balance: Double,
					duration: Double,
					history: Double,
					purpose: Double,
					amount:  Double,
					savings: Double,
					instPercent: Double,
					guarantors: Double,
					assets: Double,
					concCredit: Double,
					credits: Double,
					sexMarried: Double,
					employment: Double,
					occupation: Double,
					residenceDuration: Double,
					age: Double,
					apartment: Double,
					dependents: Double,
					hasPhone: Double,
					foreignWorker: Double
					)
					
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
	
				var realTimeMessage: String = "" 
			
				private[this] lazy val logger = Logger.getLogger(getClass)
				private[this] val config = ConfigurationFactory.load()
			    
				val spark = SparkSession.builder
				.appName("Credit Prediction")
				.master("local[*]")
				.getOrCreate
					
				val sqlContext= new org.apache.spark.sql.SQLContext(spark.sparkContext)
				import sqlContext._
				import sqlContext.implicits._
//----------------------------------------------------------------------------------------------------------//				
				// main method
				def main(args: Array[String]): Unit = {
 			
					val sleep: Int = 1000
					var key:Int = 0
					var keyMsg:String = ""
				
				    println("Spark Context : " + spark.sparkContext)
				
					val streaming = new StreamingContext(spark.sparkContext, Seconds(config.getStreaming.getWindow))

					val servers = config.getProducer.getHosts.toArray.mkString(",")
					
					val params = Map[String, Object](
					"bootstrap.servers" -> servers,
					"key.deserializer" -> classOf[StringDeserializer],
					"value.deserializer" -> classOf[StringDeserializer],
					"auto.offset.reset" -> "latest",
					"group.id" -> "dashboard",
					"enable.auto.commit" -> (false: java.lang.Boolean)
					)

				// topic names which will be read
					val kafkaTopics = Array(config.getProducer.getKafkaTopic)
		
				// create kafka direct stream object
					val stream = KafkaUtils.createDirectStream[String, String](
					streaming, PreferConsistent, Subscribe[String, String](kafkaTopics, params))
			
				// just alias for simplicity
					type Record = ConsumerRecord[String, String]
					
					stream.foreachRDD((rdd: RDD[Record]) => {
					
				// convert string to PoJo and generate rows as tuple group
					val pairs = rdd.map(row => (row.value()))
				
					val kafkaRDD = pairs.map(line  => line.split(',')).map(line => kafkacredit(
					line(0).replaceAll("kafkacredit\\(" , "").trim.toDouble,
					line(1).trim.toDouble,
					line(2).trim.toDouble,
					line(3).trim.toDouble,
					line(4).trim.toDouble,
					line(5).trim.toDouble,
					line(6).trim.toDouble,
					line(7).trim.toDouble,
					line(8).trim.toDouble,
					line(9).trim.toDouble,
					line(10).trim.toDouble,
					line(11).trim.toDouble,
					line(12).trim.toDouble,
					line(13).trim.toDouble,
					line(14).trim.toDouble,
					line(15).trim.toDouble,
					line(16).trim.toDouble,
					line(17).trim.toDouble,
					line(18).trim.toDouble,
					line(19).trim.toDouble,
					line(20).replaceAll("\\):" , "").trim.toDouble)) 
					
					val sqlContext= new org.apache.spark.sql.SQLContext(spark.sparkContext)
					import sqlContext.implicits._
										
					// Writing to Kafka for downstream apps
					val parsedData = kafkaRDD.foreach(line => {
					val parts = (line + ":")
					var msg: String = parts.replaceAll("kafkacredit\\(" , "").trim
					realTimeMessage = msg.replaceAll("\\):" , "").trim
					println("Real Time Data : " + realTimeMessage)
					val args : Array[String] = Array(realTimeMessage, "")
					CreditMLModel.main(args)
					println("Back here ")
					
//-------------------------------------------------------------------------------------------------------------------//
					}) 	//parsedData.foreach(println)
					
				})  // foreachRDD
		
				// create streaming context and submit streaming jobs
				streaming.start()

				// wait to killing signals etc.
				streaming.awaitTermination()
		}   //main

   }   	//object