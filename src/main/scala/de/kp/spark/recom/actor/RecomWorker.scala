package de.kp.spark.recom.actor
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-Recom project
* (https://github.com/skrusche63/spark-recom).
* 
* Spark-Recom is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Spark-Recom is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Spark-Recom. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.core.model._
import de.kp.spark.recom.model._

import scala.concurrent.Future

abstract class RecomWorker(@transient sc:SparkContext) extends BaseActor {
 
  override def receive = {

    case req:ServiceRequest => {
      
      val origin = sender 
      try {
      
        val uid = req.data("uid")
        req. task.split(":")(0) match {
        
          case "predict" => {

            val missing = missingPredictParams(req)
            sender ! response(req, missing)

            if (missing == false) {

              val response = doPredictRequest(req).mapTo[String]
              response.onSuccess {
        
                case result => {
                  val intermediate = Serializer.deserializeResponse(result)
                  origin ! buildPredictResponse(req,intermediate)
        
                }

              }
              response.onFailure {
                case throwable => origin ! failure(req,throwable.getMessage)	 	      
	          }
         
            }
      
            context.stop(self)
          
          }
        
          case "recommend" => {

            val missing = missingRecommendParams(req)
            sender ! response(req, missing)

            if (missing == false) {

              val response = doRecommendRequest(req).mapTo[String]
              response.onSuccess {
        
                case result => {
                  val intermediate = Serializer.deserializeResponse(result)
                  origin ! buildRecommendResponse(req,intermediate)
                }

              }
              response.onFailure {
                case throwable => origin ! failure(req,throwable.getMessage)	 	      
	          }
         
            }
      
            context.stop(self)
          
          }
      
          /*
           * This request builds implicit user ratings using the
           * remote Akka user preference engine; it is the first
           * step of generating a certain recommender model
           */
          case "build" => {

            val missing = missingBuildParams(req)
            sender ! response(req, missing)

            if (missing == false) {

              cache.addStatus(req,ResponseStatus.BUILDING_STARTED)
              doBuildRequest(req)
          
            }
      
            context.stop(self)
          
          }
        
          /*
           * This request trains a certain recommender model; it is the
           * second step of generating a certain recommender model
           */
          case "train" => {

            val missing = missingTrainParams(req)
            if (missing == false) {
 
              cache.addStatus(req,ResponseStatus.TRAINING_STARTED)
              doTrainRequest(req)

            }
      
            context.stop(self)
          
          }
         
          case _ => {
           
            val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
          
            sender ! Serializer.serializeResponse(failure(req,msg))
            context.stop(self)
          
          }
          
        }
      
      } catch {
        
      case e:Exception => {
          
          sender ! Serializer.serializeResponse(failure(req,e.getMessage))
          context.stop(self)
         
        }
      }
    }
   
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! Serializer.serializeResponse(failure(null,msg))
      context.stop(self)
      
    }
  
  }
  /**
   * Methods to support BUILD requests
   */
  protected def missingBuildParams(req:ServiceRequest):Boolean = false
  
  protected def doBuildRequest(req:ServiceRequest)

  /**
   * Methods to support PREDICT requests
   */
  protected def missingPredictParams(req:ServiceRequest):Boolean = false

  protected def doPredictRequest(req:ServiceRequest):Future[Any]

  protected def buildPredictResponse(request:ServiceRequest, intermediate:ServiceResponse):Any
  /**
   * Methods to support RECOMMEND requests
   */
  protected def missingRecommendParams(req:ServiceRequest):Boolean = false

  protected def doRecommendRequest(req:ServiceRequest):Future[Any]

  protected def buildRecommendResponse(request:ServiceRequest, intermediate:ServiceResponse):Any
  /**
   * Methods to support TRAIN requests
   */
  protected def missingTrainParams(req:ServiceRequest):Boolean = false

  protected def doTrainRequest(req:ServiceRequest)

}