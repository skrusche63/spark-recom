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

import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names

import de.kp.spark.core.model._

import de.kp.spark.recom.RequestContext
import de.kp.spark.recom.model._

import scala.concurrent.Future

abstract class BaseWorker(@transient ctx:RequestContext) extends BaseActor {
 
  override def receive = {

    case req:ServiceRequest => {
      
      val origin = sender 
      val uid = req.data(Names.REQ_UID)

      try {
        val Array(task,topic) = req.task.split(":")
        task match {

          /*
           * A 'predict' request retrieves a rating for a certain dataset
           * provided with this request; the format of the dataset depends
           * on the algorithm specified 
           */
          case "predict" => {

            val missing = missingPredictParams(req)
            origin ! response(req, missing)

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
            origin ! response(req, missing)

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
           * A 'similar' request retrieves items that are similar to a set
           * of items provided with this request
           */
          case "similar" => {

            val missing = missingSimilarParams(req)
            origin ! response(req, missing)

            if (missing == false) {

              val response = doSimilarRequest(req).mapTo[String]
              response.onSuccess {
        
                case result => {
                  val intermediate = Serializer.deserializeResponse(result)
                  origin ! buildSimilarResponse(req,intermediate)
                }

              }
              response.onFailure {
                case throwable => origin ! failure(req,throwable.getMessage)	 	      
	          }
         
            }
      
            context.stop(self)
          
          }
                
          case _ => {
           
            val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
          
            origin ! failure(req,msg)
            context.stop(self)
          
          }
          
        }
      
      } catch {
        
        case e:Exception => {
          
          origin ! failure(req,e.getMessage)
          context.stop(self)
         
        }
      }
    }
   
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! failure(null,msg)
      context.stop(self)
      
    }
  
  }
 
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
   * Methods to support SIMILAR requests
   */
  protected def missingSimilarParams(req:ServiceRequest):Boolean = false

  protected def doSimilarRequest(req:ServiceRequest):Future[Any]

  protected def buildSimilarResponse(request:ServiceRequest, intermediate:ServiceResponse):Any
 
}