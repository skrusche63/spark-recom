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

import de.kp.spark.recom.RemoteContext
import scala.concurrent.Future

class CARActor(@transient sc:SparkContext,rtx:RemoteContext) extends RecomWorker(sc) {
  /**
   * The user rating is built by delegating the request to the 
   * remote rating service; this Akka service represents the 
   * User Preference engine of Predictiveworks.
   */
  def doBuildRequest(req:ServiceRequest) {
      
    val service = req.service
    val message = Serializer.serializeRequest(req)
    /*
     * Building user rating is a fire-and-forget task
     * from the recommendation service prespective
     */
    rtx.send(service,message)
    
  }
  /**
   * In the training of a factorization model is delegated
   * to the context-aware analysis engine
   */
  def doTrainRequest(req:ServiceRequest) {
      
    val service = "context"
    val message = Serializer.serializeRequest(new ServiceRequest(service,"train",req.data))
    /*
     * Training a factorization model is a fire-and-forget task
     * from the recommendation service prespective
     */
    rtx.send(service,message)
    
  }

  def doPredictRequest(req:ServiceRequest):Future[Any] = {

    /*
     * The Context-Aware Analysis engine is a general purpose
     * engine based on factorization models and is capable to
     * predict target variables for feature vectors.
     */   
    val service = "context"
    val message = Serializer.serializeRequest(new ServiceRequest(service,"get:feature",req.data))
    
    rtx.send(service,message)
     
  }

  def buildPredictResponse(req:ServiceRequest,intermediate:ServiceResponse):Any = {
    
    if (intermediate.status == ResponseStatus.SUCCESS) {

      val features = req.data("features").split(",").map(_.toDouble).toList
      val target   = intermediate.data("prediction").toDouble
    
      TargetedPoint(features,target)
     
    } else {
      /*
       * In case of an error, send the intermediate message as it contains
       * the specification of the respective failure
       */
      intermediate
      
    }    

  }

  def doRecommendRequest(req:ServiceRequest):Future[Any] = null

  def buildRecommendResponse(req:ServiceRequest,intermediate:ServiceResponse):Any = null
 
}