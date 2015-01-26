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
import de.kp.spark.recom.model._

import de.kp.spark.recom._
import de.kp.spark.recom.handler.CARHandler

import de.kp.spark.recom.source.EventSource
import scala.concurrent.Future

class CARActor(@transient ctx:RequestContext) extends BaseWorker(ctx) {
  
  private val service = "context"

  /**
   * The user rating is built by delegating the request to the 
   * remote rating service; this Akka service represents the 
   * User Preference engine of Predictiveworks.
   */
  def doBuildRequest(req:ServiceRequest) {
    /*
     * The user specifies the algorithm as 'CAR'; this algorithm
     * is not known by the user preference engine and must be replaced
     * by EPREF, i.e. NPREF and CAR are strongly correlated by this
     * recommender.
     * 
     * EPREF is an event based preference algorithm that is supported by
     * the preference engine
     */
    val excludes = List(Names.REQ_ALGORITHM)
    val data = Map(Names.REQ_ALGORITHM -> "EPREF", Names.REQ_NEXT_ALGORITHM -> "CAR") ++ 
                 req.data.filter(kv => excludes.contains(kv._1) == false)  
    
    val message = Serializer.serializeRequest(new ServiceRequest(req.service,req.task,data))
    /*
     * Building user rating is a fire-and-forget task
     * from the recommendation service prespective
     */
    ctx.send(req.service,message)
    
  }
  /**
   * In the training of a factorization model is delegated
   * to the context-aware analysis engine
   */
  def doTrainRequest(req:ServiceRequest) {
    
    /*
     * Train requests initiated by the Recommender are actually
     * restricted to text file or parquet file data sources
     */
    val source = req.data(Names.REQ_SOURCE)
    
    val sources = List(Sources.FILE,Sources.PARQUET)
    if (sources.contains(source) == false)
      throw new Exception("The CAR algorithm actually only supports text & parquet files as data sources.")
    
    /*
     * Training a factorization model or a correlation matrix is a fire-and-forget 
     * task from the recommendation service prespective; the request task is either
     * specified as 'train:matrix' or 'train:model'
     */
    val message = Serializer.serializeRequest(new ServiceRequest(service,req.task,req.data))
    ctx.send(service,message)
    
  }

  def doPredictRequest(req:ServiceRequest):Future[Any] = {

    val message = new CARHandler(ctx).buildPredictRequest(req)
    ctx.send(service,message)
     
  }

  def buildPredictResponse(req:ServiceRequest,intermediate:ServiceResponse):Any = {
    
    if (intermediate.status == ResponseStatus.SUCCESS) {
      new CARHandler(ctx).buildPredictResponse(req,intermediate)
     
    } else {
      /*
       * In case of an error, send the intermediate message as it contains
       * the specification of the respective failure
       */
      intermediate
      
    }    

  }
  /**
   * A CAR based recommendation request supports two different use cases:
   * 
   * 1) a certain user is provided and those items are determined that are 
   * similar to the active items; different from the similarity request, the 
   * result of the Context-Aware Analysis engine is transformed into a list 
   * of scored fields.
   * 
   * This request requires the existence of an item-based correlation matrix
   * 
   * 2) a certain item is provided and those users are determined that are similar
   * to those that have voted for this item; different from the similarity request, 
   * the result of the Context-Aware Analysis engine is transformed into a list 
   * of scored fields.
   * 
   * This request requires the existence of an user-based correlation matrix
   * 
   */
  def doRecommendRequest(req:ServiceRequest):Future[Any] = {
    
    val message = new CARHandler(ctx).buildRecommendRequest(req)
    ctx.send(service,message)
    
  }

  def buildRecommendResponse(req:ServiceRequest,intermediate:ServiceResponse):Any = {
    
    if (intermediate.status == ResponseStatus.SUCCESS) {
      new CARHandler(ctx).buildRecommendResponse(req,intermediate)
      
    } else {
      /*
       * In case of an error, send the intermediate message as it contains
       * the specification of the respective failure
       */
      intermediate

    }
  
  }

  /*
   * This request determines for each item in a list those items that are most
   * similar; the number of items must be restricted by a request parameter
   */
  def doSimilarRequest(req:ServiceRequest):Future[Any] = {
    
    val message = new CARHandler(ctx).buildSimilarRequest(req)
    ctx.send(service,message)
        
  }
  
  def buildSimilarResponse(req:ServiceRequest,intermediate:ServiceResponse):Any = {
    
    if (intermediate.status == ResponseStatus.SUCCESS) {
      new CARHandler(ctx).buildSimilarResponse(req,intermediate)
      
    } else {
      /*
       * In case of an error, send the intermediate message as it contains
       * the specification of the respective failure
       */
      intermediate

    }
    
  }

}