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
import de.kp.spark.core.redis.RedisDB

import de.kp.spark.core.model._
import de.kp.spark.recom.model._

import de.kp.spark.recom._
import de.kp.spark.recom.als._

import de.kp.spark.recom.hadoop.HadoopIO

import scala.concurrent.Future

class ALSActor(@transient ctx:RequestContext) extends BaseWorker(ctx) {

  val sink = new RedisDB(host,port.toInt)
  
  def doPredictRequest(req:ServiceRequest):Future[Any] = {
    
    val site  = req.data(Names.REQ_SITE)

    val users = req.data(Names.REQ_USERS).split(",").toList
    val items = req.data(Names.REQ_ITEMS).split(",").map(_.toInt).toList
    
    /*
     * The response returned is dynamically derived from the
     * input parameters
     */    
    Future {

      if (users.length == 1 && items.isEmpty == false) {
      
        val user = users(0)
      
        val model = new ALSRecommenderModel(ctx, req)
        val preferences = Preferences(model.predict(site,user,items))
    
        serialize(req.data(Names.REQ_UID),preferences)
      
      } else if (users.length > 1 && items.isEmpty == false) {
      
        val model = new ALSRecommenderModel(ctx, req)
        val preferences = Preferences(model.predict(site,users,items))
    
        serialize(req.data(Names.REQ_UID),preferences)
      
      } else {
      
        val msg = "Provided combination of input parameters is not supported."
        Serializer.serializeResponse(failure(req,msg))  
    
      }
    
    }
  
  }

  def buildPredictResponse(req:ServiceRequest,intermediate:ServiceResponse):Any = {
    
    if (intermediate.status == ResponseStatus.SUCCESS) {
      /*
       * Send the list of prefences back to the requestor
       */
      Serializer.deserializePreferences(intermediate.data(Names.REQ_RESPONSE))
      
    } else {
      /*
       * In case of an error, send the intermediate message as it contains
       * the specification of the respective failure
       */
      intermediate
      
    }
    
  }
  
  def doRecommendRequest(req:ServiceRequest):Future[Any] = {
    
    /*
     * The matrix factorization model does not support 'similar'
     * requests with respect to any predictor variable 
     */
    val topic = req.task.split(":")(1)
    if (List(Topics.ITEM,Topics.USER).contains(topic) == false)
      throw new Exception("This recommendation request is not supported for the ALS algorithm.")
    
    val site  = req.data(Names.REQ_SITE)
    val total = req.data(Names.REQ_TOTAL).toInt

    /*
     * The response returned is dynamically derived from the
     * input parameters
     */
    val user = if (req.data.contains(Names.REQ_USER)) req.data(Names.REQ_USER) else null
    val item = if (req.data.contains(Names.REQ_ITEM)) req.data(Names.REQ_ITEM).toInt else -1
    
    Future {
    
      if (user != null && item == -1) {
        /*
         * Retrieve the top 'k' item recommendations
         * for a certain site and a single user from
         * the trained ALS model  
         */  
        val model = new ALSRecommenderModel(ctx, req)
        val preferences = Preferences(model.recommend(site,user,total))
    
        serialize(req.data(Names.REQ_UID),preferences)
      
      } else if (user == null && item != -1) {
      
        val model = new ALSRecommenderModel(ctx, req)
        val preferences = Preferences(model.recommend(site,item,total))
    
        serialize(req.data(Names.REQ_UID),preferences)
     
      } else {
      
        val msg = "Provided combination of input parameters is not supported."
        Serializer.serializeResponse(failure(req,msg))  
    
      }
    
    }
    
  }
  
  def buildRecommendResponse(req:ServiceRequest,intermediate:ServiceResponse):Any = {
    
    if (intermediate.status == ResponseStatus.SUCCESS) {
      /*
       * Send the list of prefences back to the requestor
       */
      Serializer.deserializePreferences(intermediate.data(Names.REQ_RESPONSE))
      
    } else {
      /*
       * In case of an error, send the intermediate message as it contains
       * the specification of the respective failure
       */
      intermediate
      
    }
    
  }
  /**
   * Similar requests are not supported for matrix factorization
   */
  def doSimilarRequest(req:ServiceRequest):Future[Any] = {
    throw new Exception("Similar requests are not suported for the ALS algorithm.")
  }
  
  def buildSimilarResponse(req:ServiceRequest,intermediate:ServiceResponse):Any = null
  
  /**
   * In order to be compliant with the functionality of the ASR and CAR actor, 
   * we have to serialize the result here
   */   
  private def serialize(uid:String,preferences:Preferences):String = {
     
    val data = Map(Names.REQ_UID -> uid,Names.REQ_RESPONSE -> Serializer.serializePreferences(preferences))
    val response = ServiceResponse("","",data,ResponseStatus.SUCCESS)
      
    Serializer.serializeResponse(response)
     
  }
  
}