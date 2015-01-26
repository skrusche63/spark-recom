package de.kp.spark.recom.handler
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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.recom.model._
import de.kp.spark.recom.format.CARFormatter

class CARHandler(@transient sc:SparkContext) {

  /**
   * This method builds a feature vector from the request parameter that can
   * be sent to the Context-Aware Analysis engine. A feature vector is usually
   * used with 'predict' requests
   */
  def buildPredictRequest(req:ServiceRequest):String = {

    val service = "context"
    val task = "predict:vector"
    
    if (req.data.contains(Names.REQ_FEATURES)) {
      /*
       * The required feature vector is already part of the user request;
       * in this case no additional transformation is performed
       */
      return Serializer.serializeRequest(new ServiceRequest(service,task,req.data))
      
    }    
    /*
     * The required feature vector must be created from the request parameters;
     * it is required that the 'user', a certain 'item' of interest and the
     * respective 'context' is provided by the request.
     *
     * This is the common request type, where a certain (user,item) pair and an
     * observed context is provided; in this case, the data are transformed into 
     * the basic feature vector,
     */
    val uid = req.data(Names.REQ_UID)
    if (req.data.contains(Names.REQ_USER) == false) {      
      val msg = String.format("""[UID %s] No 'user' parameter provided.""",uid)
      throw new Exception(msg)      
    }
    
    if (req.data.contains(Names.REQ_ITEM) == false) {      
      val msg = String.format("""[UID %s] No 'item' parameter provided.""",uid)
      throw new Exception(msg)      
    }

    if (req.data.contains(Names.REQ_EVENT) == false) {      
      val msg = String.format("""[UID %s] No 'event' parameter provided.""",uid)
      throw new Exception(msg)      
    }

    if (req.data.contains(Names.REQ_DAY_OF_WEEK) == false) {      
      val msg = String.format("""[UID %s] No 'day of week' parameter provided.""",uid)
      throw new Exception(msg)      
    }
 
    if (req.data.contains(Names.REQ_HOUR_OF_DAY) == false) {      
      val msg = String.format("""[UID %s] No 'hour of day' parameter provided.""",uid)
      throw new Exception(msg)      
    }
 
    /*
     * In the training phase, the related item is the one that was rated just
     * before the active item was rated; in the context of a 'predict' request,
     * we have to generalize this interpretation and consider this second item
     * as one, that has some temporal relation to the active one 
     */
    if (req.data.contains(Names.REQ_RELATED) == false) {      
      val msg = String.format("""[UID %s] No 'related item' parameter provided.""",uid)
      throw new Exception(msg)      
    }
   
    val features = new CARFormatter(sc,req).format.mkString(",")
    
    val data = Map(Names.REQ_FEATURES -> features) ++ req.data     
    return Serializer.serializeRequest(new ServiceRequest(service,task,data))

  }

  def buildPredictResponse(req:ServiceRequest,res:ServiceResponse):Any = {
    
    val target = res.data(Names.REQ_RESPONSE).toDouble
    /*
     * We distinguish between two different requests types: one request provides
     * a completely specified feature vector, and, the other request type comes
     * with event oriented input information and must be transformed into a 
     * feature vector. 
     */
    val filter = List(Names.REQ_USER,Names.REQ_ITEM,Names.REQ_EVENT,Names.REQ_DAY_OF_WEEK,Names.REQ_HOUR_OF_DAY,Names.REQ_RELATED)
    val context = req.data.filter(kv => filter.contains(kv._1))
    
    if (context.size == filter.size) {
      
      PreferenceWithContext(
        /* User & item part */
        req.data(Names.REQ_SITE),
        req.data(Names.REQ_USER),
        req.data(Names.REQ_ITEM),
        /* Context specific information */ 
        req.data(Names.REQ_EVENT),
        req.data(Names.REQ_DAY_OF_WEEK).toInt,
        req.data(Names.REQ_HOUR_OF_DAY).toInt,
        req.data(Names.REQ_RELATED),
        target)
      
    } else if (req.data.contains(Names.REQ_FEATURES)) {
      
      val features = req.data(Names.REQ_FEATURES).split(",").map(_.toDouble).toList    
      TargetedPoint(features,target)
    
    } else {
      throw new Exception("This request type is not supported by the CAR algorithm.")
    }
  
  }
  /**
   * A CAR based recommendation request determines those items that are similar
   * to the active items of a certain user; different from the similarity request,
   * the result of the Context-Aware Analysis engine is transformed into a list
   * of scored fields.
   */
  def buildRecommendRequest(req:ServiceRequest):String = {

    val service = "context"
    val task = "similar:vector"

    val formatter = new CARFormatter(sc,req)
    
    val topic = req.task.split(":")(1)    
    val data = if (topic == Topics.ITEM) {
      /*
       * This request recommends similar items to those that have 
       * been voted by a specified 'user'
       */
      val columns = formatter.itemsAsList.mkString(",")
      
      val excludes = List(Names.REQ_COLUMNS, Names.REQ_MATRIX)
      Map(Names.REQ_COLUMNS -> columns,Names.REQ_MATRIX -> "item") ++  req.data.filter(kv => excludes.contains(kv._1) == false)  
     
    } else if (topic == Topics.USER) {
      /*
       * This request recommends similar users to those that have 
       * been voted for a specified 'item'
       */
      val columns = formatter.usersAsList.mkString(",")
      
      val excludes = List(Names.REQ_COLUMNS, Names.REQ_MATRIX)
      Map(Names.REQ_COLUMNS -> columns,Names.REQ_MATRIX -> "user") ++  req.data.filter(kv => excludes.contains(kv._1) == false)  
      
    } else {
      throw new Exception("This request type is not supported by the CAR algorithm.")
    }
 
    return Serializer.serializeRequest(new ServiceRequest(service,task,data))

  }

  def buildRecommendResponse(req:ServiceRequest,res:ServiceResponse):Any = {

    val similars = Serializer.deserializeSimilarColumnsList(req.data(Names.REQ_RESPONSE)).items
    /*
     * Reduce to the total number of scored columns
     */
    val total = req.data(Names.REQ_TOTAL).toInt
    val scoredColumns = similars.flatMap(x => x.items).sortBy(x => -x.score).take(total)
    
    val formatter = new CARFormatter(sc,req)
    
    val topic = req.task.split(":")(1)    
    if (topic == Topics.ITEM) {
 
      val lookup = formatter.index2item
      val offset = formatter.itemCount
      
      ScoredFields(scoredColumns.map(x => ScoredField(lookup(x.col-offset),x.score)))
      
      
    } else if (topic == Topics.USER) {
 
      val lookup = formatter.index2user
      ScoredFields(scoredColumns.map(x => ScoredField(lookup(x.col),x.score)))
      
    } else {
      throw new Exception("This request type is not supported by the CAR algorithm.")
    }
    
    null
  }

  def buildSimilarRequest(req:ServiceRequest):String = {

    val service = "context"
    val task = "similar:vector"
    
    val users = req.data.contains(Names.REQ_USERS)
    val items = req.data.contains(Names.REQ_ITEMS)
    
    if (users == false && items == false) {
      new Exception("This similarity request is not supported by the CAR algorithm.")
    }
    
    val formatter = new CARFormatter(sc,req)
    val data = if (users) {
      
      val columns = formatter.usersAsCols.mkString(",")
     
      val excludes = List(Names.REQ_COLUMNS,Names.REQ_MATRIX)
      Map(Names.REQ_COLUMNS -> columns,Names.REQ_MATRIX -> "user") ++  req.data.filter(kv => excludes.contains(kv._1) == false)  
      
    } else {
      
      val columns = formatter.itemsAsCols.mkString(",")
      
      val excludes = List(Names.REQ_COLUMNS,Names.REQ_MATRIX)
      Map(Names.REQ_COLUMNS -> columns,Names.REQ_MATRIX -> "item") ++  req.data.filter(kv => excludes.contains(kv._1) == false)  
     
    }

    return Serializer.serializeRequest(new ServiceRequest(service,task,data))
    
  }
  
  def buildSimilarResponse(req:ServiceRequest,res:ServiceResponse):Any = {
    
    val similars = Serializer.deserializeSimilarColumnsList(req.data(Names.REQ_RESPONSE)).items
    
    val users = req.data.contains(Names.REQ_USERS)
    val items = req.data.contains(Names.REQ_ITEMS)

    val formatter = new CARFormatter(sc,req)
    
    if (users == false && items == false) {
      new Exception("This similarity request is not supported by the CAR algorithm.")
    }

    if (users) {
 
      val lookup = formatter.index2user
      SimilarFieldsList(similars.map(x => SimilarFields(lookup(x.col),x.items.map(v => ScoredField(lookup(v.col),v.score)))))
      
    } else {
 
      val lookup = formatter.index2item
      val offset = formatter.itemCount

      SimilarFieldsList(similars.map(x => SimilarFields(lookup(x.col-offset),x.items.map(v => ScoredField(lookup(v.col-offset),v.score)))))
      
    }
   
  }

}