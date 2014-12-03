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

import de.kp.spark.core.Names

import de.kp.spark.core.model._
import de.kp.spark.recom.model._

import de.kp.spark.recom.RemoteContext
import de.kp.spark.recom.format.CARFormatter

import scala.concurrent.Future

class CARActor(@transient sc:SparkContext,rtx:RemoteContext) extends BaseWorker(sc) {
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
    val message = Serializer.serializeRequest(new ServiceRequest(service,req.task,req.data))
    /*
     * Training a factorization model or a correlation matrix is a fire-and-forget 
     * task from the recommendation service prespective; the request task is either
     * specified as 'train:matrix' or 'train:model'
     */
    rtx.send(service,message)
    
  }

  def doPredictRequest(req:ServiceRequest):Future[Any] = {

    val uflag = req.data.contains(Names.REQ_USER)
    val iflag = req.data.contains(Names.REQ_ITEM)
    
    val cflag = req.data.contains(Names.REQ_CONTEXT)
    val fflag = req.data.contains(Names.REQ_FEATURES)
    
    if (fflag) {
      /*
       * This request type is usually not used as it describes a
       * very basic approach to predictions with a full specified
       * feature vector
       */
      val service = "context"
      val message = Serializer.serializeRequest(new ServiceRequest(service,"get:feature",req.data))
    
      rtx.send(service,message)
    
    } else if (uflag && iflag && cflag) {
      /*
       * This is the common request type, where a certain (user,item) pair 
       * and an observed context is provided; in this case, the data are
       * transformed into the basic feature vector
       */
      val features = new CARFormatter().format(req).mkString(",")
      val data = req.data.filter(x => x._1 != Names.REQ_USER && x._1 != Names.REQ_ITEM).map(x => {
        
        if (x._1 == Names.REQ_CONTEXT) (Names.REQ_FEATURES,features) else x
        
      })
      
      val service = "context"
      val message = Serializer.serializeRequest(new ServiceRequest(service,"get:feature",data))
    
      rtx.send(service,message)
      
    } else {
      
      val msg = "Provided combination of input parameters is not supported."
      Future {Serializer.serializeResponse(failure(req,msg))}  
    
    }
     
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

  def doRecommendRequest(req:ServiceRequest):Future[Any] = {
    
    val topic = req.task.split(":")(1)
    topic match {
      
      case Topics.SIMILAR => {
      
        val service = "context"
        val message = Serializer.serializeRequest(new ServiceRequest(service,"get:similar",req.data))
    
        rtx.send(service,message)
        
      }
      
      case _ =>  throw new Exception("This recommendation request is not supported by the CAR algorithm.")

    }
    
  }

  def buildRecommendResponse(req:ServiceRequest,intermediate:ServiceResponse):Any = {
    
    if (intermediate.status == ResponseStatus.SUCCESS) {
      
      if (req.data.contains(Topics.SIMILAR)) {
        /*
         * This response describes the top most similar items with 
         * respect to list of items
         */
        Serializer.deserializeSimilars(req.data(Topics.SIMILAR))
        
      } else throw new Exception("The recommendation response is not supported by the CAR algorithm.")
      
    } else {
      /*
       * In case of an error, send the intermediate message as it contains
       * the specification of the respective failure
       */
      intermediate

    }
  
  }

}