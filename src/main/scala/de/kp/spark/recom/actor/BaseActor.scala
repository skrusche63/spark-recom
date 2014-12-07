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

import akka.actor.{Actor,ActorLogging,ActorRef}

import de.kp.spark.core.Names

import de.kp.spark.core.model._
import de.kp.spark.core.redis.RedisCache

import de.kp.spark.recom.Configuration
import de.kp.spark.recom.model._

import scala.concurrent.Future

abstract class BaseActor extends Actor with ActorLogging {

  val (host,port) = Configuration.redis
  val cache = new RedisCache(host,port.toInt)

  implicit val ec = context.dispatcher
  
  protected def failure(req:ServiceRequest,message:String):ServiceResponse = {
    
    if (req == null) {
      val data = Map(Names.REQ_MESSAGE -> message)
      new ServiceResponse("","",data,ResponseStatus.FAILURE)	
      
    } else {
      val data = Map(Names.REQ_UID -> req.data(Names.REQ_UID), Names.REQ_MESSAGE -> message)
      new ServiceResponse(req.service,req.task,data,ResponseStatus.FAILURE)	
    
    }
    
  }
  
  protected def response(req:ServiceRequest,missing:Boolean):ServiceResponse = {
    
    val uid = req.data(Names.REQ_UID)
    
    if (missing == true) {
      val data = Map(Names.REQ_UID -> uid, Names.REQ_MESSAGE -> Messages.MISSING_PARAMETERS(uid))
      new ServiceResponse(req.service,req.task,data,ResponseStatus.FAILURE)	
  
    } else {
      val data = Map(Names.REQ_UID -> uid, Names.REQ_MESSAGE -> Messages.MODEL_BUILDING_STARTED(uid))
      new ServiceResponse(req.service,req.task,data,ResponseStatus.BUILDING_STARTED)	
  
    }

  }

  protected def serialize(resp:Any) = {
    
    if (resp.isInstanceOf[Preferences]) {
      /*
       * This is the response type used for 'predict' and 
       * also 'recommend' requests that refer to the ALS 
       * or ASR algorithms 
      */
      Serializer.serializePreferences(resp.asInstanceOf[Preferences])
          
    } else if (resp.isInstanceOf[TargetedPoint]) {
      /*
       * This is the response type used for 'predict'
       * requests that refer to the CAR algorithm
       */
      Serializer.serializeTargetedPoint(resp.asInstanceOf[TargetedPoint])
            
    } else if (resp.isInstanceOf[ServiceResponse]) {
      /*
       * This is the common response type used for almost
       * all requests
       */
      Serializer.serializeResponse(resp.asInstanceOf[ServiceResponse])
            
    }
    
  }

}