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

import de.kp.spark.core.model._
import de.kp.spark.core.redis.RedisCache

import de.kp.spark.recom.model._
import scala.concurrent.Future

abstract class BaseActor extends Actor with ActorLogging {

  val cache = new RedisCache()
  implicit val ec = context.dispatcher
  
  protected def failure(req:ServiceRequest,message:String):ServiceResponse = {
    
    if (req == null) {
      val data = Map("message" -> message)
      new ServiceResponse("","",data,ResponseStatus.FAILURE)	
      
    } else {
      val data = Map("uid" -> req.data("uid"), "message" -> message)
      new ServiceResponse(req.service,req.task,data,ResponseStatus.FAILURE)	
    
    }
    
  }
  
  protected def onResponse(request:ServiceRequest,response:Future[ServiceResponse],receiver:ActorRef) {
    
    response.onSuccess {
      case result => {
        receiver ! Serializer.serializeResponse(result)
        context.stop(self)
      }
    }

    response.onFailure {
      case throwable => {           
        val resp = failure(request,throwable.toString)
        receiver ! Serializer.serializeResponse(resp)	                  

        context.stop(self)
              
      }	  
    
    }
    
  }
  
  protected def response(req:ServiceRequest,missing:Boolean):ServiceResponse = {
    
    val uid = req.data("uid")
    
    if (missing == true) {
      val data = Map("uid" -> uid, "message" -> Messages.MISSING_PARAMETERS(uid))
      new ServiceResponse(req.service,req.task,data,ResponseStatus.FAILURE)	
  
    } else {
      val data = Map("uid" -> uid, "message" -> Messages.MODEL_BUILDING_STARTED(uid))
      new ServiceResponse(req.service,req.task,data,ResponseStatus.BUILDING_STARTED)	
  
    }

  }
  
  protected def validate(req:ServiceRequest):Option[String] = {

    val uid = req.data("uid")
    
    if (cache.statusExists(req)) {            
      return Some(Messages.TASK_ALREADY_STARTED(uid))   
    }

    req.data.get("algorithm") match {
        
      case None => {
        return Some(Messages.NO_ALGORITHM_PROVIDED(uid))              
      }
        
      case Some(algorithm) => {
        if (Algorithms.isAlgorithm(algorithm) == false) {
          return Some(Messages.ALGORITHM_IS_UNKNOWN(uid,algorithm))    
        }
          
      }
    
    }  
    
    req.data.get("source") match {
        
      case None => {
        return Some(Messages.NO_SOURCE_PROVIDED(uid))       
      }
        
      case Some(source) => {
        if (Sources.isSource(source) == false) {
          return Some(Messages.SOURCE_IS_UNKNOWN(uid,source))    
        }          
      }
        
    }

    None
    
  }

}