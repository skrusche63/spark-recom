package de.kp.spark.recom.asr
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

import akka.actor._

import de.kp.spark.core.Names

import de.kp.spark.core.actor._
import de.kp.spark.core.model._

import de.kp.spark.recom.RequestContext
import de.kp.spark.recom.model._

import scala.collection.mutable.Buffer

class ASRFlow(ctx:RequestContext,params:Map[String,String]) extends Actor with ActorLogging {

  implicit val ec = context.dispatcher
  
  def receive = {
    
    case message:StartBuild => {
      
      try {
        
        /*
         * Send response to ModelBuilder (parent)
         */
        val res_params = params ++ Map(Names.REQ_MESSAGE -> "ASR model building started.")
        sender ! ServiceResponse("recommendation","build",res_params,ResponseStatus.BUILDING_STARTED)
        
        /* 
         * Build service request message to invoke remote Association Analysis 
         * engine to train a factorization machine model
         */
        val req  = buildRequest
      
        val serialized = Serializer.serializeRequest(req)
        val response = ctx.send(req.service,serialized).mapTo[String]
      
        /*
         * The RemoteSupervisor actor monitors the Redis cache entries of this
         * association rule mining request and informs this actor (as parent)
         * that a certain status has been reached
         */
        val status = ResponseStatus.MINING_FINISHED
        val supervisor = context.actorOf(Props(new Supervisor(req,status,ctx.config)))
      
        /*
         * We evaluate the response message from the remote Association Analysis 
         * engine to check whether an error occurred
         */
        response.onSuccess {
        
          case result => {
 
            val res = Serializer.deserializeResponse(result)
            if (res.status == ResponseStatus.FAILURE) {

              val res_params = params ++ res.data            
              context.parent ! BuildFailed(res_params)
            
              context.stop(self)

            }
         
          }

      }

      response.onFailure {
          
        case throwable => {
        
          val res_params = params ++ Map(Names.REQ_MESSAGE -> throwable.getMessage)
          context.parent ! BuildFailed(res_params)
          
          context.stop(self)
            
          }
	    }
        
      } catch {
        case e:Exception => {

          val res_params = params ++ Map(Names.REQ_MESSAGE -> e.getMessage)
          context.parent ! BuildFailed(res_params)
          
          context.stop(self)
          
        }

      }

    }
   
    case event:StatusEvent => {

      val res_params = params ++ Map(Names.REQ_MODEL -> "model")
      context.parent ! BuildFinished(res_params)
      
      context.stop(self)
      
    }
    
  }
  
  private def buildRequest:ServiceRequest = {
    
    val service = "association"
    val task = "train:model"
    
    val req_params = params ++ Map(Names.REQ_ALGORITHM -> "TOPKNR")      
    ServiceRequest(service,task,req_params)
    
  }  

}