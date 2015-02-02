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

import akka.actor.{ActorRef,Props}

import akka.pattern.ask
import akka.util.Timeout

import akka.actor.{OneForOneStrategy, SupervisorStrategy}

import de.kp.spark.core.Names

import de.kp.spark.core.actor._
import de.kp.spark.core.model._

import de.kp.spark.recom._
import de.kp.spark.recom.model._

import de.kp.spark.recom.car.CARFormatter

import scala.concurrent.duration.DurationInt
import scala.concurrent.Future

class RecomMaster(@transient val ctx:RequestContext) extends BaseActor {
  
  val (duration,retries,time) = ctx.config.actor         
  implicit val timeout:Timeout = DurationInt(time).second

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries=retries,withinTimeRange = DurationInt(time).minutes) {
    case _ : Exception => SupervisorStrategy.Restart
  }
  
  def receive = {
    
    /**
     * This request is sent by a remote Akka actor and may comprise
     * either a ServiceRequest or a ServiceResponse
     */
    case message:String => {
	  	    
	  val origin = sender	  
	  try {
	    
	    val req = Serializer.deserializeRequest(message)
        val response = doRequest(req)

        response.onSuccess {
          case result => origin ! serialize(result)
        }
        response.onFailure {
          case result => origin ! serialize(failure(req,Messages.GENERAL_ERROR(req.data(Names.REQ_UID))))	      
	    }
	  
	  } catch {
	    
	    case e:Exception => {
          origin ! Serializer.serializeResponse(failure(null,e.getMessage))
	    }
	  
	  }
    
    }
  
    case _ => {
 
      val msg = Messages.REQUEST_IS_UNKNOWN()          
      log.error(msg)

    }
    
  }

  private def doRequest(req:ServiceRequest):Future[Any] = {
    ask(actor(req),req)
  }
  
  private def actor(req:ServiceRequest):ActorRef = {
    
    val req_params = req.data
    
    val worker = req.task.split(":")(1)    
    worker match {

      case "build" => context.actorOf(Props(new ModelBuilder(ctx,req_params)))
      /*
       * The subsequent tasks are delegated to the algorithm specific actors;
       * this is done by the Distributor actor
       * 
       * Master [static] ----> Distributor [request] ----> (e.g) CARActor [request]
       * 
       */
      case "predict"   => context.actorOf(Props(new Distributor(ctx)))      
      case "recommend" => context.actorOf(Props(new Distributor(ctx)))  
      case "similar"   => context.actorOf(Props(new Distributor(ctx)))      

      /*
       * Metadata management is part of the core functionality; field or metadata
       * specifications can be registered in, and retrieved from a Redis database
       */
      case "fields"   => context.actorOf(Props(new FieldQuestor(Configuration)))
      case "register" => context.actorOf(Props(new BaseRegistrar(Configuration)))        
      /*
       * Index management is part of the core functionality; an Elasticsearch 
       * index can be created and appropriate (tracked) items can be saved.
       */  
      case "index" => context.actorOf(Props(new BaseIndexer(Configuration)))
      case "track" => context.actorOf(Props(new BaseTracker(Configuration)))
      /*
       * Status management is part of the core functionality and comprises the
       * retrieval of the stati of a certain data mining or model building task
       */        
      case "status" => context.actorOf(Props(new StatusQuestor(Configuration)))

      case _ => throw new Exception("Task is unknown.")
      
    }
  
  }

}