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
import akka.actor.{ActorRef,Props}

import akka.pattern.ask
import akka.util.Timeout

import akka.actor.{OneForOneStrategy, SupervisorStrategy}
import de.kp.spark.core.model._

import de.kp.spark.recom.Configuration
import de.kp.spark.recom.model._

import de.kp.spark.recom.RemoteContext

import scala.concurrent.duration.DurationInt
import scala.concurrent.Future

class RecomMaster(@transient val sc:SparkContext) extends BaseActor {
  
  val (duration,retries,time) = Configuration.actor         
  implicit val timeout:Timeout = DurationInt(time).second

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries=retries,withinTimeRange = DurationInt(time).minutes) {
    case _ : Exception => SupervisorStrategy.Restart
  }
  /**
   * The RemoteContext is used to interact with the User Preference engine
   * as well as with other engines from Predictiveworks.
   */
  private val rtx = new RemoteContext()
  
  def receive = {
    
    /**
     * This request is sent by a remote Akka actor and may comprise
     * either a ServiceRequest or a ServiceResponse
     */
    case message:String => {
	  	    
	  val origin = sender
	  
	  try {
	    
	    deserializeRequest(message) match {
	      /*
           * We try to deserialize the external message as a ServiceRequest;
           * this is the most frequent use case and will considered first
           */
	      case Some(req) => {
	      
	        val response = doRequest(req)
	        /*
	         * In this case a response must be sent to the sender as a request
	         * is always answered
	         */
            response.onSuccess {
              case result => origin ! serialize(result)
            }
            response.onFailure {
              case result => origin ! serialize(failure(req,Messages.GENERAL_ERROR(req.data("uid"))))	      
	        }
	      
	      }
	    
	      case None => {
	        /*
             * Next we try to deserialize the external message as a ServiceResponse;
             * the message may be sent by the user preference engine or one of the
             * engine of Predictiveworks.
             */
	        deserializeResponse(message) match {
	          /*
	           * In this case no response is sent to the sender as this is a 
	           * notification to a previously invoked data processing task
	           */
	          case Some(res) => doResponse(res)
	          
	          case None => throw new Exception("Unknown message")
	          
	        }
	      
	      }
	  
	    }
	  
	  } catch {
	    case e:Exception => {
      
	      val msg = Messages.REQUEST_IS_UNKNOWN()          
          origin ! Serializer.serializeResponse(failure(null,msg))
	      
	    }
	  
	  }
    
    }
    
    /**
     * This request is sent by the REST API
     */
    case req:ServiceRequest => {
	  	    
	  val origin = sender

	  val response = doRequest(req)
      response.onSuccess {
        case result => origin ! result
      }
      response.onFailure {
        case result => origin ! failure(req,Messages.GENERAL_ERROR(req.data("uid")))	      
	  }
      
    }
  
    case _ => {
 
      val msg = Messages.REQUEST_IS_UNKNOWN()          
      log.error(msg)

    }
    
  }

  private def deserializeRequest(message:String):Option[ServiceRequest] = {
    
    try {
      
      Some(Serializer.deserializeRequest(message))
      
    } catch {
      case e:Exception => None
    
    }
    
  }
  
  private def deserializeResponse(message:String):Option[ServiceResponse] = {
    
    try {
      
      Some(Serializer.deserializeResponse(message))
      
    } catch {
      case e:Exception => None
    
    }
    
  }

  private def doRequest(req:ServiceRequest):Future[ServiceResponse] = {
	  
    req.task.split(":")(0) match {

      case "get" => ask(actor("questor"),req).mapTo[ServiceResponse]
      case "index" => ask(actor("indexer"),req).mapTo[ServiceResponse]

      case "register"  => ask(actor("registrar"),req).mapTo[ServiceResponse]
      case "status" => ask(actor("monitor"),req).mapTo[ServiceResponse]

      case "build" => ask(actor("builder"),req).mapTo[ServiceResponse]
      case "train" => ask(actor("trainer"),req).mapTo[ServiceResponse]

      case "track"  => ask(actor("tracker"),req).mapTo[ServiceResponse]
       
      case _ => Future {     
        failure(req,Messages.TASK_IS_UNKNOWN(req.data("uid"),req.task))
      }
      
    }
    
  }
  
  private def doResponse(res:ServiceResponse) {
    
    val service = res.service
    service match {

      case "association" => {
        /*
         * The response is sent by the Association Analysis as a notification
         * to a certain data mining task; in this case no further action has
         * to be taken
         */
      }
      case "context" => {
        /*
         * The response is sent by the Context-Aware analysis engine and 
         * indicates that the building process of a certain factorization
         * model has been finished successfully; in this case no further
         * action has to be taken
         */
      } 
      case "rating" => {
        /*
         * The response is sent by the user preference service and indicates
         * that the computation of an implicit rating has finished; in this
         * case the training of the recommendation model (ALS) or factorization
         * model (CAR) has to be initiated
         */
        
        val status = res.status
        if (status == ResponseStatus.BUILDING_FINISHED) {

          /*
           * Build the task: the task value within the response message
           * from the rating service is either build:item or build:event
           */
          val task = res.task.replace("build","train")
          /*
           * The service is actually not set with here, as the respective
           * value is determined by the actors that process this request
           */
          val req = new ServiceRequest("",task,res.data)
          ask(actor("trainer"),req)
          
        }
        
      }
      case _ => 
        
    }
    
    
  }
  
  private def actor(worker:String):ActorRef = {
    
    worker match {

      case "builder" => context.actorOf(Props(new RecomBuilder(sc,rtx)))
      case "trainer" => context.actorOf(Props(new RecomTrainer(sc,rtx)))
  
      case "indexer" => context.actorOf(Props(new RecomIndexer()))

      case "questor" => context.actorOf(Props(new RecomQuestor(sc,rtx)))
      case "monitor" => context.actorOf(Props(new RecomMonitor()))
        
      case "registrar" => context.actorOf(Props(new RecomRegistrar()))        
      case "tracker" => context.actorOf(Props(new RecomTracker()))

      case _ => null
      
    }
  
  }

}