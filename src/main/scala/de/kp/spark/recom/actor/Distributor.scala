package de.kp.spark.recom.actor
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
 * 
 * This file is part of the Spark-Recom project
 * (https://github.com/skrusche63/spark-cluster).
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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.recom._
import de.kp.spark.recom.model._

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
/**
 * The Distributor actor is responsible for distributing service requests
 * with respect to the algorithm provided by the request
 */
class Distributor(@transient ctx:RequestContext) extends BaseActor {
  
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender              
      val response = try {
        
        validate(req) match {
            
          case None => execute(req)             
          case Some(message) => Future {failure(req,message)}
            
        }        
                   
      
      } catch {
        case e:Exception => Future {failure(req,e.getMessage)}
        
      }
      
      response.onSuccess {
        
        case result => {
          origin ! result
          context.stop(self)
        }
      
      }
      response.onFailure {
      
        case throwable => {           
          origin ! failure(req,throwable.toString)	                  
          context.stop(self)            
        }	      
      
      }
         
    }
    
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! failure(null,msg)
      context.stop(self)

    }
  
  }
 
  protected def actor(req:ServiceRequest):ActorRef = {

    req.data(Names.REQ_ALGORITHM) match {
      
      case Algorithms.ALS => context.actorOf(Props(new ALSActor(ctx)))   
      //case Algorithms.ASR => context.actorOf(Props(new ASRActor(ctx)))   
      case Algorithms.CAR => context.actorOf(Props(new CARActor(ctx)))   

      case _ => null
      
    }
  
  }
 
  protected def execute(req:ServiceRequest):Future[Any] = {

    val (duration,retries,time) = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(time).second
    
    ask(actor(req), req)
    
  }
  
  protected def validate(req:ServiceRequest):Option[String] = {

    val uid = req.data(Names.REQ_UID)
    
    if (cache.statusExists(req)) {            
      return Some(Messages.TASK_ALREADY_STARTED(uid))   
    }

    req.data.get(Names.REQ_ALGORITHM) match {
        
      case None => {
        return Some(Messages.NO_ALGORITHM_PROVIDED(uid))              
      }
        
      case Some(algorithm) => {
        if (Algorithms.isAlgorithm(algorithm) == false) {
          return Some(Messages.ALGORITHM_IS_UNKNOWN(uid,algorithm))    
        }
          
      }
    
    }  

    None
    
  }

}