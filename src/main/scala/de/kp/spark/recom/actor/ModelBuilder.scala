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

import de.kp.spark.recom.als.ALSFlow
import de.kp.spark.recom.asr.ASRFlow
import de.kp.spark.recom.car.CARFlow

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class ModelBuilder(@transient ctx:RequestContext,params:Map[String,String]) extends BaseActor {
  
  def receive = {

    case message:StartBuild => {
      
      val origin = sender              
      val response = try {
        
        validate match {
            
          case None => execute             
          
          case Some(message) => Future {
            val req = ServiceRequest("recommendation","build",params)
            failure(req,message)
          }
        
        }        
      
      } catch {
        case e:Exception => Future {
          val req = ServiceRequest("recommendation","build",params)
          failure(req,e.getMessage)
        }
        
      }
      
      response.onSuccess {
        
        case result => {
          origin ! result
        }
      
      }
      response.onFailure {
      
        case throwable => {    
          val req = ServiceRequest("recommendation","build",params)
          origin ! failure(req,throwable.toString)	                  
        }	      
      
      }
         
    }
    
    case msg:BuildFailed => {
      
      val uid = msg.params(Names.REQ_UID)
      
      val start = new java.util.Date().getTime.toString            
      log.info(String.format("""[UID: %s] Preference building failed at %s.""",uid,start))
      
    }
    
    case msg:BuildFinished => {
      
      val uid = msg.params(Names.REQ_UID)
      
      val start = new java.util.Date().getTime.toString            
      log.info(String.format("""[UID: %s] Predictive model building finished at %s.""",uid,start))

      context.stop(self)
      
    }
     
    case msg:LearnFailed => {
      
      val uid = msg.params(Names.REQ_UID)
      
      val start = new java.util.Date().getTime.toString            
      log.info(String.format("""[UID: %s] Predictive model building failed at %s.""",uid,start))
      
    }
    
    case msg:LearnFinished => {
      
      val uid = msg.params(Names.REQ_UID)
      
      val start = new java.util.Date().getTime.toString            
      log.info(String.format("""[UID: %s] Predictive model building finished at %s.""",uid,start))

      context.stop(self)
      
    }
   
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! failure(null,msg)
      context.stop(self)

    }
  
  }
 
  protected def execute:Future[Any] = {

    val (duration,retries,time) = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(time).second
    
    ask(actor, StartBuild)
    
  }
  
  protected def validate:Option[String] = {

    val uid = params(Names.REQ_UID)
    
    val algorithm = params.get(Names.REQ_ALGORITHM)
    val rating    = params.get(Names.REQ_RATING)
    
    algorithm match {
        
      case None => {
        return Some(Messages.NO_ALGORITHM_PROVIDED(uid))              
      }
        
      case Some(value) => {
        if (Algorithms.isAlgorithm(value) == false) {
          return Some(Messages.ALGORITHM_IS_UNKNOWN(uid,value))    
        }
          
      }
    
    }  

    if (algorithm.get == Algorithms.ASR) return None
    
    rating match {
        
      case None => {
        return Some(Messages.NO_RATING_PROVIDED(uid))       
      }
        
      case Some(value) => {
        if (Ratings.isRating(value) == false) {
          return Some(Messages.RATING_IS_UNKNOWN(uid,value))    
        }          
      }
        
    }

    None
  
    
  }

  protected def actor:ActorRef = {

    val req_params = params   
    req_params(Names.REQ_ALGORITHM) match {
      
      case Algorithms.ALS => context.actorOf(Props(new ALSFlow(ctx,req_params)))   
      
      case Algorithms.ASR => context.actorOf(Props(new ASRFlow(ctx,req_params)))   
      
      case Algorithms.CAR => context.actorOf(Props(new CARFlow(ctx,req_params)))   

      case _ => null
      
    }
  
  }

}