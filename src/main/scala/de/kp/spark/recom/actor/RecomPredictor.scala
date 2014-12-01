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

import org.apache.spark.SparkContext

import akka.actor.{ActorRef,Props}
import akka.pattern.ask
import akka.util.Timeout

import de.kp.spark.core.model._

import de.kp.spark.recom.model._
import de.kp.spark.recom.Configuration

import de.kp.spark.recom.RemoteContext

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class RecomPredictor(@transient sc:SparkContext,rtx:RemoteContext) extends BaseActor {
  
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data("uid")
          
      val response = get(req)            
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
 
  private def actor(req:ServiceRequest):ActorRef = {

    req.data("algorithm") match {
      
      case Algorithms.ALS => context.actorOf(Props(new ALSActor(sc,rtx)))   
      
      case Algorithms.ASR => context.actorOf(Props(new ASRActor(sc,rtx)))   
      case Algorithms.CAR => context.actorOf(Props(new CARActor(sc,rtx)))   

      case _ => null
      
    }
  
  }
 
  private def get(req:ServiceRequest):Future[Any] = {

    val (duration,retries,time) = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(time).second
    
    ask(actor(req), req)
    
  }

}