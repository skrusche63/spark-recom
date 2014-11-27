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

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class RecomTrainer(@transient val sc:SparkContext) extends BaseActor {
  
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data("uid")

      req.task.split(":")(0) match {
        
        case "train" => {
          
          val response = validate(req) match {
            
            case None => train(req).mapTo[ServiceResponse]            
            case Some(message) => Future {failure(req,message)}
            
          }

          onResponse(req,response,origin)
         
        }
        
        case _ => {
          
          val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
          
          origin ! Serializer.serializeResponse(failure(req,msg))
          context.stop(self)
          
        }
        
      }
      
    }
    
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! Serializer.serializeResponse(failure(null,msg))
      context.stop(self)

    }
  
  }
 
  private def actor(req:ServiceRequest):ActorRef = {

    req.data("algorithm") match {
      
      case Algorithms.ALS => context.actorOf(Props(new ALSActor(sc)))   
      
      case Algorithms.ASR => context.actorOf(Props(new ASRActor(sc)))   
      case Algorithms.CAR => context.actorOf(Props(new CARActor(sc)))   

      case _ => null
      
    }
  
  }
 
  private def train(req:ServiceRequest):Future[Any] = {

    val (duration,retries,time) = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(time).second
    
    ask(actor(req), req)
    
  }

}