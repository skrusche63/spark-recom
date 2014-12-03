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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.recom.model._
import de.kp.spark.recom.Configuration

import de.kp.spark.recom.RemoteContext

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/**
 * BuildActor is responsible for build implict user ratings from the 
 * data source provided; this is either an event- or item-based source.
 */
class BuildActor(@transient sc:SparkContext,rtx:RemoteContext) extends BaseActor {
  
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data(Names.REQ_UID)
          
      val response = validate(req) match {
            
        case None => build(req).mapTo[ServiceResponse]            
        case Some(message) => Future {failure(req,message)}
            
      }

      onResponse(req,response,origin)
         
    }
    
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! failure(null,msg)
      context.stop(self)

    }
  
  }
 
  private def actor(req:ServiceRequest):ActorRef = {

    req.data(Names.REQ_ALGORITHM) match {
      
      case Algorithms.ALS => context.actorOf(Props(new ALSActor(sc,rtx)))   
      
      case Algorithms.ASR => context.actorOf(Props(new ASRActor(sc,rtx)))   
      case Algorithms.CAR => context.actorOf(Props(new CARActor(sc,rtx)))   

      case _ => null
      
    }
  
  }
 
  private def build(req:ServiceRequest):Future[Any] = {

    val (duration,retries,time) = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(time).second
    
    ask(actor(req), req)
    
  }
  
  override def validate(req:ServiceRequest):Option[String] = {

    /**
     * We have to make sure that there is a sink specified, and,
     * that the respective sink is set to FILE; this ensures that
     * the ratings are saved as file on a Hadoop file system 
     */
    val uid = req.data(Names.REQ_UID)
    if (req.data.contains(Names.REQ_SINK) == false) {
      return Some(Messages.NO_SINK_PROVIDED(uid))    
      
    }
    
    if (req.data(Names.REQ_SINK) != Sinks.FILE) {
      return Some(Messages.FILE_SINK_REQUIRED(uid))          
    }
    
    super.validate(req)
   
    
  }

}