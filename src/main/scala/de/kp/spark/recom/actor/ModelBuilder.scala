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
 * The Builder actor is responsible to build implict user ratings from the 
 * data source provided; this is either an event- or item-based source.
 */
class ModelBuilder(@transient sc:SparkContext,rc:RemoteContext) extends Distributor(sc,rc) {
  
  override def validate(req:ServiceRequest):Option[String] = {

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
    
    req.data.get(Names.REQ_SOURCE) match {
        
      case None => {
        return Some(Messages.NO_SOURCE_PROVIDED(uid))       
      }
        
      case Some(source) => {
        if (Sources.isSource(source) == false) {
          return Some(Messages.SOURCE_IS_UNKNOWN(uid,source))    
        }          
      }
        
    }

    /**
     * We have to make sure that there is a sink specified, and,
     * that the respective sink is set to FILE; this ensures that
     * the ratings are saved as file on a Hadoop file system 
     */
    req.data.get(Names.REQ_SINK) match {
        
      case None => {
        return Some(Messages.NO_SINK_PROVIDED(uid))       
      }
        
      case Some(sink) => {
        if (Sinks.isSink(sink) == false) {
          return Some(Messages.SINK_IS_UNKNOWN(uid,sink))    
        }          
      }
        
    }

    None
  
    
  }

}