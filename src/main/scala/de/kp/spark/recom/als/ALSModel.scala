package de.kp.spark.recom.als
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

import de.kp.spark.core.redis.RedisCache

import de.kp.spark.recom.{Configuration,RequestContext}
import de.kp.spark.recom.model._

class ALSModel(ctx:RequestContext,params:Map[String,String]) extends Actor with ActorLogging {

  val (host,port) = Configuration.redis
  val cache = new RedisCache(host,port.toInt)

  def receive = {
    
    case message:StartLearn => {
      
      try {
     
        val uid = params(Names.REQ_UID)
        val name = params(Names.REQ_NAME)
      
        val start = new java.util.Date().getTime.toString            
        log.info(String.format("""[UID: %s] %s matrix request received at %s.""",uid,name,start))
        
        val req = ServiceRequest("","",params)
        cache.addStatus(req,ResponseStatus.TRAINING_STARTED)

        /*
         * Build ALS recommendation model based on the request data;
         * users & items refer to the reference information that is 
         * used to map a certain user (uid) and item (iid) to the 
         * Integer representation required by the ALS algorithm
         */
        new ALSRecommender(ctx).train(req)
        cache.addStatus(req,ResponseStatus.TRAINING_FINISHED)

        val res_params = params ++ Map(Names.REQ_MODEL -> "matrix")
        context.parent ! LearnFinished(res_params)
      
        context.stop(self)
        
      } catch {
        case e:Exception => {
        
          val res_params = params ++ Map(Names.REQ_MESSAGE -> e.getMessage)
          context.parent ! LearnFailed(res_params)
          
          context.stop(self)
          
        }
      
      }
    
    }
    
  }

}