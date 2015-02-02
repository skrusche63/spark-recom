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

import de.kp.spark.recom.RequestContext
import de.kp.spark.recom.model._

import scala.collection.mutable.Buffer
/**
 * ALSPipeline controls the data analytics process with respect
 * to build matrix factorization recommendation models; the process
 * comprises a sequence of two steps: a) build preferences,
 * b) build prediction model
 */
class ALSPipeline(ctx:RequestContext,params:Map[String,String]) extends Actor with ActorLogging {
  
  private def STEPS = Buffer.empty[String]
  private def COUNT = 2
  
  def receive = {
    
    case message:StartBuild => {
      
      try {
        
        validate
       
        val uid = params(Names.REQ_UID)
        val start = new java.util.Date().getTime.toString            

        log.info(String.format("""[UID: %s] CAR pipeline started at %s.""",uid,start))
     
        /*
         * The following parameters must be provided by the requestor:
         * 
         * - 'site', 'uid', 'name'
         * 
         * - 'algorithm' = ALS
         * - 'rating'    = implicit | explicit
         * 
         * The recommender expects implict or explicit rating data 
         * tracked by a prior data processing task and indexed in
         * an Elasticsearch cluster
         * 
         * - 'source'    = ELASTIC
         * 
         * The 'algorithm' value must be replaced by 'NPREF' 
         * 
         */
        val req_params = params ++ Map(Names.REQ_ALGORITHM -> "NPREF", Names.REQ_SOURCE -> "ELASTIC", Names.REQ_SINK -> "PARQUET")
      
        /* Delegate request to ALSPreference actor */
        val builder = context.actorOf(Props(new ALSPreference(ctx,params)))
        builder ! StartBuild
        
      } catch {
        case e:Exception => {

          val res_params = params ++ Map(Names.REQ_MESSAGE -> e.getMessage)
          context.parent ! LearnFailed(res_params)
          
          context.stop(self)
          
        }

      }
      
    }
    
    case message:BuildFailed => {
      /*
       * We stop the data analytics pipeline here and
       * inform the requestor about this situation
       */
      context.parent ! message
      context.stop(self)
      
    }
    
    case message:BuildFinished => {
      /*
       * Build user preferences has been successfully
       * finished; so we start learning the prediction
       * model with CARModel actor
       */
      val res_params = message.params
      /*
       * Build user preferences has been successfully finished; so we start learning 
       * the prediction model with ALSModel actor; the model is always saved in a 
       * REDIS instance, and therefore no SINK must be provided
       */
      val excludes = List(Names.REQ_ALGORITHM,Names.REQ_SINK)
      val req_params = res_params.filter(kv => excludes.contains(kv._1) == false) ++ Map(Names.REQ_SOURCE -> "PARQUET")
      
      val actor = context.actorOf(Props(new ALSMatrix(ctx,req_params)))
      actor ! StartLearn
        
      
    }
    
    case message:LearnFailed => {
      /*
       * We stop the data analytics pipeline here and
       * inform the requestor about this situation
       */
      context.parent ! message
      context.stop(self)
            
    }
    
    case message:LearnFinished => {
           
      /*
       * The model and matrix building request is finished;
       * we stop the data analytics pipeline here and inform 
       * the requestor about this situation
       */
      context.parent ! message
      context.stop(self)
      
    }
    
  }

  private def validate() {
    
    if (params.contains(Names.REQ_SITE) == false)
      throw new Exception("Parameter 'site' is missing")
    
    if (params.contains(Names.REQ_UID) == false)
      throw new Exception("Parameter 'uid' is missing")
    
    if (params.contains(Names.REQ_NAME) == false)
      throw new Exception("Parameter 'name' is missing")
    
  }
}