package de.kp.spark.recom.car
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
import de.kp.spark.core.model._

import de.kp.spark.recom.RequestContext
import de.kp.spark.recom.model._

import scala.collection.mutable.Buffer
/**
 * CARFlow controls the data analytics process to build Context-Aware recommendation
 * models. This process comprises 3 subsequent steps:
 * 
 * 1) Build implict user preferences from customer interaction events; these events
 *    must be tracked & stored in a previous step. The CAR recommender requires an
 *    Elasticsearch cluster as the user event data store.
 *    
 * 2) Learn a factorization machine model from the user preferences; the respective
 *    preferences must be stored as a Parquet file with the 
 *    
 * 3) Learn interaction matrices from the user and also from the item features to 
 *    specify user and item similarity to support respective similarity requests 
 */
class CARFlow(ctx:RequestContext,params:Map[String,String]) extends Actor with ActorLogging {
  
  def receive = {
    
    case message:StartBuild => {
      
      try {
        
        /*
         * Send response to ModelBuilder (parent)
         */
        val res_params = params ++ Map(Names.REQ_MESSAGE -> "CAR model building started.")
        sender ! ServiceResponse("recommendation","build",res_params,ResponseStatus.BUILDING_STARTED)
       
        validate
       
        val uid = params(Names.REQ_UID)
        val start = new java.util.Date().getTime.toString            

        log.info(String.format("""[UID: %s] CAR pipeline started at %s.""",uid,start))
     
        val req_params = params ++ Map(
            Names.REQ_ALGORITHM -> "EPREF", 
            /*
             * User events as a basis for preference modeling
             * are tracked and stored in an Elasticsearch cluster
             */
            Names.REQ_SOURCE -> "ELASTIC", 
            /*
             * The preference modeler is required to save the
             * results as parquet file
             */
            Names.REQ_SINK -> "PARQUET")
      
        val actor = context.actorOf(Props(new CARPreference(ctx,params)))
        actor ! StartBuild
        
      } catch {
        case e:Exception => {

          val res_params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ params
          context.parent ! BuildFailed(res_params)
          
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
      
      val res_params = message.params
      /*
       * Build user preferences has been successfully finished; so we start learning 
       * the prediction model with CARModel actor; the model is always saved in a 
       * REDIS instance, and therefore no SINK must be provided
       */
      val excludes = List(Names.REQ_ALGORITHM,Names.REQ_SINK)
      val req_params = res_params.filter(kv => excludes.contains(kv._1) == false) ++ Map(
          /*
           * The Context-Aware Analysis engine retrieves preference data
           * from a parquet file; the factorization machine model is stored
           * on the HDFS file system
           */
          Names.REQ_SOURCE -> "PARQUET")
      
      val actor = context.actorOf(Props(new CARModel(ctx,req_params)))
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

    if (params.contains(Names.REQ_RATING) == false)
      throw new Exception("Parameter 'rating' is missing")
    
  }
  
}