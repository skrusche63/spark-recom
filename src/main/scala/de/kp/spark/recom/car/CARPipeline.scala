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
 * CARPipeline controls the data analytics process with respect
 * to build context-aware recommendation models; the process
 * comprises a sequence of three steps: a) build preferences,
 * b) build prediction model, and, c) build similarity matrix
 */
class CARPipeline(ctx:RequestContext,params:Map[String,String]) extends Actor with ActorLogging {
  
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
         * - 'algorithm' = CAR
         * - 'rating'    = implicit | explicit
         * 
         * The recommender expects implict or explicit rating data 
         * tracked by a prior data processing task and indexed in
         * an Elasticsearch cluster
         * 
         * - 'source'    = ELASTIC
         * 
         * The 'algorithm' value must be replaced by 'EPREF' 
         */
        val req_params = params ++ Map(Names.REQ_ALGORITHM -> "EPREF", Names.REQ_SOURCE -> "ELASTIC", Names.REQ_SINK -> "PARQUET")
      
        /* Delegate request to CARPreference actor */
        val actor = context.actorOf(Props(new CARPreference(ctx,params)))
        actor ! StartBuild
        
      } catch {
        case e:Exception => {

          val res_params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ params
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
      
      val res_params = message.params
      /*
       * Build user preferences has been successfully finished; so we start learning 
       * the prediction model with CARModel actor; the model is always saved in a 
       * REDIS instance, and therefore no SINK must be provided
       */
      val excludes = List(Names.REQ_ALGORITHM,Names.REQ_SINK)
      val req_params = res_params.filter(kv => excludes.contains(kv._1) == false) ++ Map(Names.REQ_SOURCE -> "PARQUET")
      
      /* Delegate request to CARModel actor */
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

      val res_params = message.params
      
      val model = res_params(Names.REQ_MODEL)
      if (model == "model") {
        
        /*
         * The Context-Aware Analysis engine has built the factorization model;
         * the subsequent step is to compute the similarity matrix to support
         * similarity requests: 
         * 
         * We actually support two different similarity matrices, one for users
         * and the other one for items; after having finished model building, 
         * the next step is to build the 'user' similarity matrix
         */
        val uid = res_params(Names.REQ_UID)
        val name = res_params(Names.REQ_NAME)
          
        val formatter = new CARFormatter(ctx, ServiceRequest("","",Map(Names.REQ_UID -> uid, Names.REQ_NAME -> name)))          
        val (start,end) = formatter.getUserBlock
    
        val excludes = List(Names.REQ_SOURCE, Names.REQ_ALGORITHM,Names.REQ_SINK,Names.REQ_START,Names.REQ_END,Names.REQ_MATRIX)          
        val req_params = res_params.filter(kv => excludes.contains(kv._1) == false) ++ Map(
              Names.REQ_START -> start.toString,Names.REQ_END -> end.toString,Names.REQ_MATRIX -> "user"
        )  
      
        val actor = context.actorOf(Props(new CARMatrix(ctx,req_params)))
        actor ! StartLearn
        
      } else {
        
        /*
         * The Context-Aware Analysis engine has built a similarity matrix;
         * we have to determine to which matrix the message refers to.
         */
          val matrix = res_params(Names.REQ_MATRIX) 
          if (matrix == "user") {
            /*
             * In this case, we have to build the item similarity matrix as
             * the final preparation step
             */  
            /*
             * Retrieve the unique task identifier and the name of the factorization 
             * model; note, that this name is the one that is known by the requestor 
             */
            val uid = res_params(Names.REQ_UID)
            val name = res_params(Names.REQ_NAME)
          
            val formatter = new CARFormatter(ctx,new ServiceRequest("","",Map(Names.REQ_UID -> uid, Names.REQ_NAME -> name)))
            val (start,end) = formatter.getItemBlock
    
            val excludes = List(Names.REQ_SOURCE, Names.REQ_ALGORITHM,Names.REQ_SINK,Names.REQ_START,Names.REQ_END,Names.REQ_MATRIX)          
            val req_params = res_params.filter(kv => excludes.contains(kv._1) == false) ++ Map(
                Names.REQ_START -> start.toString,Names.REQ_END -> end.toString,Names.REQ_MATRIX -> "user"
            )  
      
            /* Delegate request to CARMatrix actor */
            val actor = context.actorOf(Props(new CARMatrix(ctx,req_params)))
            actor ! StartLearn
         
          } else {
            
            /*
             * The model and matrix building request is finished;
             * we stop the data analytics pipeline here and inform 
             * the requestor about this situation
             */
            context.parent ! message
            context.stop(self)
          
          }
       
      }
      
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