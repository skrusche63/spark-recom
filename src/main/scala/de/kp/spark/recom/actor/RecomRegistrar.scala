package de.kp.spark.recom.actor
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

import de.kp.spark.core.model._
import de.kp.spark.recom.model._

import scala.collection.mutable.ArrayBuffer
/**
 * RecomRegistrar registers different field specifications in
 * a Redis instance, note, that there is actually a difference
 * in the description of 'item': for item-based data sources,
 * an 'Integer ' datatype is required, while for event-based
 * sources a 'String' datatype must be used.
 * 
 * This heterogenity will be removed with one of the next releases.
 */
class RecomRegistrar extends BaseActor {
  
  def receive = {
    
    case req:ServiceRequest => {

      val uid = req.data("uid")      
      val origin = sender   
      
      val response = try {
        
        val fields = ArrayBuffer.empty[Field]
        req.task.split(":")(1) match {
        
          case "event" => {
            /*
             * Metadata description to specify the fields of an event
             * based data source; events may be derived from web logs
             * and are used to compute context-aware recommendations
             * with the extra dimension 'event' and 'time'
             */
            fields += new Field("site","string",req.data("site"))
            fields += new Field("timestamp","long",req.data("timestamp"))

            fields += new Field("user","string",req.data("user"))
            fields += new Field("item","string",req.data("item"))

            fields += new Field("event","integer",req.data("event"))
            cache.addFields(req, new Fields(fields.toList))
            
          }
          
          case "item" => {
            /*
             * Metadata description to specify the fields of an item
             * based data source; items may be derived from transaction
             * databases and are used to compute context-independent
             * recommendations
             */
            fields += new Field("site","string",req.data("site"))
            fields += new Field("timestamp","long",req.data("timestamp"))

            fields += new Field("user","string",req.data("user"))
            fields += new Field("group","string",req.data("group"))

            fields += new Field("item","integer",req.data("item"))
            cache.addFields(req, new Fields(fields.toList))
            
          }
            
          case _ => throw new Exception("Information concept is unknown.")
        
        }
        
        new ServiceResponse(req.service,req.task,Map("uid"-> uid),ResponseStatus.SUCCESS)
        
      } catch {
        case throwable:Throwable => failure(req,throwable.getMessage)
      }
      
      origin ! Serializer.serializeResponse(response)

    }
    
  }

}