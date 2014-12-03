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

import de.kp.spark.core.Names
import de.kp.spark.core.spec.FieldBuilder

import de.kp.spark.core.model._
import de.kp.spark.recom.model._

/**
 * RecomRegistrar registers different field specifications in
 * a Redis instance, note, that there is actually a difference
 * in the description of 'item': for item-based data sources,
 * an 'Integer ' datatype is required, while for event-based
 * sources a 'String' datatype must be used.
 * 
 * This heterogenity will be removed with one of the next releases.
 */
class RegisterActor extends BaseActor {
  
  def receive = {
    
    case req:ServiceRequest => {

      val uid = req.data(Names.REQ_UID)      
      val origin = sender   
      
      val response = try {
        /*
         * Topic is either 'event' or 'item'
         */
        val topic = req.task.split(":")(1)
        
        val fields = new FieldBuilder().build(req,topic)
        cache.addFields(req, fields)
        
        new ServiceResponse(req.service,req.task,Map(Names.REQ_UID-> uid),ResponseStatus.SUCCESS)
        
      } catch {
        case throwable:Throwable => failure(req,throwable.getMessage)
      }
      
      origin ! response

    }
    
  }

}