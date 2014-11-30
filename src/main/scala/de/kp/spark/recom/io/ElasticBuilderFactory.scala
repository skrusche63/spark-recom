package de.kp.spark.recom.io
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

import org.elasticsearch.common.xcontent.XContentBuilder
import de.kp.spark.core.elastic.ElasticItemBuilder

object ElasticBuilderFactory {
  /*
   * Definition of common parameters for all indexing tasks
   */
  val TIMESTAMP_FIELD:String = "timestamp"

  val SITE_FIELD:String = "site"
  val USER_FIELD:String = "user"

  val GROUP_FIELD:String = "group"
  val ITEM_FIELD:String  = "item"

  val EVENT_FIELD:String = "event"

  def getBuilder(builder:String,mapping:String):XContentBuilder = {
    
    builder match {

      case "event" => new ElasticEventBuilder().createBuilder(mapping)
      case "item"  => new ElasticItemBuilder().createBuilder(mapping)
      
      case _ => null
      
    }
  
  }

}