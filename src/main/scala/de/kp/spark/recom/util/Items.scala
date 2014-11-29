package de.kp.spark.recom.util
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
import de.kp.spark.core.redis.RedisClient

import scala.collection.JavaConversions._

object Items {

  private val client = RedisClient()
  
  private def buildFromRedis(req:ServiceRequest):Seq[String] = {
        
    val k = "items:" + req.data("site")
    val data = client.zrange(k, 0, -1)

    val users = if (data.size() == 0) {
      List.empty[String]
    
    } else {
      
      data.map(record => {
        /* format = timestamp:item:name */
        val Array(timestamp,iid,name) = record.split(":")
        iid
        
      }).toList
      
    }
    
    users
  
  }
  
  def get(req:ServiceRequest):Dict = {
    /**
     * Actually the user database is retrieved from
     * Redis instance
     */
    new Dict().build(buildFromRedis(req))
    
  }

}