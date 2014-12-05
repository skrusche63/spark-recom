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

import de.kp.spark.core.Names

import de.kp.spark.core.model._
import de.kp.spark.core.redis.RedisDB

import de.kp.spark.recom.Configuration

abstract class DBObject extends Serializable {

  val (host,port) = Configuration.redis
  val client = new RedisDB(host,port.toInt)
  
  def exists(req:ServiceRequest,topic:String):Boolean = {
    
    val k = topic + ":" + req.data(Names.REQ_UID) + ":" + req.data(Names.REQ_NAME)
    client.exists(k)
    
  }
  
  def get(req:ServiceRequest):Dict
  
}

object Events extends DBObject {
  
  override def get(req:ServiceRequest):Dict = new Dict().build(client.events(req))  
  def exists(req:ServiceRequest):Boolean = super.exists(req,"event")
  
}

object Items extends DBObject {
  
  override def get(req:ServiceRequest):Dict = new Dict().build(client.items(req))
  def exists(req:ServiceRequest):Boolean = super.exists(req,"item")

}

object Users extends DBObject {
  
  override def get(req:ServiceRequest):Dict = new Dict().build(client.users(req))
  def exists(req:ServiceRequest):Boolean = super.exists(req,"user")

}
