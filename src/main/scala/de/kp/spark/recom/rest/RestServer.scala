package de.kp.spark.recom.rest
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

import akka.actor.ActorSystem

import de.kp.spark.core.SparkService
import de.kp.spark.recom.Configuration

object RestServer extends SparkService {
  
  /* Create Spark context */
  private val sc = createCtxLocal("RecomContext",Configuration.spark)      
  
  private def start(args:Array[String],system:ActorSystem) {

    val (host,port) = Configuration.rest
    
    /* Start REST API */
    new RestApi(host,port,system,sc).start()
      
  }
  
  def main(args: Array[String]) {
    start(args, ActorSystem("RecomServer"))
  }
  
}