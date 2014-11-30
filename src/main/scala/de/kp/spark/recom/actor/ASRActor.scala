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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.core.model._
import de.kp.spark.recom.model._

import de.kp.spark.recom.RemoteContext
import scala.concurrent.Future

/**
 * ASRActor is responsible for interaction with the Association
 * Analysis engine to build recommendations from association rules
 */
class ASRActor(@transient sc:SparkContext,rtx:RemoteContext) extends RecomWorker(sc) {
  /**
   * Recommendations based on association rules do not need to
   * build user preferences first; therefore, the request is
   * delegated to mining the respective association rules
   */
  def doBuildRequest(req:ServiceRequest) {

    val service = "association"    
    doTrainRequest(new ServiceRequest(service,"train",req.data))
    
  }
  /**
   * In case of association rule based recommendation models, the 
   * term 'model' is equivalent to the respective association rules
   */
  def doTrainRequest(req:ServiceRequest) {
      
    val service = req.service
    val message = Serializer.serializeRequest(req)
    /*
     * Mining association rules is a fire-and-forget task
     * from the recommendation service prespective
     */
    rtx.send(service,message)
    
  }
  
  def doGetRequest(req:ServiceRequest):Future[Any] = {

    val service = "association"
    val message = Serializer.serializeRequest(new ServiceRequest(service,"get:recommendation",req.data))
    
    rtx.send(service,message)
    
  }

  override def buildGetResponse(req:ServiceRequest,intermediate:ServiceResponse):Any = null
  
}