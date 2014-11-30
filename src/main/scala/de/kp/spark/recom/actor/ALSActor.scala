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

import de.kp.spark.recom.{Configuration,Recommender,RemoteContext}

import de.kp.spark.recom.sink.RedisSink
import de.kp.spark.recom.hadoop.HadoopIO

import scala.concurrent.Future

class ALSActor(@transient sc:SparkContext,rtx:RemoteContext) extends RecomWorker(sc) {
  
  private val sink = new RedisSink()
  
  /**
   * The user rating is built by delegating the request to the 
   * remote rating service; this Akka service represents the 
   * User Preference engine of Predictiveworks.
   */
  def doBuildRequest(req:ServiceRequest) {
      
    val service = req.service
    val message = Serializer.serializeRequest(req)
    /*
     * Building user rating is a fire-and-forget task
     * from the recommendation service prespective
     */
    rtx.send(service,message)
    
  }
 
  def doTrainRequest(req:ServiceRequest) {
          
    cache.addStatus(req,ResponseStatus.TRAINING_STARTED)
    /*
     * Build ALS recommendation model based on the request data;
     * users & items refer to the reference information that is 
     * used to map a certain user (uid) and item (iid) to the 
     * Integer representation required by the ALS algorithm
     */
    val model = new Recommender(sc).train(req)
          
    /* Register model */
    val now = new java.util.Date()
    val dir = Configuration.model + "/als-" + now.getTime().toString
    /*
     * The ALS model trained is saved on the HDFS file system and
     * must be load before building any recommendations
     */
    HadoopIO.writeRecom(model,dir)
    
    sink.addModel(req,dir)          
    cache.addStatus(req,ResponseStatus.TRAINING_FINISHED)

 }

  def doGetRequest(req:ServiceRequest):Future[Any] = null

  def buildGetResponse(req:ServiceRequest,intermediate:ServiceResponse):Any = null
  
}