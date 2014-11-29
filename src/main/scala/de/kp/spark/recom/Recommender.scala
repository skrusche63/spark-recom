package de.kp.spark.recom
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
import org.apache.spark.mllib.recommendation.{ALS,MatrixFactorizationModel,Rating}

import de.kp.spark.core.model._
import de.kp.spark.recom.hadoop.HadoopIO

import de.kp.spark.recom.source.NPrefSource

import de.kp.spark.recom.sink.RedisSink
import de.kp.spark.recom.util.{Dict,Items,Users}

class RecommenderModel(@transient sc:SparkContext,req:ServiceRequest) {
  
  private val sink = new RedisSink()
  private val model = HadoopIO.readRecom(sink.model(req))

  // TODO
  
}


class Recommender(@transient sc:SparkContext) extends Serializable {

  def train(req:ServiceRequest):MatrixFactorizationModel = {
    
    /* Partitions used to partition the training dataset */
    val partitions = if (req.data.contains("partitions")) req.data("partitions").toInt else 20
    
    val rank = if (req.data.contains("rank")) req.data("rank").toInt else 10
    val iter = if (req.data.contains("iter")) req.data("iter").toInt else 20

    val lambda = if (req.data.contains("lambda")) req.data("lambda").toDouble else 0.01

    /*
     * The format of the 'items' is; site(String), user(String), 
     * item (Int) and preference (Int)
     */
    val source = new NPrefSource(sc)
    val nprefs = source.get(req).repartition(partitions)
    /*
     * Create user dictionary as the ALS predictor requires 
     * integers to uniquely specify users
     */
    val users = Users.get(req)
    val busers = sc.broadcast(users)

    val items = Items.get(req)
    /* Convert to spark ratings */
    val ratings = nprefs.map(x => {

      val (site,user,iid,score) = x
      
      val uid = busers.value.getLookup(user)
      Rating(uid,iid,score)
      
    })
    
    /* Build model */
    ALS.train((ratings).repartition(partitions),rank,iter,lambda)        

  }

}