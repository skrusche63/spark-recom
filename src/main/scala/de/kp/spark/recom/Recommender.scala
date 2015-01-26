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

import org.apache.spark.mllib.recommendation.{ALS,MatrixFactorizationModel,Rating}

import de.kp.spark.core.model._
import de.kp.spark.core.redis.RedisDB

import de.kp.spark.recom.cache.ALSCache
import de.kp.spark.recom.model._

import de.kp.spark.recom.hadoop.HadoopIO
import de.kp.spark.recom.source.ItemSource

import de.kp.spark.recom.util.{Dict,Items,Users}

class RecommenderModel(@transient ctx:RequestContext,req:ServiceRequest) {

  private val (host,port) = Configuration.redis
  private val sink = new RedisDB(host,port.toInt)
 
  private val model = HadoopIO.readRecom(sink.model(req))
  private val users = Users.get(req)
    
  /**
   * For a certain (site,user) predict the 'k' highest scored
   * items
   */
  def predict(site:String,user:String,item:Int):List[Preference] = {
    
    val uid = users.field2index(user)
    
    val rating = model.predict(uid, item)
    List(Preference(site,user,item,rating))
    
  }

  /**
   * For a certain (site,user) combination and a list of provided items,
   * this method predicts the associated ratings or scores.
   */
  def predict(site:String,user:String,items:List[Int]):List[Preference] = {

    val uid = users.field2index(user)
    val candidates = ctx.sc.parallelize(items)
    
    val ratings = model.predict(candidates.map((uid, _))).collect
    ratings.sortBy(-_.rating).map(x => Preference(site,user,x.product,x.rating)).toList
    
  }
  /**
   * For a combined (user,item) list, this method predicts the associated ratings
   * or scores
   */
  def predict(site:String,users:List[String],items:List[Int]):List[Preference] = {
    
    val pairs = users.zip(items).toList
    predict(site,pairs)

  }

  /**
   * For a combined (user,item) list, this method predicts the associated ratings
   * or scores
   */
  private def predict(site:String,pairs:List[(String,Int)]):List[Preference] = {
    
    val data = ctx.sc.parallelize(pairs.map(pair => {
      
      val uid = users.field2index(pair._1)
      val iid = pair._2
      
      (uid,iid)
      
    }))

    val ratings = model.predict(data).collect
    ratings.sortBy(-_.rating).map(x => Preference(site,users.index2field(x.user),x.product,x.rating)).toList
    
  }
  
  /**
   * Recommends items to a user. The number returned may be less than the total.
   */
  def recommend(site:String,user:String,total:Int):List[Preference] = {
    
    val uid = users.field2index(user)
    
    val ratings = model.recommendProducts(uid, total)
    ratings.sortBy(-_.rating).map(x => Preference(site,user,x.product,x.rating)).toList
    
  }

  /**
   * Recommends users to a product. That is, this returns users who are most likely 
   * to be interested in a product.
   */  
  def recommend(site:String,item:Int,total:Int):List[Preference] = {
    val ratings = model.recommendProducts(item, total)
    ratings.sortBy(-_.rating).map(x => Preference(site,users.index2field(x.user),x.product,x.rating)).toList
  }
  
  /**
   * The private method retrieves an ALS model either from the 
   * ALSCache or from the Hadoop file system
   */
  private def get(req:ServiceRequest):MatrixFactorizationModel = {
    
    val uid = req.data("uid")
    if (ALSCache.exists(uid)) {
      ALSCache.get(uid)
    
    } else {
      
      val model = HadoopIO.readRecom(sink.model(req))
      ALSCache.add(uid,model)
      
      model
      
    }
    
  }
  
}

class Recommender(@transient ctx:RequestContext) extends Serializable {

  def train(req:ServiceRequest):MatrixFactorizationModel = {
    
    /* Partitions used to partition the training dataset */
    val partitions = if (req.data.contains("partitions")) req.data("partitions").toInt else 20
    
    val rank = if (req.data.contains("rank")) req.data("rank").toInt else 10
    val iter = if (req.data.contains("iter")) req.data("iter").toInt else 20

    val lambda = if (req.data.contains("lambda")) req.data("lambda").toDouble else 0.01

    /*
     * The format of the 'items' is: site(String), user(String), 
     * item (Int) and preference (Int)
     */
    val source = new ItemSource(ctx)
    val nprefs = source.get(req).repartition(partitions)
    /*
     * Create user dictionary as the ALS predictor requires 
     * integers to uniquely specify users
     */
    val users = Users.get(req)
    val busers = ctx.sc.broadcast(users)

    val items = Items.get(req)
    /* Convert to spark ratings */
    val ratings = nprefs.map(x => {

      val (site,user,iid,score) = x
      
      val uid = busers.value.field2index(user)
      Rating(uid,iid,score)
      
    })
    
    /* Build model */
    ALS.train((ratings).repartition(partitions),rank,iter,lambda)        

  }

}