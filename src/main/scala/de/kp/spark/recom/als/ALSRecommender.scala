package de.kp.spark.recom.als
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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.recommendation.{ALS,MatrixFactorizationModel,Rating}

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.recom.{Configuration,RequestContext}
import de.kp.spark.recom.model._

class RDict(val elems:Seq[String]) extends Serializable {
 
  val lookup = elems.zipWithIndex.toMap

  val getIndex = lookup
  val getTerm = elems

  val size = elems.size

}

class ALSRecommenderModel(@transient ctx:RequestContext,req:ServiceRequest) {

  private val params = req.data
    
  private val uid = params(Names.REQ_UID)
  private val name = params(Names.REQ_NAME)
      
  /*
   * Retrieve matrix factorization and associated user and product
   * lookup dictionaries from the file system
   */
  val store = String.format("""%s/%s/%s/2""",ctx.base,name,uid)         
  val (udict,idict,model) = new MFUtil(ctx.sc).read(store)
    
  val ulookup = udict.map{case (user,uid) => (uid,user)}
  val ilookup = idict.map{case (item,iid) => (iid,item)}
    
  /**
   * For a certain (site,user) predict the 'k' highest scored
   * items
   */
  def predict(site:String,user:String,item:Int):List[Preference] = {
    
    val uid = udict(user)
    val iid = idict(item.toString)
    
    val rating = model.predict(uid,iid)
    List(Preference(site,user,item,rating))
    
  }

  /**
   * For a certain (site,user) combination and a list of provided items,
   * this method predicts the associated ratings or scores.
   */
  def predict(site:String,user:String,items:List[Int]):List[Preference] = {

    val uid = udict(user)
    val iids = items.map(x => idict(x.toString))
    
    val ilookup = idict.map{case (item,iid) => (iid,item)}
    val candidates = ctx.sc.parallelize(iids)
    
    val ratings = model.predict(candidates.map((uid, _))).collect
    ratings.sortBy(-_.rating).map(x => Preference(site,user,ilookup(x.product).toInt,x.rating)).toList
    
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
      
      val uid = udict(pair._1)
      val iid = idict(pair._2.toString)
      
      (uid,iid)
      
    }))

    val ratings = model.predict(data).collect
    ratings.sortBy(-_.rating).map(x => Preference(site,ulookup(x.user),ilookup(x.product).toInt,x.rating))
    
    null
    
  }
  
  /**
   * Recommends items to a user. The number returned may be less than the total.
   */
  def recommend(site:String,user:String,total:Int):List[Preference] = {
    
    val uid = udict(user)
    
    val ratings = model.recommendProducts(uid, total)
    ratings.sortBy(-_.rating).map(x => Preference(site,user,x.product,x.rating)).toList
    
  }

  /**
   * Recommends users to a product. That is, this returns users who are most likely 
   * to be interested in a product.
   */  
  def recommend(site:String,item:Int,total:Int):List[Preference] = {
  
    val iid = idict(item.toString)
    
    val ratings = model.recommendProducts(item, total)
    ratings.sortBy(-_.rating).map(x => Preference(site,ulookup(x.user),ilookup(x.product).toInt,x.rating)).toList
  
  }
  
}

class ALSRecommender(@transient ctx:RequestContext) extends Serializable {

  def train(req:ServiceRequest) {
    
    val uid = req.data(Names.REQ_UID)
    val name = req.data(Names.REQ_NAME)
    
    /* Partitions used to partition the training dataset */
    val partitions = if (req.data.contains("partitions")) req.data("partitions").toInt else 20
    
    val rank = if (req.data.contains("rank")) req.data("rank").toInt else 10
    val iter = if (req.data.contains("iter")) req.data("iter").toInt else 20

    val lambda = if (req.data.contains("lambda")) req.data("lambda").toDouble else 0.01

    val table = preferences(req.data)
    
    val users = table.groupBy(x => x._2).map(_._1).collect
    val udict = ctx.sc.broadcast(new RDict(users))
        
    val items = table.groupBy(x => x._3).map(_._1.toString).collect
    val idict = ctx.sc.broadcast(new RDict(items))
        
    val trainset = table.map(x => {
          
      val ux = udict.value.getIndex(x._2)
      val ix = idict.value.getIndex(x._3.toString)
          
      Rating(ux,ix,x._4)
          
    })        
        
    val model = ALS.train((trainset).repartition(partitions),rank,iter,lambda)        
    val rmse = computeRMSE(model,trainset)
        
    val modelMF = new MFModel(rank,rmse,model.userFeatures,model.productFeatures)

    val store = String.format("""%s/%s/%s/2""",ctx.base,name,uid)         
    new MFUtil(ctx.sc).write(store,udict.value.lookup,idict.value.lookup,modelMF)

  }
  
  private def computeRMSE(model:MatrixFactorizationModel,ratings:RDD[Rating]): Double = { 

    val dataset = ratings.map(x => (x.user,x.product))
    val predictions = model.predict(dataset).map(x => ((x.user,x.product),x.rating))
  
    Math.sqrt(ratings
      .map(x => ((x.user,x.product), x.rating)).join(predictions)
      .map{case ((ucol, icol), (r1, r2)) => {
             
        val err = (r1 - r2)
        err * err
            
      }}.mean())
    
  }

  private def preferences(params:Map[String,String]):RDD[(String,String,Int,Double)] = {
    
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)
    
    val store = String.format("""%s/%s/%s/1""",ctx.base,name,uid)
    
    val parquetFile = ctx.sqlc.parquetFile(store)
    val metadata = parquetFile.schema.fields.zipWithIndex
    
    parquetFile.map(row => {

      val values = row.iterator.zipWithIndex.map(x => (x._2,x._1)).toMap
      val data = metadata.map(entry => {
      
        val (field,col) = entry
      
        val colname = field.name
        val colvalu = values(col)
      
        (colname,colvalu)
          
      }).toMap

      val site = data("site").asInstanceOf[String]
      val user = data("user").asInstanceOf[String]

      val item = data("item").asInstanceOf[Int]
      val score = data("score").asInstanceOf[Double]
    
      (site,user,item,score)
      
    })
    
  }

}