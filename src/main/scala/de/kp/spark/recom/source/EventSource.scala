package de.kp.spark.recom.source
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

import de.kp.spark.core.Names
import de.kp.spark.core.redis.RedisCache

import de.kp.spark.core.source.FileSource

import de.kp.spark.core.model._

import de.kp.spark.recom.Configuration
import de.kp.spark.recom.model._

import de.kp.spark.recom.util.{Users,Items}

class EventSource(@transient sc:SparkContext) {

  val (host,port) = Configuration.redis
  val cache = new RedisCache(host,port.toInt)
  /**
   * The method retrieves the list of items that have been rated by a certain user and 
   * uses the additional context information for filtering; the respective items are 
   * specified by their column positions in the feature representation of an event.
   */
  def getItemsByCol(req:ServiceRequest):List[Int] = {
    
    val algorithm = req.data(Names.REQ_ALGORITHM)
    if (algorithm == Algorithms.CAR) {
    
      val path = Configuration.file(0)
      val rawset = new FileSource(sc).connect(path,req)

      val (user,context) = columns(req)
      
      val busr = sc.broadcast(user)
      val bctx  = sc.broadcast(context)
      /*
       * We restrict to those lines in the dataset that refer to the
       * provided and further restrict to those that match the provided
       * context and values
       */
      val filtered = rawset.filter(line => {
        
        val Array(target,point) = line.split(":")
        val features = point.split(":").map(_.toDouble)
        
        val usr = busr.value
        val ctx = bctx.value
        
        val is_user = features(usr) == 1.toDouble
        val has_ctx = (ctx.isEmpty == false) && ctx.map(kv => if (features(kv._1) == kv._2) 0 else 1).max == 0
        
        is_user && has_ctx
        
      })
      
      /*
       * Determine start and end point of item block 
       */
      val start = Users.get(req).size
      val end   = start + Items.get(req).size - 1
      
      val bs = sc.broadcast(start)
      val be   = sc.broadcast(end)
      
      filtered.map(line => {
        
        val Array(target,point) = line.split(":")
        /* 
         * Slice items from feature vector; as this is a binary representation
         * each sliced array has a single column that is different from '0' 
         */
        val items = point.split(":").map(_.toDouble).slice(bs.value,be.value)
        /*
         * Determine the column position by using the start point
         * of the item block as an offset for the sliced array
         */
        bs.value + items.indexOf(1.toDouble)
        
      }).collect().toList
      
    } else {
      throw new Exception("Recommending items for an event data source is restricted to the CAR algorithm.")
    
    }

  }
  
  /**
   * Private method to determine the column positions of the provided 
   * user name and context column name
   */
  private def columns(req:ServiceRequest):(Int,List[(Int,Double)]) = {
    
    val user =  req.data(Names.REQ_USER)
    /* context is a key-value data structure of the form k,v;k,v; */
    val context = if (req.data.contains(Names.REQ_CONTEXT)) req.data(Names.REQ_CONTEXT).split(";").toList else List.empty[String]

    val fields = cache.fields(req)   
    val lookup = fields.zipWithIndex.map(x => (x._1.name,x._2)).toMap
    
    val uid = lookup(user)
    val cid = context.map(kv => {
      
      val Array(name,value) = kv.split(",")
      (lookup(name),value.toDouble)
    
    })
    
    (uid,cid)
    
  }
  
}