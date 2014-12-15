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

import de.kp.spark.core.source._
import de.kp.spark.core.model._

import de.kp.spark.recom.Configuration
import de.kp.spark.recom.model._

import de.kp.spark.recom.format.CARFormatter
import scala.collection.mutable.WrappedArray

class EventSource(@transient sc:SparkContext) {

  val (host,port) = Configuration.redis
  val cache = new RedisCache(host,port.toInt)
  
  /**
   * This method retrieves the 'rated items' block of a certain user from the computed
   * preferences or ratings. The 'rated item block' is the same for each active item of
   * the user. Therefore, only the first user record is taken into account.
   * 
   * The result is used to specify a feature vector from event orient input information.
   */
  def ratedItemsAsCols(req:ServiceRequest,formatter:CARFormatter):Array[Double] = {
    
    val user = req.data(Names.REQ_USER)
    /*
     * Determine start & end position for the rated item block
     */
    val userCount = formatter.userCount
    val itemCount = formatter.itemCount
    
    val start = userCount + itemCount
    val end = start + itemCount - 1
    
    req.data(Names.REQ_SOURCE) match {
      
      case Sources.FILE => {

        val path = Configuration.input(0)
        val rawset = new FileSource(sc).connect(path,req)
    
        val busr = sc.broadcast(formatter.userAsCol(user))      
        val head = rawset.filter(line => {
        
          val Array(target,point) = line.split(",")
          val features = point.split(" ").map(_.toDouble)
      
          features(busr.value) == 1.toDouble
    
        }).take(1)(0)
    
        val Array(target,point) = head.split(",")    
        point.split(" ").map(_.toDouble).slice(start,end)
        
      }
      
      case Sources.PARQUET => {

        val path = Configuration.input(0)
        val rawset = new ParquetSource(sc).connect(path,req,List.empty[String])
    
        val busr = sc.broadcast(formatter.userAsCol(user))      
        val head = rawset.filter(record => {
        
          val seq = record.toSeq
          val features = seq(1)._2.asInstanceOf[WrappedArray[Double]]
      
          features(busr.value) == 1.toDouble
    
        }).take(1)(0)
        
        val point = head.toSeq(0)._2.asInstanceOf[WrappedArray[Double]]
        point.slice(start,end).toArray
 
      }
      
      case _ => throw new Exception("This event data source is not supported.")
      
    }
    
  }
  
  def activeUsersAsList(req:ServiceRequest,formatter:CARFormatter):List[Int] = {

    val (item,context) = itemFieldsAsCols(req)
      
    val bitm = sc.broadcast(item)
    val bctx = sc.broadcast(context)

    /*
     * Determine start and end point of user block 
     */
    val start = 0
    val end   = start + formatter.userCount - 1
      
    val bs = sc.broadcast(start)
    val be = sc.broadcast(end)

    req.data(Names.REQ_SOURCE) match {
      
      case Sources.FILE => {
    
        val path = Configuration.input(0)
        val rawset = new FileSource(sc).connect(path,req)
        /*
         * We restrict to those lines in the dataset that refer to the provided
         * item and further restrict to those that match the provided context
         */
        val filtered = rawset.filter(line => {
        
          val Array(target,point) = line.split(",")
          val features = point.split(" ").map(_.toDouble)
        
          val itm = bitm.value
          val ctx = bctx.value
        
          val is_item = features(itm) == 1.toDouble
          val has_ctx = (ctx.isEmpty == false) && ctx.map(kv => if (features(kv._1) == kv._2) 0 else 1).max == 0
        
          is_item && has_ctx
        
        })
      
        filtered.map(line => {
        
          val Array(target,point) = line.split(",")
          /* 
           * Slice users from feature vector; as this is a binary representation
           * each sliced array has a single column that is different from '0' 
           */
          val users = point.split(" ").map(_.toDouble).slice(bs.value,be.value)
          /*
           * Determine the column position by using the start point
           * of the user block as an offset for the sliced array
           */
          bs.value + users.indexOf(1.toDouble)
        
        }).collect().toList.distinct
        
      }
      
      case Sources.PARQUET => {
        
        val path = Configuration.input(0)
        val rawset = new ParquetSource(sc).connect(path,req,List.empty[String])
        /*
         * We restrict to those lines in the dataset that refer to the provided
         * item and further restrict to those that match the provided context
         */
        val filtered = rawset.filter(record => {
        
          val seq = record.toSeq
          val features = seq(1)._2.asInstanceOf[WrappedArray[Double]]
        
          val itm = bitm.value
          val ctx = bctx.value
        
          val is_item = features(itm) == 1.toDouble
          val has_ctx = (ctx.isEmpty == false) && ctx.map(kv => if (features(kv._1) == kv._2) 0 else 1).max == 0
        
          is_item && has_ctx
        
        })
      
        filtered.map(record => {
        
          val seq = record.toSeq
          val point = seq(1)._2.asInstanceOf[WrappedArray[Double]]
        
          /* 
           * Slice users from feature vector; as this is a binary representation
           * each sliced array has a single column that is different from '0' 
           */
          val users = point.slice(bs.value,be.value)
          /*
           * Determine the column position by using the start point
           * of the user block as an offset for the sliced array
           */
          bs.value + users.indexOf(1.toDouble)
        
        }).collect().toList.distinct
        
      }
      
      case _ => throw new Exception("This event data source is not supported.")
      
    }
  
  }
  
  /**
   * The method retrieves the list of active items that have been rated by a certain user and 
   * uses the additional context information for filtering; the respective items are 
   * specified by their column positions in the feature representation of an event.
   */
  def activeItemsAsList(req:ServiceRequest,formatter:CARFormatter):List[Int] = {

    val (user,context) = userFieldsAsCols(req)
      
    val busr = sc.broadcast(user)
    val bctx = sc.broadcast(context)
      
    /*
     * Determine start and end point of item block 
     */
    val start = formatter.userCount
    val end   = start + formatter.itemCount - 1
      
    val bs = sc.broadcast(start)
    val be = sc.broadcast(end)

    req.data(Names.REQ_SOURCE) match {
      
      case Sources.FILE => {
    
        val path = Configuration.input(0)
        val rawset = new FileSource(sc).connect(path,req)
        /*
         * We restrict to those lines in the dataset that refer to the provided
         * user and further restrict to those that match the provided context
         */
        val filtered = rawset.filter(line => {
        
          val Array(target,point) = line.split(",")
          val features = point.split(" ").map(_.toDouble)
        
          val usr = busr.value
          val ctx = bctx.value
        
          val is_user = features(usr) == 1.toDouble
          val has_ctx = (ctx.isEmpty == false) && ctx.map(kv => if (features(kv._1) == kv._2) 0 else 1).max == 0
        
          is_user && has_ctx
        
        })
      
        filtered.map(line => {
        
          val Array(target,point) = line.split(",")
          /* 
           * Slice items from feature vector; as this is a binary representation
           * each sliced array has a single column that is different from '0' 
           */
          val items = point.split(" ").map(_.toDouble).slice(bs.value,be.value)
          /*
           * Determine the column position by using the start point
           * of the item block as an offset for the sliced array
           */
          bs.value + items.indexOf(1.toDouble)
        
        }).collect().toList.distinct
     
      }
      
      case Sources.PARQUET => {
    
        val path = Configuration.input(0)
        val rawset = new ParquetSource(sc).connect(path,req,List.empty[String])
        /*
         * We restrict to those lines in the dataset that refer to the provided
         * user and further restrict to those that match the provided context
         */
        val filtered = rawset.filter(record => {
        
          val seq = record.toSeq
          val features = seq(1)._2.asInstanceOf[WrappedArray[Double]]
        
          val usr = busr.value
          val ctx = bctx.value
        
          val is_user = features(usr) == 1.toDouble
          val has_ctx = (ctx.isEmpty == false) && ctx.map(kv => if (features(kv._1) == kv._2) 0 else 1).max == 0
        
          is_user && has_ctx
        
        })
      
        filtered.map(record => {
        
          val seq = record.toSeq
          val point = seq(1)._2.asInstanceOf[WrappedArray[Double]]
          /* 
           * Slice items from feature vector; as this is a binary representation
           * each sliced array has a single column that is different from '0' 
           */
          val items = point.slice(bs.value,be.value)
          /*
           * Determine the column position by using the start point
           * of the item block as an offset for the sliced array
           */
          bs.value + items.indexOf(1.toDouble)
        
        }).collect().toList.distinct
     
      }
      
      case _ => throw new Exception("This event data source is not supported.")
      
    }

  }
  
  /**
   * Private method to determine the column positions of the provided 
   * user & context fields
   */
  private def userFieldsAsCols(req:ServiceRequest):(Int,List[(Int,Double)]) = {
    
    val user =  req.data(Names.REQ_USER)

    val fields = cache.fields(req)   
    val lookup = fields.zipWithIndex.map(x => (x._1.name,x._2)).toMap
    
    val uid = lookup(user)
    val cid = context(req).map(kv => {
      
      val (name,value) = kv
      (lookup(name),value.toDouble)
    
    }).toList
    
    (uid,cid)
    
  }
  /**
   * Private method to determine the column positions of the provided 
   * item & context fields
   */
  private def itemFieldsAsCols(req:ServiceRequest):(Int,List[(Int,Double)]) = {
    
    val item =  req.data(Names.REQ_ITEM)

    val fields = cache.fields(req)   
    val lookup = fields.zipWithIndex.map(x => (x._1.name,x._2)).toMap
    
    val iid = lookup(item)
    val cid = context(req).map(kv => {
      
      val (name,value) = kv
      (lookup(name),value.toDouble)
    
    }).toList
    
    (iid,cid)
    
  }
  
  private def context(req:ServiceRequest):Map[String,String] = {
    
    val filter = List(Names.REQ_EVENT,Names.REQ_DAY_OF_WEEK,Names.REQ_HOUR_OF_DAY,Names.REQ_RELATED)
    req.data.filter(kv => filter.contains(kv._1))

  }
  
}