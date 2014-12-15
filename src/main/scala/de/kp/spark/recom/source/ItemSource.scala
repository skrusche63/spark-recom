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
import de.kp.spark.core.model._

import de.kp.spark.core.source._

import de.kp.spark.recom.Configuration
import de.kp.spark.recom.model._

class ItemSource(@transient sc:SparkContext) {

  def get(req:ServiceRequest):RDD[(String,String,Int,Int)] = {
   
    val algorithm = req.data(Names.REQ_ALGORITHM)
    if (algorithm == Algorithms.ALS) {
    
      req.data(Names.REQ_SOURCE) match {
        
        case Sources.FILE => {
       
          val path = Configuration.input(1)
          val rawset = new FileSource(sc).connect(path,req)
          rawset.map(line => {
        
            val Array(site,user,item,pref) = line.split(",")
            (site,user,item.toInt,pref.toInt)
        
          })
         
        }
        
        case Sources.PARQUET => {
           
          val path = Configuration.input(1)
          val rawset = new ParquetSource(sc).connect(path,req,List.empty[String])
         
          rawset.map(record => {
            
            val site = record(Names.REQ_SITE).asInstanceOf[String]
            val user = record(Names.REQ_USER).asInstanceOf[String]

            val item = record(Names.REQ_ITEM).asInstanceOf[Int]
            val score = record(Names.REQ_SCORE).asInstanceOf[Double]
           
            (site,user,item,score.toInt)
          })

        }
        
        case _ => throw new Exception("This item data source is not supported by the ALS algorithm.")
        
      }
      
    } else {
      throw new Exception("Recommending items for an item data source is restricted to the ALS algorithm.")
    }
    
  }

}