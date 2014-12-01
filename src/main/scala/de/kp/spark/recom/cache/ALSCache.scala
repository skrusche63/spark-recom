package de.kp.spark.recom.cache
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

import java.util.Date

import de.kp.spark.recom.Configuration
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel}

import scala.collection.mutable.ArrayBuffer

object ALSCache {
  
  private val size = Configuration.cache
  private val cache = new LRUCache[String,MatrixFactorizationModel](size)

  def add(uid:String,model:MatrixFactorizationModel) {
    /*
     * In case of an already existing model that refers 
     * to the same unique task identifier, the model is 
     * replaced by the provided one   
     */     
    cache.put(uid,model)
    
  }
  
  def exists(uid:String):Boolean = cache.containsKey(uid)

  def get(uid:String):MatrixFactorizationModel = cache.get(uid).get
  
}