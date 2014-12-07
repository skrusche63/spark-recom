package de.kp.spark.recom.util
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

import scala.collection.mutable.{Buffer,Map}

class Dict extends Serializable {

  /**
   * Reference to the external data designators is used
   * to speed up data access
   */
  val terms  = Buffer.empty[String]

  val field2index = Map.empty[String,Int]
  val index2field = Map.empty[Int,String]
  
  /**
   * Build a dictionary from a distint sequence of terms 
   */
  def build(seq:Seq[String]):Dict = {
  
    seq.map(entry => terms += entry)
    
    seq.zipWithIndex.map(entry => field2index += entry._1 -> entry._2)
    seq.zipWithIndex.map(entry => index2field += entry._2 -> entry._1)
    
    this
    
  }
  
  def size = terms.size
  
}