package de.kp.spark.recom.format
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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.recom.util.{Dict,Events,Items,Users}

class CARFormatter extends Serializable {

  def format(req:ServiceRequest):Array[Double] = {
    
    val idict = Items.get(req)
    val udict = Users.get(req)
    
    val user = req.data(Names.REQ_USER)
    val item = req.data(Names.REQ_ITEM) 
    
    /*
     * Determine user block; we actually ignore the 'site'
     * parameter here, as it is expected that the respective
     * directories are retrieved on a per 'site' basis
     */
    val ublock = activeUserBlock(user,udict)
        
    /* Build block to descibe the active (rated) item */
    val iblock = activeItemBlock(item,idict)
    
    val context = req.data(Names.REQ_CONTEXT).split(",").map(_.toDouble)
    ublock ++ iblock ++ context
    
  }
  
  /**
   * This method determines the user vector part from the externally
   * known and provided unique user identifier
   */
  private def activeUserBlock(uid:String,udict:Dict):Array[Double] = {
    
    val block = Array.fill[Double](udict.size)(0.0)
    
    val pos = udict.getLookup(uid)
    block(pos) = 1
    
    block
    
  }

  /**
   * This method determines the item vector part from the externally
   * known and provided unique item identifier
   */
  private def activeItemBlock(iid:String, idict:Dict):Array[Double] = {
    
    val block = Array.fill[Double](idict.size)(0.0)
    
    val pos = idict.getLookup(iid)
    block(pos) = 1
    
    block
    
  }
  
}