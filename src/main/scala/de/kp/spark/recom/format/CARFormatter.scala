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

import de.kp.spark.recom.RequestContext

import de.kp.spark.recom.source.EventSource
import de.kp.spark.recom.util.{Dict,Events,Items,Users}

class CARFormatter(@transient ctx:RequestContext,req:ServiceRequest) extends Serializable {

  private val edict = Events.get(req)
    
  private val idict = Items.get(req)
  private val udict = Users.get(req)

  def userCount = udict.size
  def index2user = udict.index2field
  
  def itemCount = idict.size
  def index2item = idict.index2field
  
  def format:Array[Double] = {
    
    val user = req.data(Names.REQ_USER)
    val item = req.data(Names.REQ_ITEM) 
    
    /*
     * Determine user block; we actually ignore the 'site'
     * parameter here, as it is expected that the respective
     * directories are retrieved on a per 'site' basis
     */
    val ublock = activeUserCols(user,udict)
        
    /* Build block to descibe the active (rated) item */
    val iblock = activeItemCols(item,idict)
    
    val context = contextAsCols
    ublock ++ iblock ++ context
    
  }
  
  /**
   * This method determines the user vector part from the externally
   * known and provided unique user identifier
   */
  private def activeUserCols(uid:String,udict:Dict):Array[Double] = {
    
    val block = Array.fill[Double](udict.size)(0.0)
    
    val pos = udict.field2index(uid)
    block(pos) = 1
    
    block
    
  }

  /**
   * This method determines the item vector part from the externally
   * known and provided unique item identifier
   */
  private def activeItemCols(iid:String, idict:Dict):Array[Double] = {
    
    val block = Array.fill[Double](idict.size)(0.0)
    
    val pos = idict.field2index(iid)
    block(pos) = 1
    
    block
    
  }
  
  def usersAsList:List[Int] = new EventSource(ctx).activeUsersAsList(req, this)
  
  def itemsAsList:List[Int] = new EventSource(ctx).activeItemsAsList(req, this)
  
  def userAsCol(uid:String):Int = udict.field2index(uid)
    
  /**
   * This method transforms a list of user identifiers into
   * a binary representation
   */
  def usersAsCols:Array[Double] = {
 
    val block = Array.fill[Double](udict.size)(0.0)
    
    val users = req.data(Names.REQ_USERS).split(":")
    users.foreach(uid => {
 
      val pos = udict.field2index(uid)
      block(pos) = 1
       
    })
    
    block

  }
  /**
   * This method transforms a list of item identifiers into
   * a binary representation
   */
  def itemsAsCols:Array[Double] = {
 
    val block = Array.fill[Double](idict.size)(0.0)
    
    val items = req.data(Names.REQ_ITEMS).split(":")
    items.foreach(iid => {
 
      val pos = idict.field2index(iid)
      block(pos) = 1
       
    })
    
    block

  }

  def eventAsCols:Array[Double] = {
    
    val event = req.data(Names.REQ_EVENT)
    
    val block = Array.fill[Double](edict.size)(0.0)
    
    val pos = edict.field2index(event)
    block(pos) = 1
    
    block
    
  }
  /**
   * This method retrieves the 'rated item block' from the 
   * computed user preferences or ratings
   */
  def ratedAsCols:Array[Double] = {
    new EventSource(ctx).ratedItemsAsCols(req, this)
  }
  
  def relatedAsCols:Array[Double] = {

    val block = Array.fill[Double](idict.size)(0.0)    
    val iid = req.data(Names.REQ_RELATED)
 
    val pos = idict.field2index(iid)
    block(pos) = 1
    
    block
    
  }

  def timeAsCols:Array[Double] = {
    
    val dayOfweek = req.data(Names.REQ_DAY_OF_WEEK).toInt
    
    val dblock = Array.fill[Double](1)(7)
    dblock(dayOfweek) = 1
 
    val hourOfday = req.data(Names.REQ_HOUR_OF_DAY).toInt
    
    val hblock = Array.fill[Double](1)(24)
    hblock(hourOfday) = 1

    dblock ++ hblock
    
  }
  
  /**
   * The context part of a feature vector contains the following blocks:
   * 
   * a) rated items block
   * 
   * b) day of week block
   * 
   * c) hour of day block
   * 
   * d) event block
   * 
   * e) item rated before block
   * 
   */
  def contextAsCols:Array[Double] = {

    val ratedCols = ratedAsCols
    
    /* The time dependent part of the context comprises the
     * subordinate blocks, day of week and time of day 
     */
    val timeCols = timeAsCols

    val eventCols = eventAsCols
    val relatedCols = relatedAsCols
    
    ratedAsCols ++ timeCols ++ eventCols ++ relatedCols
    
  }
  
}