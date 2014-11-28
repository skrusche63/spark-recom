package de.kp.spark.recom.model
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

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

import de.kp.spark.core.model._

object Algorithms {
  
  val ALS:String = "ALS"
  /* Association rule based recommendations */
  val ASR:String = "ASR"
  /* Context-aware recommendations */
  val CAR:String = "CAR"

  private def algorithms = List(ALS,ASR,CAR) 
  def isAlgorithm(algorithm:String):Boolean = algorithms.contains(algorithm)
  
}

object Messages extends BaseMessages {
  
  def MISSING_PARAMETERS(uid:String):String = 
    String.format("""[UID: %s] Parameters are missing.""", uid)

  def MODEL_BUILDING_STARTED(uid:String) = 
    String.format("""[UID: %s] Model building started.""", uid)
  
}

object ResponseStatus extends BaseStatus

object Serializer extends BaseSerializer

object Services {

  val ASSOCIATION:String = "association"
  val CONTEXT:String = "context"    
    
  val RATING:String = "rating"    
  val SERIES:String = "series"
    
  private val services = List(ASSOCIATION,CONTEXT,RATING,SERIES)
  def isService(service:String):Boolean = services.contains(service)
  
}

object Sources {

  val FILE:String    = "FILE"
  val ELASTIC:String = "ELASTIC" 
  val JDBC:String    = "JDBC"    
  val PIWIK:String   = "PIWIK"  
    
  private val sources = List(FILE,ELASTIC,JDBC,PIWIK)
  
  def isSource(source:String):Boolean = sources.contains(source)
  
}

