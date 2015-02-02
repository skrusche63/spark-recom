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

case class Preference(
  site:String,user:String,item:Int,score:Double
)
/**
 * This information element is used by the CAR algorithm to
 * specify the result of a prediction request 
 */
case class PreferenceWithContext(
  site:String,
  user:String,
  item:String,
  /*
   * The fields 'event', 'dayOfweek', 'hourOfday' and 'related' 
   * specify the context for a certain prediction request
   */
  event:String,
  dayOfweek:Int,
  hourOfday:Int,
  related:String,
  score:Double
)

case class Preferences(items:List[Preference])

case class ScoredColumn(col:Int,score:Double)
case class SimilarColumns(col:Int,items:List[ScoredColumn])

case class SimilarColumnsList(items:List[SimilarColumns])

case class ScoredField(name:String,score:Double)
case class ScoredFields(items:List[ScoredField])

case class SimilarFields(name:String,items:List[ScoredField])
case class SimilarFieldsList(items:List[SimilarFields])

case class TargetedPoint(features:List[Double],target:Double)

/**
 * A derived association rule that additionally specifies the matching weight
 * between the antecent field and the respective field in mined and original
 * association rules
 */
case class WeightedRule (
  antecedent:List[Int],consequent:List[Int],support:Int,confidence:Double,weight:Double)
/**
 * A set of weighted rules assigned to a certain user of a specific site
 */
case class UserRules(site:String,user:String,items:List[WeightedRule])

case class MultiUserRules(items:List[UserRules])

object Algorithms {
  
  val ALS:String = "ALS"
  /* Association rule based recommendations */
  val ASR:String = "ASR"
  /* Context-aware recommendations */
  val CAR:String = "CAR"

  private def algorithms = List(ALS,ASR,CAR) 
  def isAlgorithm(algorithm:String):Boolean = algorithms.contains(algorithm)
  
}

/****************************************************************************
 * 
 *              Data Analytics Pipeline
 * 
 ***************************************************************************/

case class StartBuild()

case class BuildFailed(params:Map[String,String])

case class BuildFinished(params:Map[String,String])

case class StartLearn()

case class LearnFailed(params:Map[String,String])

case class LearnFinished(params:Map[String,String])


object Messages extends BaseMessages {
  
  def MISSING_PARAMETERS(uid:String):String = 
    String.format("""[UID: %s] Parameters are missing.""", uid)

  def MODEL_BUILDING_STARTED(uid:String) = 
    String.format("""[UID: %s] Model building started.""", uid)
  
}

object ResponseStatus extends BaseStatus

object Serializer extends BaseSerializer {
  
  /*
   * Multi user rules specify the result of association analysis 
   * and are used to build product recommendations built on top 
   * of association rules
   */
  def deserializeMultiUserRules(rules:String):MultiUserRules = read[MultiUserRules](rules)  

  /*
   * Preferences are the result of the ALS & ASR based prediction
   * and recommendation functionality
   */
  def serializePreferences(preferences:Preferences):String = write(preferences)
  def deserializePreferences(preferences:String):Preferences = read[Preferences](preferences)
  /*
   * ScoredFields are the result of the CAR based recommendation
   * functionality
   */  
  def serializeScoredFields(scoredFields:ScoredFields):String = write(scoredFields) 
  def deserializeScoredFields(scoredFields:String):ScoredFields = read[ScoredFields](scoredFields)
  /*
   * Similars are the result of the CAR based recommendation
   * functionality
   */
  def serializeSimilarColumnsList(similars:SimilarColumnsList):String = write(similars) 
  def deserializeSimilarColumnsList(similars:String):SimilarColumnsList = read[SimilarColumnsList](similars)
  /*
   * TargetedPoint is the result of the CAR based prediction
   * functionality
   */
  def serializeTargetedPoint(targetedPoint:TargetedPoint):String = write(targetedPoint)
  def deserializeTargetedPoint(targetedPoint:String):TargetedPoint = read[TargetedPoint](targetedPoint)
  
}

object Services {

  val ASSOCIATION:String = "association"
  val CONTEXT:String = "context"    
    
  val RATING:String = "rating"    
  val SERIES:String = "series"
    
  private val services = List(ASSOCIATION,CONTEXT,RATING,SERIES)
  def isService(service:String):Boolean = services.contains(service)
  
}

object Sinks {

  val FILE:String    = "FILE"
  val PARQUET:String = "PARQUET"  
    
  private val sinks = List(FILE,PARQUET)
  
  def isSink(sink:String):Boolean = sinks.contains(sink)
  
}

object Sources {

  val FILE:String    = "FILE"
  val ELASTIC:String = "ELASTIC" 
  val JDBC:String    = "JDBC"    
  val PARQUET:String = "PARQUET"  
  val PIWIK:String   = "PIWIK"  
    
  private val sources = List(FILE,ELASTIC,JDBC,PARQUET,PIWIK)
  
  def isSource(source:String):Boolean = sources.contains(source)
  
}

object Topics {
  /*
   * 'event' & 'item' specify the data source type a certain
   * recommender request is based on; these topics are used
   * for almost all requests
   */
  val EVENT:String = "event" 
  val ITEM:String  = "item"

  val USER:String    = "user" 
    
  private val topics = List(EVENT,ITEM,USER)
  
  def isTopic(topic:String):Boolean = topics.contains(topic)
  
}
