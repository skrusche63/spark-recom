package de.kp.spark.recom.handler
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

import de.kp.spark.recom.model.Serializer
import de.kp.spark.recom.format.CARFormatter

class CARHandler {

  /**
   * This method builds a feature vector from the request parameter that can
   * be sent to the Context-Aware Analysis engine. A feature vector is usually
   * used with 'predict' requests
   */
  def buildFeatureReq(req:ServiceRequest):String = {

    val service = "context"
    val task = "get:feature"
    
    if (req.data.contains(Names.REQ_FEATURES)) {
      /*
       * The required feature vector is already part of the user request;
       * in this case no additional transformation is performed
       */
      return Serializer.serializeRequest(new ServiceRequest(service,task,req.data))
      
    }    
    /*
     * The required feature vector must be created from the request parameters;
     * it is required that the 'user', a certain 'item' of interest and the
     * respective 'context' is provided by the request.
     *
     * This is the common request type, where a certain (user,item) pair and an
     * observed context is provided; in this case, the data are transformed into 
     * the basic feature vector,
     */
    val uid = req.data(Names.REQ_UID)
    if (req.data.contains(Names.REQ_USER) == false) {      
      val msg = String.format("""[UID %s] No 'user' parameter provided.""",uid)
      throw new Exception(msg)      
    }
    
    if (req.data.contains(Names.REQ_ITEM) == false) {      
      val msg = String.format("""[UID %s] No 'item' parameter provided.""",uid)
      throw new Exception(msg)      
    }

    if (req.data.contains(Names.REQ_CONTEXT) == false) {      
      val msg = String.format("""[UID %s] No 'context' parameter provided.""",uid)
      throw new Exception(msg)      
    }
    
    val features = new CARFormatter().format(req).mkString(",")
    
    /* Update request parameters */
    val filter = List(Names.REQ_USER,Names.REQ_ITEM)
    val data = req.data.filter(x => filter.contains(x._1) == false).map(x => {
      if (x._1 == Names.REQ_CONTEXT) (Names.REQ_FEATURES,features) else x
    })
      
    return Serializer.serializeRequest(new ServiceRequest(service,task,data))

  }

}