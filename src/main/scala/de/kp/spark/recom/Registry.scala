package de.kp.spark.recom
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

import scala.xml._

import scala.io.Source
import scala.collection.mutable.{ArrayBuffer,HashMap}

import scala.util.control.Breaks._

object Registry {

  private val path = "services.xml"
  
  private val root:Elem = XML.load(getClass.getClassLoader.getResource(path))
  
  private val services = HashMap.empty[String,String]
  
  load()
  
  private def load() {

    for (service <- root \ "service") {
      
      val name  = (service \ "@name").toString
      val url = service.text
      
      services += name -> url
      
    }

  }

  def get(name:String):String = services.get(name).get
  
  def main(args:Array[String]) {
    
  }
  
}