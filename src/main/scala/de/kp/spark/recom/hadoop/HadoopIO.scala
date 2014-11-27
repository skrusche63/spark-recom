package de.kp.spark.recom.hadoop
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
import java.io.{ObjectInputStream,ObjectOutputStream} 

import org.apache.hadoop.conf.{Configuration => HadoopConf}

import org.apache.hadoop.fs.{FileSystem,Path}
import org.apache.hadoop.io.{SequenceFile,Text}

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import de.kp.spark.recom.util.Dict

object HadoopIO {

  def writeRecom(userspec:Dict,itemspec:Dict,matrix:MatrixFactorizationModel,path:String) {
    
    /* Write users to Hadoop */
    val users = userspec.getTerms.mkString(",")
    writeToHadoop(users, path + "/users")

    /* Write items to Hadoop */
    val items = itemspec.getTerms.mkString(",")
    writeToHadoop(items, path + "/items")

    try {

      /* Write MatrixFactorizationModel */      		
      val conf = new HadoopConf()
	  val fs = FileSystem.get(conf)

      val oos = new ObjectOutputStream(fs.create(new Path(path + "/model/matrix.obj")))   
      oos.writeObject(matrix)
    
      oos.close

	} catch {
	  case e:Exception => throw new Exception(e.getMessage())

	}

  }
  
  def readRecom(path:String):(Dict,Dict,MatrixFactorizationModel) = {
    
    /* Read users from Hadoop */
    val users = readFromHadoop(path + "/users").split(",").toSeq
    val userspec = new Dict().build(users)

    /* Read items from Hadoop */
    val items = readFromHadoop(path + "/items").split(",").toSeq
    val itemspec = new Dict().build(items)

    try {
      
      /* Read Matrix FactorizationModel */
      val conf = new HadoopConf()
	  val fs = FileSystem.get(conf)

      val ois = new ObjectInputStream(fs.open(new Path(path + "/model/matrix.obj")))
      val matrix = ois.readObject().asInstanceOf[MatrixFactorizationModel]
      
      ois.close()
      
      (userspec,itemspec,matrix)
      
    } catch {
	  case e:Exception => throw new Exception(e.getMessage())
      
    }
    
  }

  private def writeToHadoop(ser:String,file:String) {

    try {
		
      val conf = new HadoopConf()
	  val fs = FileSystem.get(conf)

      val path = new Path(file)
	  val writer = new SequenceFile.Writer(fs, conf, path, classOf[Text], classOf[Text])

	  val k = new Text()
	  val v = new Text(ser)

	  writer.append(k,v)
	  writer.close()

	} catch {
	  case e:Exception => throw new Exception(e.getMessage())

	}
 
  }
  
  private def readFromHadoop(file:String):String = {
    
    try {
		
      val conf = new HadoopConf()
	  val fs = FileSystem.get(conf)

      val path = new Path(file)
      
      val reader = new SequenceFile.Reader(fs,path,conf)

      val k = new Text()
      val v = new Text()

      reader.next(k, v)
      reader.close()
      
      v.toString

	} catch {
	  case e:Exception => throw new Exception(e.getMessage())

	}

  }
  
}