package org.wordfrequency


import java.io._
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object WordFrequency {

  def createBufferedReader(filepath:String) = new BufferedReader(new FileReader(new File(filepath)))

  def findData(bufferedReader:BufferedReader, delimeter:String = ",") = {
    val file = Stream.continually(bufferedReader.readLine()).takeWhile(_ != null)
    val data =
      for (line <- file) yield {
        line.split(delimeter).toSet
      }
    data
  }

  def createWriter(filepath:String) = {
    val writer = new BufferedWriter(new FileWriter(filepath))
    writer
  }

  def mapToString(m:Map[String,Int]) = {
    for ((k, v) <- m.toList.sortBy(_._1.toLowerCase)) yield {
      s""""$k": $v"""
    }
  }

  def process(queries:IndexedSeq[Set[String]], records:IndexedSeq[Set[String]], outputFilepath:String) = {
    val futureResults =
      for (query <- queries) yield Future{
        val results =
          for (record <- records) yield {
            query subsetOf record match {
              case true => record -- query
              case false => Set[String]()
            }
          }

        results.flatten.groupBy(identity).mapValues(_.size)
      }

    Future.sequence(futureResults).map { results =>
      val data =
        for (r <- results) yield {
          mapToString(m = r).mkString(", ")
        }
      write(data = data, outputFilepath = outputFilepath)
    }
  }

  def write(outputFilepath:String, data:IndexedSeq[String]) = {
    val writer = createWriter(filepath = outputFilepath)
    for(d <- data) {
      writer.write(s"{$d}")
      writer.newLine()
    }
    writer.close()
  }

  def main(args: Array[String]) = {

    args.length != 3 match {
      case true => println("usage: queriesFilepath recordsFilepath outputFilepath")
      case false =>
        val queriesBufferedReader = createBufferedReader(args(0))
        val recordsBufferedReader = createBufferedReader(args(1))
        val outputFilepath = args(2)
        val records = findData(bufferedReader = recordsBufferedReader).toIndexedSeq
        val queries = findData(bufferedReader = queriesBufferedReader).toIndexedSeq
        process(queries = queries, records = records, outputFilepath = outputFilepath)
        queriesBufferedReader.close()
        recordsBufferedReader.close()
    }
  }
}
