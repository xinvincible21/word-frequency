package org.wordfrequency

import java.io._
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object WordFrequency {

  def createRecordsBufferedReader(filepath:String) = new BufferedReader(new FileReader(new File(filepath)))
  def createQueriesBufferedReader(filepath:String) = new BufferedReader(new FileReader(new File(filepath)))

  def findRecords(recordsBufferedReader:BufferedReader) = {
    val recordsFile = Stream.continually(recordsBufferedReader.readLine()).takeWhile(_ != null)
    val records =
      for (line <- recordsFile) yield {
        line.split(",").toSet
      }
    records
  }

  def findQueries(queriesBufferedReader:BufferedReader) = {
    val queriesFile = Stream.continually(queriesBufferedReader.readLine()).takeWhile(_ != null)
    val queries =
      for (line <- queriesFile) yield {
        line.split(",").toSet
      }
    queries
  }

  def writeOutput(filepath:String) = {
    val writer = new BufferedWriter(new FileWriter(filepath))
    writer
  }

  def mapToString(r:Map[String,Int]) = {
    for ((k, v) <- r) yield {
      s""""$k": $v"""
    }
  }

  def process(queries:IndexedSeq[Set[String]], records:IndexedSeq[Set[String]], writer:BufferedWriter) = {
    val futureResults =
      for (query <- queries) yield Future{
        val results =
          for (record <- records) yield {
            query subsetOf record match {
              case true => record -- query
              case false => Set[String]()
            }
          }

        results.toList.flatten.groupBy(identity).mapValues(_.size)
      }

    val results = Await.result(Future.sequence(futureResults), 20 seconds)
    for (r <- results) {
      val o = mapToString(r = r).mkString(", ")
      writer.write(s"{$o}")
      writer.newLine()
    }

  }

  def main(args: Array[String]) = {

    if(args.length != 3) println("usage: queriesFilepath recordsFilepath outputFilepath")
    val queriesBufferedReader = createQueriesBufferedReader(args(0))
    val recordsBufferedReader = createRecordsBufferedReader(args(1))
    val writer = writeOutput(args(2))
    val records = findRecords(recordsBufferedReader = recordsBufferedReader).toIndexedSeq
    val queries = findQueries(queriesBufferedReader = queriesBufferedReader).toIndexedSeq
    process(queries = queries, records = records, writer = writer)
    queriesBufferedReader.close()
    recordsBufferedReader.close()
    writer.close()
  }
}