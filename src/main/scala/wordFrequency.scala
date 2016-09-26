package org.wordfrequency

import java.io._
import scala.collection.parallel.ParMap
import scala.collection.parallel.ParSeq

object WordFrequency {

  def createRecordsBufferedReader(filepath:String) = new BufferedReader(new FileReader(new File(filepath)))
  def createQueriesBufferedReader(filepath:String) = new BufferedReader(new FileReader(new File(filepath)))

  def findRecords(recordsBufferedReader:BufferedReader) = {
    val recordsFile = Stream.continually(recordsBufferedReader.readLine()).takeWhile(_ != null)
    val records =
      for (line <- recordsFile.par) yield {
        line.split(",").toSet
      }
    records
  }

  def findQueries(queriesBufferedReader:BufferedReader) = {
    val queriesFile = Stream.continually(queriesBufferedReader.readLine()).takeWhile(_ != null)
    val queries =
      for (line <- queriesFile.par) yield {
        line.split(",").toSet
      }
    queries
  }

  def writeOutput(filepath:String) = {
    val writer = new BufferedWriter(new FileWriter(filepath))
    writer
  }

  def mapToString(r:ParMap[String,Int]) = {
    for ((k, v) <- r.par) yield {
      s""""$k": $v"""
    }
  }

  def process(queries:ParSeq[Set[String]], records:ParSeq[Set[String]], writer:BufferedWriter) = {
    val parResults =
      for (query <- queries) yield {
        val r =
          for (record <- records.par) yield {
            query subsetOf record match {
              case true => record -- query
              case false => Set[String]()
            }
          }
        r.flatten.groupBy(identity).mapValues(_.size)
      }

      for (r <- parResults.par) {
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
    val records = findRecords(recordsBufferedReader = recordsBufferedReader)
    val queries = findQueries(queriesBufferedReader = queriesBufferedReader)
    process(queries = queries, records = records, writer = writer)
    queriesBufferedReader.close()
    recordsBufferedReader.close()
    writer.close()
  }
}