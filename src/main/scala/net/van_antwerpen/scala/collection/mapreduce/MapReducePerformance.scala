package net.van_antwerpen.scala.collection.mapreduce

import java.net.URL
import scala.collection.immutable.SortedMap
import scalaz._
import Scalaz._
import Monoid._
import Aggregator._
import MapReduce._

object Timer {
  import java.util.concurrent.TimeUnit
  import java.util.concurrent.TimeUnit._
  
  def timed[A](msg: String)(f: => A, timeUnit: TimeUnit = MILLISECONDS): A = {
    print(msg+" ...")
    val start = System.nanoTime()
	val result: A = f
	val end = System.nanoTime()
	println(" in "+timeUnit.convert(end-start, NANOSECONDS)+"ms")
	result
  }
}

object MapReducePerformance extends App {
  //import GHEntities._
  
  /*
  val LIMIT = 1e6.toInt
  val GROUP = 1e5.toInt
  val PERIOD = 7 * 24 * 3600
  
  val resource = "/commits.txt"
  def readLines(res: String) = {
    import scalax.io.Resource
    Resource.fromURL(this.getClass.getResource(res)).lines()
  }
  
  val commits = timed("read commits") {
    readLines(resource)
    .drop( 0*LIMIT )
    .take( LIMIT )
    .flatMap( GHRelationsViz.parseStringToCommit )
    .toList
  }

  commits.grouped( GROUP ).drop( 0 ).take( 5 ).foreach(cs => {
    println
    def report(m: SortedMap[Int,Set[Commit]]) = println("have "+m.foldLeft(0)( _ + _._2.size )+" left after grouping")
    val resMR = timed("mapReduce chunk") { mrGroupCommits(PERIOD)(cs) }
    report(resMR)
    val resMFL = timed("monoid foldLeft chunk") { mflGroupCommits(PERIOD)(cs) }
    report(resMFL)
    val resHFL = timed("handmade foldLeft chunk") { hflGroupCommits(PERIOD)(cs) }
    report(resHFL)
  })
  
  def mrGroupCommits(w: Int)(cs: Traversable[Commit]) =
    cs.mapReduce[(Int,Commit),SortedMap[Int,Set[Commit]]](groupCommits(w))
  def groupCommits(w: Int)(c: Commit) = {
    val cc = c.copy(timestamp = c.timestamp - (c.timestamp % w))
    (cc.timestamp -> cc)
  }

  def mflGroupCommits(w: Int)(cs: Traversable[Commit]) = {
    cs.foldLeft(SortedMap.empty[Int,Set[Commit]])( (m,c) => {
      val cc = c.copy(timestamp = c.timestamp - (c.timestamp % w))
      m |+| SortedMap(cc.timestamp -> Set(cc))
    })
  }

  def hflGroupCommits(w: Int)(cs: Traversable[Commit]) = {
    cs.foldLeft(SortedMap.empty[Int,Set[Commit]])( (m,c) => {
      val cc = c.copy(timestamp = c.timestamp - (c.timestamp % w))
      m + m.get( cc.timestamp ).fold( v => cc.timestamp -> (v + cc), cc.timestamp -> Set(cc) )
    })
  }  
  */
 
}
