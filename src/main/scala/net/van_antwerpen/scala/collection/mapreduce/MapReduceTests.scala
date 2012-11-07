package net.van_antwerpen.scala.collection.mapreduce

import scala.collection.immutable.{Set,TreeMap,SortedMap,SortedSet,TreeSet}

import Aggregator._
import CollectionAggregators._
import ValueAggregators._
import MapReduce._

import scala.math.Ordering

object MapReduceTests extends App {
  
  println( List(1,2) |<| 3 )
  println( List(1,2) |<<| List(3) )
  println( List(1,2) |<<| Set(3) )
  println( Set(1,2) |<| 3 )
  println( Set(1,2) |<<| Set(3) )
  println( Set(1,2) |<<| List(3) )
  println( Map(1 -> 2, 2 -> 2) |<| (1 -> 3) )
  println( Map(1 -> 2, 2 -> 2) |<<| Map(1 -> 3) )
  println( Map(1 -> 2, 2 -> 2) |<<| List((1 -> 3)) )

  println( Set(1,2) |<<| Some(Set(3)) )
  println( "1" |<<| Some("1") )
  //println( 1 |<<| None:Option[Int] )
  println( 0 |<<| Some(1) )
  
  println( SortedMap.empty[Int,Set[String]] |<| SortedMap(1 -> Set("aap")) )
  println( SortedMap.empty[Int,Set[String]] |<<| Map(1 -> Set("aap")) )
  println( Set.empty[Int] |<| Set(1) )
  println( Map.empty[String,Int] |<| ("aap",1) |<| ("noot",1) |<| ("aap",1) )
  println( Map.empty[String,Int] |<<| List( ("aap",1),("noot",1),("aap",1) ) )

  val words = List("aap","anders","bijna","boer","noot","mies","meisje","zaag","zonder","fiets","zonder","aap","mies","aap")
  
  def wordCount(s: String) = (s,1)
  val wordMap = words.mapReduce[Map[String,Int]](wordCount)
  println(wordMap)
  val parWordMap = words.par.mapReduce[Map[String,Int]](wordCount)
  println(parWordMap)

  def wordPlusLength(s: String) = (s.size,s)
  val wordPlusLengths = words.mapReduce[SortedMap[Int,String]](wordPlusLength)
  println(wordPlusLengths)

  def wordLength(s: String) = s.size
  val totalLength = words.mapReduce[Int](wordLength)
  println(totalLength)
  
  def countAndConcat(s: String) = (1,s)
  //implicit val agg = Tuple2Aggregator[Int,String,Int,String]
  val countedAndTogether = words.mapReduce[(Int,String)](wordInstance)
  println(countedAndTogether)

  def wordInstance(s: String) = (1,s)
  val totalWords = words.mapReduce[(Int,Set[String])](wordInstance)
  println(totalWords)

  def word(s: String) = s
  val wordSet = words.mapReduce[Set[String]](word)
  println(wordSet)
  val wordSortedSet = words.mapReduce[SortedSet[String]](word)
  println(wordSortedSet)
  val wordList = words.mapReduce[List[String]](word)
  println(wordList)

  def nestedWordCount(s: String) = (s.head.toString,(s,1))
  val nestedWordMap = words.mapReduce[SortedMap[String,Map[String,Int]]](nestedWordCount)
  println(nestedWordMap)
  
  def nestedWordLength(s: String) = (s.size,(s,1))
  val nestedWordLengths = words.mapReduce[SortedMap[Int,Map[String,Int]]](nestedWordLength)
  println(nestedWordLengths)

  def goCrazy(s: String) = (s.size,(s,("count",1)))
  val crazy = words.mapReduce[SortedMap[Int,Map[String,Map[String,Int]]]](goCrazy)
  println(crazy)
  
}
