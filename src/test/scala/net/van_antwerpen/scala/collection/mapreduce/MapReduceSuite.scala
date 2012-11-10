package net.van_antwerpen.scala.collection.mapreduce

import org.scalatest.FunSuite

import scala.collection.immutable.{SortedMap,SortedSet}

import MapReduce._
import Aggregator._

class MapReduceSuite extends FunSuite {

  val words = List("aap","noot","acht","fiets","aap","noot","aap")
  
  test("word count") {
    expect(Map("aap" -> 3, "noot" -> 2, "acht" -> 1, "fiets" -> 1)) {
      words.par.mapReduce[Map[String,Int]]( (_,1) )
    }    
  }

  test("word count in docs") {
    expect(Map("aap" -> 3, "noot" -> 2, "acht" -> 1, "fiets" -> 1)) {
      val docs = List("aap noot acht","fiets aap noot aap")
      def wordcount(doc: String) = doc.split(" ").toList.map( w => (w,1) )
      val wc = docs.mapReduce[Map[String,Int]]( wordcount )      
      wc
    }    
  }  
  
  test("word lengths") {
    expect(SortedMap(3 -> Set("aap"), 4 -> Set("noot","acht"), 5 -> Set("fiets"))) {
      words.mapReduce[Map[Int,Set[String]]]( w => (w.size,w) )
    }    
  }
  
  test("total length") {
    expect(26) {
      words.mapReduce[Int]( _.size ) 
    }
  }

  test("product of lengths with explicit aggregator") {
    expect(8640) {
      val ProductMonoid = new Aggregator[Int,Int] {
        override def zero = 1
        override def insert(i: Int, j: Int) = i * j
      }
      words.mapReduce( _.size )(ProductMonoid) 
    }
  }  
  
  test("very complex toSet") {
    expect(Set("aap","noot","acht","fiets")) {
      words.reduceTo[Set[String]]
    }
  }

  test("word counts grouped by length") {
    expect(SortedMap(3 -> Map("aap" -> 3), 4 -> Map("noot" -> 2, "acht" -> 1), 5 -> Map("fiets" -> 1))) {
      words.mapReduce[SortedMap[Int,Map[String,Int]]]( s => (s.size,(s,1)) )
    }
  }

  test("word counts grouped by first letter") {
    expect(SortedMap('a' -> Map("aap" -> 3, "acht" -> 1), 'n' -> Map("noot" -> 2), 'f' -> Map("fiets" -> 1))) {
      words.mapReduce[SortedMap[Char,Map[String,Int]]]( s => (s.head,(s,1)) )
    }
  }

  val letterCount = Map('e' -> 1, 's' -> 1, 'n' -> 2, 't' -> 4, 'f' -> 1, 'a' -> 7, 'i' -> 1, 'p' -> 3, 'c' -> 1, 'h' -> 1, 'o' -> 4)
  
  test("letter count 1") {
    expect(letterCount) {
      words.mapReduce[Map[Char,Int]]( s => s.toList.mapReduce[Map[Char,Int]]( c => (c,1) ) )
    }
  }

  test("letter count 2") {
    expect(letterCount) {
      words.map( s => s.toList.mapReduce[Map[Char,Int]]( c => (c,1) ) )
           .reduceTo[Map[Char,Int]]
    }
  }  
  
}