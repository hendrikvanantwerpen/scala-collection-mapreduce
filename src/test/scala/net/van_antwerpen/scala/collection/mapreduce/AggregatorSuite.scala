package net.van_antwerpen.scala.collection.mapreduce

import org.scalatest.FunSuite

import scala.collection.immutable._

import Aggregator._

class AggregatorSuite extends FunSuite {

  test("insert int") {
    expect(2) { 1 |<| 1 }
  }

  test("insert list of ints") {
    expect(4) { 1 |<| List(1,2) }
  }

  test("insert tuple of ints") {
    expect((2,2)) { (1,1) |<| (1,1) }
  }

  test("insert list of tuples of ints") {
    expect((4,4)) { (1,1) |<| List((1,1),(2,2)) }
  }

  test("insert element to list") {
    expect(List(1,2)) { List(1) |<| 2 }
  }

  test("insert list to list") {
    expect(List(1,2,3)) { List(1) |<| List(2,3) }
  }

  test("flatInsert list of lists to list") {
    expect(List(1,2,3)) { List(1) |<| List(List(2),List(3)) }
  }  

  test("flatInsert set of sets to list") {
    expect(List(1,2,3)) { List(1) |<| Set(Set(2),Set(3)) }
  }    
  
  test("insert set to list") {
    expect(List(1,2,3)) { List(1) |<| Set(2,3) }
  }  
  
  test("insert element to tuple of int and list") {
    expect((1,List(1,2))) { (0,List(1)) |<| (1,List(2)) }
  }
  
  test("insert deep element to tuple of int and list") {
    expect((1,List(1,2))) { (0,List(1)) |<| (1,2) }
  }

  test("insert shallow and deep element to tuple of int and list") {
    expect((List(1,2),List(1,2))) { (List(1),List(1)) |<| (2,List(2)) }
  }
  
  test("insert set to sorted set") {
    expect(Set(1,2)) { Set(1) |<| SortedSet(2) }
  }
  
  test("insert sorted set to set") {
    expect(SortedSet(1,2)) { SortedSet(2) |<| Set(1) }
  }

  test("insert element to simple map") {
    expect(Map(1 -> 1)) { Map.empty[Int,Int] |<| (1,1) }
  }  

  test("insert one element with multiple values into simple map") {
    expect(Map(1 -> 6)) { Map.empty[Int,Int] |<| (1,List(1,2,3)) }
  }    

  test("insert list of elements with multiple values into simple map") {
    expect(Map(1 -> 8)) { Map.empty[Int,Int] |<| List((1,List(1,2,3)),(1,List(1,1))) }
  }      
  
  test("insert map to simple map") {
    expect(Map(1 -> 1)) { Map.empty[Int,Int] |<| Map(1 -> 1) }
  }

  test("insert sorted map to simple map") {
    expect(Map(1 -> 1)) { Map.empty[Int,Int] |<| SortedMap(1 -> 1) }
  }  
  
  test("insert list of elements to simple map") {
    expect(Map(1 -> 3)) { Map.empty[Int,Int] |<| List(1 -> 1, 1 -> 2) }
  }  
  
  test("insert element to map") {
    expect(Map(1 -> Set(1))) { Map(1 -> Set.empty[Int]) |<| (1,Set(1)) }
  }  

  test("insert map to map") {
    expect(Map(1 -> Set(1))) { Map(1 -> Set.empty[Int]) |<| Map(1 -> Set(1)) }
  }    

  test("insert deep map to map") {
    expect(Map(1 -> Set(1))) { Map(1 -> Set.empty[Int]) |<| Map(1 -> 1) }
  }    
  
  test("insert deep element to map") {
    expect(Map(1 -> Set(1))) { Map(1 -> Set.empty[Int]) |<| (1,1) }
  }

  test("how deep does the rabbithole go?") {
    expect( (2, Map(1 -> Map(1 -> 3))) ) { (0,Map.empty[Int,Map[Int,Int]]) |<| (1,(1->(1->1))) |<| (1,Map(1 -> (1 -> 2))) }
  }  
  
  test("type tests for sets") {
    assert { (Set(1) |<| SortedSet(1)).isInstanceOf[Set[Int]] }
    assert { (SortedSet(1) |<| Set(1)).isInstanceOf[SortedSet[Int]] }
    assert { (TreeSet(1) |<| SortedSet(1)).isInstanceOf[TreeSet[Int]] }
    assert { (SortedSet(1) |<| TreeSet(1)).isInstanceOf[TreeSet[Int]] }
  }

  test("type tests for seqs") {
    assert { (List(1) |<| List(2)).isInstanceOf[List[Int]] }
    assert { (LinearSeq(1) |<| List(2)).isInstanceOf[LinearSeq[Int]] }
    assert { (Vector(1) |<| List(2)).isInstanceOf[Vector[Int]] }
    assert { (List(1) |<| Vector(2)).isInstanceOf[List[Int]] }
  }  

  test("type tests for maps") {
    assert { (Map(1 -> 1) |<| Map(1 -> 2)).isInstanceOf[Map[Int,Int]] }
    assert { (Map(1 -> 1) |<| TreeMap(1 -> 2)).isInstanceOf[Map[Int,Int]] }
    assert { (TreeMap(1 -> 1) |<| Map(1 -> 2)).isInstanceOf[TreeMap[Int,Int]] }
    assert { (TreeMap(1 -> 1) |<| HashMap(1 -> 2)).isInstanceOf[TreeMap[Int,Int]] }
    assert { (HashMap(1 -> 1) |<| SortedMap(1 -> 2)).isInstanceOf[HashMap[Int,Int]] }
    assert { (HashMap(1 -> 1) |<| TreeMap(1 -> 2)).isInstanceOf[HashMap[Int,Int]] }
  }    
  
}