package net.van_antwerpen.scala.collection.mapreduce

import scala.collection._
import Aggregators._

object MapReduce {

  class MapReducer[Elem](as: GenTraversableOnce[Elem]){
    
    def mapReduce[ResultElem, ResultColl]
                 (f: Elem => ResultElem)
                 (implicit p: Aggregator[ResultColl,ResultElem]) =
  	(p.nil /: as)( (c,a) => p.insert(c, f(a)) )
  	
    def flatMapReduce[ResultElem, ResultColl]
                   (f: Elem => GenTraversableOnce[ResultElem])
                   (implicit p: Aggregator[ResultColl,ResultElem]) =
  	  (p.nil /: as)( (c,a) => (c /: f(a))( (cc,aa) =>  p.insert(cc,aa) ) )

  }

  implicit def mkMapReducable[Elem](as: GenTraversableOnce[Elem]) =
    new MapReducer[Elem](as)

}
