package net.van_antwerpen.scala.collection.mapreduce

import scala.collection._
import Aggregators._

object MapReduce {

  class GenMapReducer[Elem](as: GenTraversableOnce[Elem]){
    
    class MapReducer[ResultColl] {
       def apply[ResultElem](f: Elem => ResultElem)
                (implicit p: Aggregator[ResultColl,ResultElem])
                : ResultColl =
  	     (p.zero /: as)( (c,a) => p.insert(c, f(a)) )
    }

    class FlatMapReducer[ResultColl] {
       def apply[ResultElem](f: Elem => GenTraversableOnce[ResultElem])
                (implicit p: Aggregator[ResultColl,ResultElem])
                : ResultColl =
  	     (p.zero /: as)( (c,a) => (c /: f(a))( (cc,aa) =>  p.insert(cc,aa) ) )
    }

    def mapReduce[ResultColl] = new MapReducer[ResultColl]
  	
    def flatMapReduce[ResultColl] = new FlatMapReducer[ResultColl]

  }

  implicit def mkMapReducable[Elem](as: GenTraversableOnce[Elem]) =
    new GenMapReducer[Elem](as)

}
