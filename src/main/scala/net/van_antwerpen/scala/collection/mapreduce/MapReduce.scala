package net.van_antwerpen.scala.collection.mapreduce

import scala.collection._
import Aggregator._

object MapReduce {

  class GenMapReducer[Elem](as: GenTraversableOnce[Elem]){
    
    def mapReduce[ResultColl] = new MapReducer[ResultColl]
    class MapReducer[ResultColl] {
       def apply[ResultElem](f: Elem => ResultElem)
                (implicit p: Aggregator[ResultColl,ResultElem])
                : ResultColl =
  	     (p.zero /: as)( (c,a) => p.insert(c, f(a)) )
    }

    def flatMapReduce[ResultColl] = new FlatMapReducer[ResultColl]
    class FlatMapReducer[ResultColl] {
       def apply[ResultElem](f: Elem => GenTraversableOnce[ResultElem])
                (implicit p: Aggregator[ResultColl,ResultElem])
                : ResultColl =
  	     (p.zero /: as)( (c,a) => (c /: f(a))( (cc,aa) =>  p.insert(cc,aa) ) )
    }

    def reduceTo[ResultColl](implicit p: Aggregator[ResultColl,Elem])
                : ResultColl =
  	     (p.zero /: as)( (c,a) => p.insert(c, a) )

  }

  implicit def mkMapReducable[Elem](as: GenTraversableOnce[Elem]) =
    new GenMapReducer[Elem](as)

}
