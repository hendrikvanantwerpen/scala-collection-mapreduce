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

    def reduceTo[ResultColl](implicit p: Aggregator[ResultColl,Elem])
                : ResultColl =
  	     (p.zero /: as)( (c,a) => p.insert(c, a) )

  }

  implicit def mkMapReducable[Elem](as: GenTraversableOnce[Elem]) =
    new GenMapReducer[Elem](as)

}
