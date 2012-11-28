/* Copyright 2012 Hendrik van Antwerpen
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
