package net.van_antwerpen.scala.collection.mapreduce

import scala.collection._
import scala.collection.generic.CanBuildFrom
import scalaz._
import Scalaz._

object Monoid {

  implicit def OptionNothingZero: Zero[Option[Nothing]] =
    zero(None)

  implicit def OptionNothingSemigroup: Semigroup[Option[Nothing]] =
    semigroup( (_,_) => None )

  implicit def GenTraversableOnceZero[Repr <: GenTraversableOnce[_]]
                                     (implicit bf: CanBuildFrom[Nothing,Nothing,Repr])
                                     : Zero[Repr] =
    zero(bf.apply.result)

  implicit def GenTraversableOnceSemigroup[Elem, Repr[X] <: GenTraversableOnce[X]]
                                          (implicit bf: CanBuildFrom[Repr[Elem],Elem,Repr[Elem]])
                                          : Semigroup[Repr[Elem]] =
     semigroup( (t1,t2) => {
       val b = bf(t1)
       b ++= t1.seq
       b ++= t2.seq
       b.result
     } )

  implicit def GenMapSemigroup[Key, Value, Repr[K,V] <: GenMap[K,V]]
                              (implicit vm: Monoid[Value],
                                        bf: CanBuildFrom[Repr[Key,Value],(Key,Value),Repr[Key,Value]])
                              : Semigroup[Repr[Key,Value]] =
     semigroup( (m1,m2) => {
       val b = bf(m1)
       b ++= m1.seq
       b ++= m2.map( e2 => e2._1 -> m1.get( e2._1 )
                                      .map( v1 => vm.append(v1,e2._2) )
                                      .getOrElse( vm.append(vm.zero,e2._2) ) )
               .seq
       b.result
     } )

}
