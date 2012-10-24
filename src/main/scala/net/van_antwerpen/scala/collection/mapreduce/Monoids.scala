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

  implicit def GenSeqSemigroup[Elem, Repr[X] <: GenSeq[X]]
                              : Semigroup[Repr[Elem]] =
     semigroup( (t1,t2) =>
       (t1 ++ t2)
       .asInstanceOf[Repr[Elem]]
     )

  implicit def GenSetSemigroup[Elem, Repr[X] <: GenSet[X]]
                              : Semigroup[Repr[Elem]] =
     semigroup( (t1,t2) =>
       (t1 ++ t2)
       .asInstanceOf[Repr[Elem]]
     )

  implicit def GenMapSemigroup[Key, Value, Repr[K,V] <: GenMap[K,V]]
                              (implicit vm: Monoid[Value])
                              : Semigroup[Repr[Key,Value]] =
     semigroup( (m1,m2) => 
       (m1 /: m2)( (m:Repr[Key,Value], e2:(Key,Value)) =>
         (m + (e2._1 -> m.get( e2._1 )
                         .map( v1 => vm.append(v1,e2._2) )
                         .getOrElse( vm.append(vm.zero,e2._2) ) ) )
         .asInstanceOf[Repr[Key,Value]] )
     )

}
