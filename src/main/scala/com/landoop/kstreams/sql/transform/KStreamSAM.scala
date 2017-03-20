/*
 * Copyright 2017 Landoop.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.landoop.kstreams.sql.transform

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{KeyValueMapper, Reducer, ValueMapper}

import scala.language.implicitConversions

object KStreamSAM {
  implicit def ValueMapperConverter[T, R](f: (T) => R): ValueMapper[T, R] = {
    new ValueMapper[T, R] {
      override def apply(value: T): R = f(value)
    }
  }

  implicit def ValueMapperConverter[T, R](pf: PartialFunction[T, R]): ValueMapper[T, R] = {
    new ValueMapper[T, R] {
      override def apply(value: T): R = pf(value)
    }
  }

  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = {
    new KeyValue(tuple._1, tuple._2)
  }


  implicit def Tuple2ToKeyValueMapper[K, V, KR, VR](fn: (K, V) => (KR, VR)): KeyValueMapper[K, V, KeyValue[KR, VR]] = {
    new KeyValueMapper[K, V, KeyValue[KR, VR]] {
      override def apply(key: K, value: V): KeyValue[KR, VR] = {
        Tuple2ToKeyValue(fn(key, value))
      }
    }
  }

  implicit def functionToReducer[V](f: ((V, V) => V)): Reducer[V] = new Reducer[V] {
    override def apply(l: V, r: V): V = f(l, r)
  }


  /* implicit def ValueMapperConverterFunction[T, R](f: Function1[T, R]): ValueMapper[T, R] = {
     new ValueMapper[T, R] {
       override def apply(value: T): R = f(value)
     }
   }*/


}
