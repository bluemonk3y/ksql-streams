package com.landoop.kstreams.sql.transform

/**
  * Created by stefan on 19/03/2017.
  */
case class LocalPizza(name: String, ingredients: Seq[LocalIngredient], cals: Int)

case class LocalIngredient(fieldName: String, fieldSugar: Double, fat: Double)