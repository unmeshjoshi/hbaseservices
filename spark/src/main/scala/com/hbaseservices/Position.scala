package com.hbaseservices

case class Position(acctKey: String, egKey: String, marketUnitPriceAmount: String, marketUnitPriceDate: String, valueAsOfDate:String = "19-Aug-14", assetClassCd:String = "MONEYMAREKTMF") {
  def key: String = acctKey + "_" + valueAsOfDate
}
