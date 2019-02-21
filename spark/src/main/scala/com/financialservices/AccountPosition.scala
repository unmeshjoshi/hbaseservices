package com.financialservices

case class AccountPosition(num:Int, acctKey: String, balance: String, date: String, units:String) {
  def key: String = acctKey + "_" + date
}
