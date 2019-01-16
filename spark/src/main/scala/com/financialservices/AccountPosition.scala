package com.financialservices

case class AccountPosition(acctKey: String, balance: String, date: String) {
  def key: String = acctKey + "_" + date
}
