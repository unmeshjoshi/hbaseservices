package com.hbaseservices

case class AccountPosition(acctKey: String, balance: String, date: String) {
  def key: String = acctKey + "_" + date
}
