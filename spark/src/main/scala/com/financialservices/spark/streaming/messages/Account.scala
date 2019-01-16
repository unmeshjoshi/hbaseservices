package com.financialservices.spark.streaming.messages


case class Account(val accountKey:String, val amount:String, val accountType:String, val date:String, val time:String)