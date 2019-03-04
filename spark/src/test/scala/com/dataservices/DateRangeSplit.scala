package com.dataservices

import java.time.LocalDate

import org.scalatest.FunSuite

import scala.collection.mutable.ListBuffer

class DateRangeSplit extends FunSuite {

  test("should split date range into multiple ranges") {
    val startDate = LocalDate.parse("2017-08-01")
    val endDate = LocalDate.parse("2018-08-01")

    val ranges = splitRange(DateRange(startDate, endDate), 3)

    println(ranges)

    assert(startDate == ranges(0).startDate)
    assert(endDate == ranges(ranges.length - 1).endDate)
  }

  test("should give single range when date range is less than split") {
    val startDate = LocalDate.parse("2017-08-01")
    val endDate = LocalDate.parse("2017-09-01")

    val ranges = splitRange(DateRange(startDate, endDate), 3)

    println(ranges)
    assert(ranges.size == 1)
    assert(startDate == ranges(0).startDate)
    assert(endDate == ranges(ranges.length - 1).endDate)
  }

 test("should split date range into multiple ranges if range is not exact split") {
    val startDate = LocalDate.parse("2017-08-01")
    val endDate = LocalDate.parse("2017-12-01")



    val ranges = splitRange(DateRange(startDate, endDate), 3)

    println(ranges)
    assert(ranges.size == 2)
    assert(startDate == ranges(0).startDate)
    assert(endDate == ranges(1).endDate)
  }


  case class DateRange(startDate:LocalDate, endDate:LocalDate)
  def splitRange(dateRange:DateRange, splitMonths:Int) = {
    val ranges = new ListBuffer[DateRange]()

    var newStartDate: LocalDate = dateRange.startDate
    var newEndDate: LocalDate = dateRange.startDate.plusMonths(3)
    while (newEndDate.isBefore(dateRange.endDate) ) {
      ranges += DateRange(newStartDate, newEndDate)
      newStartDate = nextDay(newEndDate)
      newEndDate = newEndDate.plusMonths(3)
    }

    if (newEndDate.isAfter(dateRange.endDate) || newEndDate.isEqual(dateRange.endDate)) {
      ranges += DateRange(newStartDate, dateRange.endDate)
    }
    ranges
  }

  private def nextDay(newEndDate: LocalDate) = {
    newEndDate.plusDays(1)
  }
}
