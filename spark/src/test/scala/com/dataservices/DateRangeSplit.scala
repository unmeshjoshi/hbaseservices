package com.dataservices

import java.time.LocalDate

import org.scalatest.FunSuite

import scala.collection.mutable.ListBuffer

class DateRangeSplit extends FunSuite {

  test("should split date range into multiple ranges") {
    val startDate = LocalDate.parse("2017-08-01")
    val endDate = LocalDate.parse("2018-08-01")

    val ranges = DateRange(startDate, endDate).splitInMonths(3)

    assert(startDate == ranges(0).startDate)
    assert(endDate == ranges(ranges.length - 1).endDate)
  }

  test("should give single range when date range is less than split") {
    val startDate = LocalDate.parse("2017-08-01")
    val endDate = LocalDate.parse("2017-09-01")

    val ranges = DateRange(startDate, endDate).splitInMonths(3)

    assert(ranges.size == 1)
    assert(startDate == ranges(0).startDate)
    assert(endDate == ranges(ranges.length - 1).endDate)
  }

 test("should split date range into multiple ranges if range is not exact split") {
    val startDate = LocalDate.parse("2017-08-01")
    val endDate = LocalDate.parse("2017-12-01")

    val ranges = DateRange(startDate, endDate).splitInMonths(3)

    assert(ranges.size == 2)
    assert(startDate == ranges(0).startDate)
    assert(endDate == ranges(1).endDate)
  }


  case class DateRange(startDate:LocalDate, endDate:LocalDate) {

    def splitInMonths(noOfMonths:Int) = {
      val ranges = new ListBuffer[DateRange]()
      var rangeSplit = monthsAfterStartDate(noOfMonths)
      while (rangeSplit.endsBefore(endDate)) {
        ranges += rangeSplit
        rangeSplit = rangeSplit.monthsAfterEndDate(noOfMonths)
      }
      if (rangeSplit.endsOnOrAfter(endDate)) {
        ranges += rangeSplit.endOn(endDate)
      }
      ranges.toList
    }

    private def monthsAfterStartDate(months:Int): DateRange = {
      DateRange(startDate, startDate.plusMonths(months))
    }

    private def monthsAfterEndDate(months:Int): DateRange = {
      DateRange(dayAfterEndDay(), endDate.plusMonths(months))
    }

    private def endsBefore(endDate:LocalDate) = {
      endDate.isBefore(endDate)
    }

    private def endsOnOrAfter(endDate:LocalDate) = {
      endDate.isAfter(endDate) || endDate.isEqual(endDate)
    }

    private def endOn(endDate:LocalDate):DateRange = {
      DateRange(startDate, endDate)
    }

    private def dayAfterEndDay() = {
      endDate.plusDays(1)
    }
  }
}
