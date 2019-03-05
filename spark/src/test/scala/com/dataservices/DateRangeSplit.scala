package com.dataservices

import java.time.LocalDate
import java.time.temporal.ChronoUnit

import com.sun.javaws.exceptions.InvalidArgumentException
import org.scalatest.FunSuite

import scala.collection.mutable.ListBuffer

class DateRangeSplit extends FunSuite {

  test("should split date range into multiple ranges") {
    val startDate = LocalDate.parse("2017-08-01")
    val endDate = LocalDate.parse("2018-08-01")

    val ranges = DateRange(startDate, endDate).splitInChunks(ChunkDuration(3, ChronoUnit.MONTHS))

    assert(startDate == ranges(0).startDate)
    assert(endDate == ranges(ranges.length - 1).endDate)
  }

  test("should give single range when date range is less than split") {
    val startDate = LocalDate.parse("2017-08-01")
    val endDate = LocalDate.parse("2017-09-01")

    val ranges = DateRange(startDate, endDate).splitInChunks(ChunkDuration(3, ChronoUnit.MONTHS))

    assert(ranges.size == 1)
    assert(startDate == ranges(0).startDate)
    assert(endDate == ranges(ranges.length - 1).endDate)
  }

 test("should split date range into multiple ranges if range is not exact split") {
    val startDate = LocalDate.parse("2017-08-01")
    val endDate = LocalDate.parse("2017-12-01")

    val ranges = DateRange(startDate, endDate).splitInChunks(ChunkDuration(3, ChronoUnit.MONTHS))
    println(ranges)
    assert(ranges.size == 2)
    assert(startDate == ranges(0).startDate)
    assert(endDate == ranges(1).endDate)
  }

  case class ChunkDuration(value:Int, units:ChronoUnit) {
    def addTo(date:LocalDate) = {
     units match  {
        case ChronoUnit.DAYS ⇒ date.plusDays(value)
        case ChronoUnit.MONTHS ⇒ date.plusMonths(value)
        case _ ⇒ throw new InvalidArgumentException(Array("Only days and months are supported"))
      }
    }
  }

  case class DateRange(startDate:LocalDate, endDate:LocalDate) {

    def splitInChunks(chunkDuration:ChunkDuration):List[DateRange] = {
      val chunks = new ListBuffer[DateRange]()
      var chunk = firstChunk(chunkDuration)
      while (chunk.endsBefore(endDate)) {
        chunks += chunk
        chunk = chunk.nextChunk(chunkDuration)
      }
      if (chunk.endsOnOrAfter(endDate)) {
        chunks += chunk.endOn(endDate)
      }
      chunks.toList
    }

    private def firstChunk(chunkDuration:ChunkDuration): DateRange = {
      DateRange(startDate, chunkDuration.addTo(startDate))
    }

    private def nextChunk(chunkDuration:ChunkDuration): DateRange = {
      DateRange(dayAfterEndDay(), chunkDuration.addTo(endDate))
    }

    private def endsBefore(otherDate:LocalDate) = {
      endDate.isBefore(otherDate)
    }

    private def endsOnOrAfter(otherDate:LocalDate) = {
      otherDate.isAfter(otherDate) || otherDate.isEqual(otherDate)
    }

    private def endOn(otherDate:LocalDate):DateRange = {
      DateRange(startDate, otherDate)
    }

    private def dayAfterEndDay() = {
      endDate.plusDays(1)
    }
  }
}
