package org.wselwood.orbitdates

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime, ZoneOffset}

/**
  *
  */
object ObsUtils {

  def dateFunc(in : String): Long = {
    val year = in.substring(0, 4).toInt
    val month = in.substring(5, 7).toInt
    val day = in.substring(8, 10).toInt

    val part = in.substring(11).replaceAll(" ", "0").toInt
    val seconds = Math.round(((24*60*60) * 0.00001) * part)

    LocalDate.of(year, month, day).atStartOfDay()
      .plus(seconds, ChronoUnit.SECONDS)
      .toInstant(ZoneOffset.UTC)
      .getEpochSecond
  }

  def trimZeroFunc(in: String) : String = {
    var i = 0
    while(i < in.length && in(i) == '0' ) {
      i = i + 1
    }

    if (in.length <= i) {
      ""
    } else {
      in.substring(i)
    }
  }

  def unpackIdFunc(in : String) : String = {
    if (isAllDigits(in.substring(1))) {
      trimZeroFunc(unpackNumbered(in))
    } else if (in(2) >= '0' && in(2) <= '9') {
      numericId(in)
    } else {
      twoCharCode(in)
    }
  }

  def twoCharCode(in : String) : String = {
    val number = unpackNumbered(in.substring(3))
    number + " " + in(0) + "-" + in(1)
  }

  def numericId(in : String) : String = {
    val year = unpackNumbered(in.substring(0, 3))
    val number = trimZeroFunc(unpackNumbered(in.substring(4, 6)))
    if (number == "" || number == "00") {
      year + " " + in(3) + in(6)
    } else {
      year + " " + in(3) + in(6) + number
    }
  }

  def unpackNumbered(in : String) : String = {
    if (in(0) >= '0' && in(0) <= '9') {
      in
    } else {
      val numeric = in.substring(1)
      val expanded = if (in(0) >= 'a' && in(0) <= 'z') {
        in(0) - 'a' + 36
      } else {
        in(0) - 'A' + 10
      }

      expanded.toString + numeric
    }
  }

  def formatDateFunc(in : Long) : String = {
    LocalDateTime.ofEpochSecond(in, 0, ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
  }

  def dayOfWeekFunc(in : Long) : Int = {
    LocalDateTime.ofEpochSecond(in, 0, ZoneOffset.UTC).getDayOfWeek.getValue
  }

  def isAllDigits(x: String) : Boolean = x.forall(Character.isDigit)


}
