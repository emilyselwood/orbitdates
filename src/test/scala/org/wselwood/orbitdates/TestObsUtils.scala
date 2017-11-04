package org.wselwood.orbitdates

import org.junit.Assert._
import org.junit.Test

/**
  *



  */
class TestObsUtils {

  @Test
  def packedIdTest(): Unit = {
    assertEquals("1995 XA", ObsUtils.unpackIdFunc("J95X00A"))
    assertEquals("1995 XL1", ObsUtils.unpackIdFunc("J95X01L"))
    assertEquals("1995 FB13", ObsUtils.unpackIdFunc("J95F13B"))
    assertEquals("1998 SQ108", ObsUtils.unpackIdFunc("J98SA8Q"))
    assertEquals("1998 SV127", ObsUtils.unpackIdFunc("J98SC7V"))
    assertEquals("1998 SS162", ObsUtils.unpackIdFunc("J98SG2S"))
    assertEquals("2099 AZ193", ObsUtils.unpackIdFunc("K99AJ3Z"))
    assertEquals("2008 AA360", ObsUtils.unpackIdFunc("K08Aa0A"))
    assertEquals("2007 TA418", ObsUtils.unpackIdFunc("K07Tf8A"))

    assertEquals("2040 P-L", ObsUtils.unpackIdFunc("PLS2040"))
    assertEquals("3138 T-1", ObsUtils.unpackIdFunc("T1S3138"))
    assertEquals("1010 T-2", ObsUtils.unpackIdFunc("T2S1010"))
    assertEquals("4101 T-3", ObsUtils.unpackIdFunc("T3S4101"))
  }

}
