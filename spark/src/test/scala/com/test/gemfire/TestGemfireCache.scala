package com.test.gemfire

import java.util.Properties

import com.gemfire.GemfireCacheProvider
import org.apache.geode.cache.{CacheFactory, GemFireCache, RegionShortcut}
import org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT
import org.apache.geode.pdx.ReflectionBasedAutoSerializer

class TestGemfireCache  extends GemfireCacheProvider {

  def create = {
    val props = new Properties()
    props.setProperty(MCAST_PORT, "0")

    val factory = new CacheFactory(props)
    val cache = factory
      .setPdxSerializer(new ReflectionBasedAutoSerializer("com.hbaseservices.*"))
      .create()

    cache.createRegionFactory(RegionShortcut.PARTITION).create("Positions")
    cache.createRegionFactory(RegionShortcut.REPLICATE).create("FxRates")
    cache.createRegionFactory(RegionShortcut.REPLICATE).create("MarketPrices")
    cache
  }

  override def getCache(): GemFireCache = create
}
