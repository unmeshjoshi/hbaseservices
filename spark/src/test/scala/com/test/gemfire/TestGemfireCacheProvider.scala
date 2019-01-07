package com.test.gemfire

import com.gemfire.GemfireCacheProvider
import org.apache.geode.cache.GemFireCache
import org.apache.geode.cache.client.{ClientCacheFactory, ClientRegionShortcut}
import org.apache.geode.pdx.ReflectionBasedAutoSerializer

class TestGemfireCacheProvider extends GemfireCacheProvider{
  lazy val clientCache: GemFireCache = createClientCache()

  override def getCache(): GemFireCache = clientCache

  private def createClientCache():GemFireCache = {
    val factory = new ClientCacheFactory()
    val clientCache = factory.addPoolLocator("127.0.0.1", 9009)
      .setPdxSerializer(new ReflectionBasedAutoSerializer("com.hbaseservices.*"))
      .setPoolMinConnections(50)
      //      .setPoolMaxConnections(-1) //unlimited
      //      .setPoolPRSingleHopEnabled(true)

      .create()
    clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("Positions")
    clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("FxRates")
    clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("MarketPrices")
    clientCache
  }
}
