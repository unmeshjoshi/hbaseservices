package com.gemfire

import com.financialservices.AccountPosition
import org.apache.geode.cache.{GemFireCache, Region}

class PositionCache(val
                    cacheProvider: GemfireCacheProvider) {

  val positionRegion: Region[String, AccountPosition] = cacheProvider.getCache().getRegion("Positions")

  def add(position: AccountPosition) = {
    positionRegion.put(position.key, position)
  }

  def get (id: String) = {
    positionRegion.get(id)
  }
}
