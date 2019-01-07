package com.gemfire

import com.hbaseservices.Position
import org.apache.geode.cache.{GemFireCache, Region}

class PositionCache(val
                    cacheProvider: GemfireCacheProvider) {

  val positionRegion: Region[String, Position] = cacheProvider.getCache().getRegion("Positions")

  def add(position: Position) = {
    positionRegion.put(position.key, position)
  }

  def get (id: String) = {
    positionRegion.get(id)
  }
}
