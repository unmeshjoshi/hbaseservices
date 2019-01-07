package com.gemfire

import org.apache.geode.cache.GemFireCache

trait GemfireCacheProvider extends Serializable {
  def getCache():GemFireCache
}
