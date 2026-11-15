package providers

// CacheMiddleware injects OpenAI prompt caching params (prompt_cache_key,
// prompt_cache_retention) for native OpenAI endpoints, and for proxies whose
// provider config opts in (cfg.SendPromptCacheKey). Other proxies and
// non-OpenAI providers get no change, because some reject unknown fields.
func CacheMiddleware(body map[string]any, cfg MiddlewareConfig) map[string]any {
	cacheKey, hasKey := cfg.Options[OptPromptCacheKey]
	retention, hasRetention := cfg.Options[OptPromptCacheRetention]

	if !hasKey && !hasRetention {
		return body // no cache options requested
	}

	if !isOpenAINativeEndpoint(cfg.APIBase) && !cfg.SendPromptCacheKey {
		return body
	}

	if hasKey {
		body["prompt_cache_key"] = cacheKey
	}
	if hasRetention {
		body["prompt_cache_retention"] = retention
	}

	return body
}
