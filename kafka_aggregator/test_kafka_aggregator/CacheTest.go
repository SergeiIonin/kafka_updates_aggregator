package test_kafka_aggregator

type CacheTest struct {
	Mapping map[string]map[string]any
}

func NewCacheTest() *CacheTest {
	return &CacheTest{
		Mapping: make(map[string]map[string]any),
	}
}

func (cache *CacheTest) Add(id string, key string, value any) error {
	_, ok := cache.Mapping[id]
	if !ok {
		cache.Mapping[id] = make(map[string]any)
	}
	cache.Mapping[id][key] = value
	return nil
}

func (cache *CacheTest) Get(id string, key string) (any, error) {
	return cache.Mapping[id][key], nil
}

func (cache *CacheTest) Update(id string, key string, value any) error {
	cache.Mapping[id][key] = value
	return nil
}

func (cache *CacheTest) Delete(id string, key string) error {
	delete(cache.Mapping[id], key)
	return nil
}
