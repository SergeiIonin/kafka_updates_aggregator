package test

type CacheTest struct {
	Mapping map[string]map[string]any
}

func NewCacheTest() *CacheTest {
	return &CacheTest{
		Mapping: make(map[string]map[string]any),
	}
}

func (cache *CacheTest) CreateNamespace(ns string) error {
	cache.Mapping[ns] = make(map[string]any)
	return nil
}
func (cache *CacheTest) DeleteNamespace(ns string) error {
	delete(cache.Mapping, ns)
	return nil
}
func (cache *CacheTest) Create(ns string, id string, key string, value any) error {
	cache.Mapping[ns][key] = value
	return nil
}
func (cache *CacheTest) Get(ns string, id string, key string) (any, error) {
	return cache.Mapping[ns][key], nil
}
func (cache *CacheTest) Update(ns string, key string, value any) error {
	cache.Mapping[ns][key] = value
	return nil
}
func (cache *CacheTest) Delete(ns string, key string) error {
	delete(cache.Mapping[ns], key)
	return nil
}
