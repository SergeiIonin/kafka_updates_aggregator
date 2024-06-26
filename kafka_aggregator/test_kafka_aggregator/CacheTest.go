package test_kafka_aggregator

import "context"

type FieldsCacheTest struct {
	Mapping map[string]map[string]any
}

func NewFieldsCacheTest() *FieldsCacheTest {
	return &FieldsCacheTest{
		Mapping: make(map[string]map[string]any),
	}
}

func (cache *FieldsCacheTest) Add(id string, key string, value any) error {
	_, ok := cache.Mapping[id]
	if !ok {
		cache.Mapping[id] = make(map[string]any)
	}
	cache.Mapping[id][key] = value
	return nil
}

func (cache *FieldsCacheTest) Get(id string, key string, ctx context.Context) (any, error) {
	return cache.Mapping[id][key], nil
}

func (cache *FieldsCacheTest) Upsert(id string, key string, value any, ctx context.Context) error {
	cache.Mapping[id][key] = value
	return nil
}

func (cache *FieldsCacheTest) Delete(id string, key string) error {
	delete(cache.Mapping[id], key)
	return nil
}
