package influxdb

import (
	"context"
	"github.com/atlassian/gostatsd"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

var testDatabase string = "test_database"

var testTimeout time.Duration = time.Second

func TestPreparePayload(t *testing.T) {
	t.Parallel()
	type testData struct {
		config *Config
		result []byte
	}
	metrics := metrics()

	expectedPoints := batchPoints()
	assert.Equal(t, 14, len(expectedPoints.Points()))

	cl, err := NewClient(&Config{
		Database:     &testDatabase,
		WriteTimeout: &testTimeout,
	})

	require.NoError(t, err)

	testTime := time.Unix(1234, 0)
	batchPoints, err := cl.preparePayload(metrics, testTime)

	require.NoError(t, err)

	assert.NotNil(t, batchPoints)
	assert.Equal(t, testDatabase, batchPoints.Database())
	assert.Equal(t, len(expectedPoints.Points()), len(batchPoints.Points()))

	for i, expected := range expectedPoints.Points() {
		point := batchPoints.Points()[i]

		assert.Equal(t, expected.Name(), point.Name())
		assert.Equal(t, testTime, point.Time())

		expectedFields, err := expected.Fields()
		require.NoError(t, err)

		pointFields, err := point.Fields()
		require.NoError(t, err)

		assert.NotNil(t, expectedFields["value"])
		assert.Equal(t, expectedFields["value"], pointFields["value"])

		for tk, tv := range expected.Tags() {
			assert.Equal(t, tv, point.Tags()[tk])
		}
	}
}

type NoopHandler struct{}

func (n *NoopHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
}

func TestSendMetricsAsync(t *testing.T) {
	t.Parallel()

	done := make(chan bool)
	timeout := time.NewTicker(time.Second)

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		data, err := ioutil.ReadAll(r.Body)
		if !assert.NoError(t, err) {
			return
		}

		assert.Contains(t, string(data), "t1_mean,tag2=v value=3.5")

		done <- true
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	address := "http://" + ts.Listener.Addr().String()

	client, err := NewClient(&Config{
		Address:      &address,
		Database:     &testDatabase,
		WriteTimeout: &testTimeout,
	})
	require.NoError(t, err)

	res := make(chan []error, 1)
	client.SendMetricsAsync(context.Background(), metrics(), func(errs []error) {
		res <- errs
	})

	select {
	case <-done:
	case <-timeout.C:
		assert.Fail(t, "timeout")
	}

	errs := <-res
	for _, err := range errs {
		assert.NoError(t, err)
	}
}

func batchPoints() client.BatchPoints {
	ts := time.Unix(123456, 0)

	ret, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database: testDatabase,
	})

	point, _ := client.NewPoint(
		"c1_count",
		map[string]string{
			"tag_1": "tag1",
		},
		map[string]interface{}{
			"value": 99,
		},
		ts,
	)

	ret.AddPoint(point)

	point, _ = client.NewPoint(
		"c1_rate",
		map[string]string{
			"tag_1": "tag1",
		},
		map[string]interface{}{
			"value": 99.1,
		},
		ts,
	)

	ret.AddPoint(point)

	point, _ = client.NewPoint(
		"t1_lower",
		map[string]string{
			"tag2": "v",
		},
		map[string]interface{}{
			"value": 5.1,
		},
		ts,
	)

	ret.AddPoint(point)

	point, _ = client.NewPoint(
		"t1_upper",
		map[string]string{
			"tag2": "v",
		},
		map[string]interface{}{
			"value": 6.1,
		},
		ts,
	)

	ret.AddPoint(point)

	point, _ = client.NewPoint(
		"t1_count",
		map[string]string{
			"tag2": "v",
		},
		map[string]interface{}{
			"value": 1,
		},
		ts,
	)

	ret.AddPoint(point)

	point, _ = client.NewPoint(
		"t1_count_ps",
		map[string]string{
			"tag2": "v",
		},
		map[string]interface{}{
			"value": 2.1,
		},
		ts,
	)

	ret.AddPoint(point)

	point, _ = client.NewPoint(
		"t1_mean",
		map[string]string{
			"tag2": "v",
		},
		map[string]interface{}{
			"value": 3.5,
		},
		ts,
	)

	ret.AddPoint(point)

	point, _ = client.NewPoint(
		"t1_median",
		map[string]string{
			"tag2": "v",
		},
		map[string]interface{}{
			"value": 4.5,
		},
		ts,
	)

	ret.AddPoint(point)

	point, _ = client.NewPoint(
		"t1_std",
		map[string]string{
			"tag2": "v",
		},
		map[string]interface{}{
			"value": 7.1,
		},
		ts,
	)

	ret.AddPoint(point)

	point, _ = client.NewPoint(
		"t1_sum",
		map[string]string{
			"tag2": "v",
		},
		map[string]interface{}{
			"value": 8.1,
		},
		ts,
	)

	ret.AddPoint(point)

	point, _ = client.NewPoint(
		"t1_sum_squares",
		map[string]string{
			"tag2": "v",
		},
		map[string]interface{}{
			"value": 9.1,
		},
		ts,
	)

	ret.AddPoint(point)

	point, _ = client.NewPoint(
		"t1_count_90",
		map[string]string{
			"tag2": "v",
		},
		map[string]interface{}{
			"value": 12.1,
		},
		ts,
	)

	ret.AddPoint(point)

	point, _ = client.NewPoint(
		"g1",
		map[string]string{
			"tag_1": "tag3",
		},
		map[string]interface{}{
			"value": 3.1,
		},
		ts,
	)

	ret.AddPoint(point)

	point, _ = client.NewPoint(
		"users",
		map[string]string{
			"tag_1": "tag4",
		},
		map[string]interface{}{
			"value": 4,
		},
		ts,
	)

	ret.AddPoint(point)

	return ret
}

func metrics() *gostatsd.MetricMap {
	timestamp := gostatsd.Nanotime(time.Unix(123456, 0).UnixNano())

	return &gostatsd.MetricMap{
		MetricStats: gostatsd.MetricStats{
			NumStats:       4,
			ProcessingTime: 10 * time.Millisecond,
		},
		FlushInterval: 1100 * time.Millisecond,
		Counters: gostatsd.Counters{
			"c1": map[string]gostatsd.Counter{
				"tag1": {PerSecond: 99.1, Value: 99, Timestamp: timestamp, Hostname: "h1", Tags: gostatsd.Tags{"tag1"}},
			},
		},
		Timers: gostatsd.Timers{
			"t1": map[string]gostatsd.Timer{
				"tag2:v": {
					Count:      1,
					PerSecond:  2.1,
					Mean:       3.5,
					Median:     4.5,
					Min:        5.1,
					Max:        6.1,
					StdDev:     7.1,
					Sum:        8.1,
					SumSquares: 9.1,
					Values:     []float64{10, 11},
					Percentiles: gostatsd.Percentiles{
						gostatsd.Percentile{Float: 12.1, Str: "count_90"},
					},
					Timestamp: timestamp,
					Hostname:  "h2",
					Tags:      gostatsd.Tags{"tag2:v"},
				},
			},
		},
		Gauges: gostatsd.Gauges{
			"g1": map[string]gostatsd.Gauge{
				"tag3": {Value: 3.1, Timestamp: timestamp, Hostname: "h3", Tags: gostatsd.Tags{"tag3"}},
			},
		},
		Sets: gostatsd.Sets{
			"users": map[string]gostatsd.Set{
				"tag4": {
					Values: map[string]struct{}{
						"joe":  {},
						"bob":  {},
						"john": {},
						"adam": {},
					},
					Timestamp: timestamp,
					Hostname:  "h4",
					Tags:      gostatsd.Tags{"tag4"},
				},
			},
		},
	}
}
