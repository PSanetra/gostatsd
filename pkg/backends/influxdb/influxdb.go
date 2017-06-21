package influxdb

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/atlassian/gostatsd"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/spf13/viper"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// BackendName is the name of this backend.
const (
	// BackendName is the name of this backend.
	BackendName = "influxdb"
	// DefaultAddress is the default address of InfluxDb server.
	DefaultAddress = "http://localhost:8086"
	// DefaultWriteTimeout is the default socket write timeout.
	DefaultWriteTimeout = 30 * time.Second
)

var (
	regWhitespace  = regexp.MustCompile(`\s+`)
	regNonAlphaNum = regexp.MustCompile(`[^a-zA-Z\d_.-]`)
)

// Config holds configuration for the InfluxDb backend.
type Config struct {
	Address      *string
	Database     *string
	Username     *string
	Password     *string
	WriteTimeout *time.Duration
}

// Client is an object that is used to send messages to a InfluxDb server's HTTP interface.
type Client struct {
	database string
	client   client.Client
}

// SendMetricsAsync flushes the metrics to the InfluxDb server, preparing payload synchronously but doing the send asynchronously.
func (c *Client) SendMetricsAsync(ctx context.Context, metrics *gostatsd.MetricMap, cb gostatsd.SendCallback) {
	if metrics.NumStats == 0 {
		cb(nil)
		return
	}

	batchPoints, err := c.preparePayload(metrics, time.Now().UTC())

	if err != nil {
		cb([]error{err})
		return
	}

	go func() {
		// Write does not take a Context parameter :-(
		err := c.client.Write(batchPoints)

		if err != nil {
			cb([]error{err})
		} else {
			cb(nil)
		}
	}()
}

func (c *Client) preparePayload(metrics *gostatsd.MetricMap, ts time.Time) (client.BatchPoints, error) {
	batchPoints, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database: c.database,
	})

	if err != nil {
		return nil, err
	}

	metrics.Counters.Each(func(key, tagsKey string, counter gostatsd.Counter) {
		k := string(sk(key))

		tags := parseTagsKey(tagsKey)

		addPoint(batchPoints, k+"_count", tags, counter.Value, ts)
		addPoint(batchPoints, k+"_rate", tags, counter.PerSecond, ts)
	})
	metrics.Timers.Each(func(key, tagsKey string, timer gostatsd.Timer) {
		k := string(sk(key))

		tags := parseTagsKey(tagsKey)

		addPoint(batchPoints, k+"_lower", tags, timer.Min, ts)
		addPoint(batchPoints, k+"_upper", tags, timer.Max, ts)
		addPoint(batchPoints, k+"_count", tags, timer.Count, ts)
		addPoint(batchPoints, k+"_count_ps", tags, timer.PerSecond, ts)
		addPoint(batchPoints, k+"_mean", tags, timer.Mean, ts)
		addPoint(batchPoints, k+"_median", tags, timer.Median, ts)
		addPoint(batchPoints, k+"_std", tags, timer.StdDev, ts)
		addPoint(batchPoints, k+"_sum", tags, timer.Sum, ts)
		addPoint(batchPoints, k+"_sum_squares", tags, timer.SumSquares, ts)

		for _, pct := range timer.Percentiles {
			addPoint(batchPoints, strings.Join([]string{k, pct.Str}, "_"), tags, pct.Float, ts)
		}
	})
	metrics.Gauges.Each(func(key, tagsKey string, gauge gostatsd.Gauge) {
		k := string(sk(key))

		tags := parseTagsKey(tagsKey)

		addPoint(batchPoints, k, tags, gauge.Value, ts)
	})
	metrics.Sets.Each(func(key, tagsKey string, set gostatsd.Set) {
		k := string(sk(key))

		tags := parseTagsKey(tagsKey)

		addPoint(batchPoints, k, tags, len(set.Values), ts)
	})
	return batchPoints, nil
}

func parseTagsKey(tagsKey string) map[string]string {
	ret := make(map[string]string)

	tags := strings.Split(tagsKey, ",")
	anonymousTagCount := 0
	for _, tag := range tags {
		if tag == "" {
			continue
		}

		sepIndex := strings.Index(tag, ":")

		if sepIndex < 0 {
			anonymousTagCount++
			ret["tag_"+strconv.Itoa(anonymousTagCount)] = tag
		} else {
			var value string

			if len(tag) > sepIndex+1 {
				value = tag[sepIndex+1:]
			}

			ret[tag[:sepIndex]] = value
		}
	}

	return ret
}

func addPoint(points client.BatchPoints, measurement string, tags map[string]string, value interface{}, ts time.Time) {
	point, err := client.NewPoint(
		measurement,
		tags,
		map[string]interface{}{
			"value": value,
		},
		ts,
	)

	if err != nil {
		log.Errorf("[%s] %s", BackendName, err)
	} else {
		points.AddPoint(point)
	}
}

// SendEvent discards events.
func (c *Client) SendEvent(ctx context.Context, e *gostatsd.Event) error {
	return nil
}

// Name returns the name of the backend.
func (c *Client) Name() string {
	return BackendName
}

// NewClientFromViper constructs a InfluxDbClient object by connecting to an address.
func NewClientFromViper(v *viper.Viper) (gostatsd.Backend, error) {
	g := getSubViper(v, "influxdb")
	g.SetDefault("address", DefaultAddress)
	g.SetDefault("write_timeout", DefaultWriteTimeout)

	return NewClient(&Config{
		Address:      addr(g.GetString("address")),
		Username:     addr(g.GetString("username")),
		Database:     addr(g.GetString("database")),
		Password:     addr(g.GetString("password")),
		WriteTimeout: addrD(g.GetDuration("write_timeout")),
	})
}

// NewClient constructs a InfluxDb backend object.
func NewClient(config *Config) (*Client, error) {
	address := getOrDefaultStr(config.Address, DefaultAddress)
	if address == "" {
		return nil, fmt.Errorf("[%s] address is required", BackendName)
	}
	database := getOrDefaultStr(config.Database, "")
	if database == "" {
		return nil, fmt.Errorf("[%s] database is required", BackendName)
	}
	writeTimeout := getOrDefaultDur(config.WriteTimeout, DefaultWriteTimeout)
	if writeTimeout < 0 {
		return nil, fmt.Errorf("[%s] writeTimeout should be non-negative", BackendName)
	}
	log.Infof("[%s] address=%s database=%s writeTimeout=%s", BackendName, address, database, writeTimeout)

	httpClient, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     address,
		Username: getOrDefaultStr(config.Username, ""),
		Password: getOrDefaultStr(config.Password, ""),
		Timeout:  writeTimeout,
		TLSConfig: &tls.Config{
			// Can't use SSLv3 because of POODLE and BEAST
			// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
			// Can't use TLSv1.1 because of RC4 cipher usage
			MinVersion: tls.VersionTLS12,
		},
	})

	if err != nil {
		return nil, err
	}

	return &Client{
		client:   httpClient,
		database: database,
	}, nil
}

func getOrDefaultStr(val *string, def string) string {
	if val != nil {
		return *val
	}
	return def
}

func getOrDefaultDur(val *time.Duration, def time.Duration) time.Duration {
	if val != nil {
		return *val
	}
	return def
}

func sk(s string) []byte {
	r1 := regWhitespace.ReplaceAllLiteral([]byte(s), []byte{'_'})
	r2 := bytes.Replace(r1, []byte{'/'}, []byte{'-'}, -1)
	return regNonAlphaNum.ReplaceAllLiteral(r2, nil)
}

func addr(s string) *string {
	return &s
}

func addrD(d time.Duration) *time.Duration {
	return &d
}

func getSubViper(v *viper.Viper, key string) *viper.Viper {
	n := v.Sub(key)
	if n == nil {
		n = viper.New()
	}
	return n
}
