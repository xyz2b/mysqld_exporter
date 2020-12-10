// Copyright 2018 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"bytes"
	"context"
	"database/sql"
	"regexp"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

)

const (
	// Exporter namespace.
	namespace = "mysql"
	// Math constant for picoseconds to seconds.
	picoSeconds = 1e12
	// Query to check whether user/table/client stats are enabled.
	userstatCheckQuery = `SHOW GLOBAL VARIABLES WHERE Variable_Name='userstat'
		OR Variable_Name='userstat_running'`
)

var (
	logRE = regexp.MustCompile(`.+\.(\d+)$`)
	extraLabels = []string{"hostname", "subsystemName", "subsystemID"}
)

func counterVecWithLabelValues(v *prometheus.CounterVec, lvs ...string) prometheus.Counter {
	if lvs != nil {
		lvs = append(lvs, extraLabels...)
	} else {
		lvs = extraLabels
	}

	counter := v.WithLabelValues(lvs...)
	return counter
}

func gaugeVecWithLabelValues(v *prometheus.GaugeVec, lvs ...string) prometheus.Gauge {
	if lvs != nil {
		lvs = append(lvs, extraLabels...)
	} else {
		lvs = extraLabels
	}

	counter := v.WithLabelValues(lvs...)
	return counter
}

func newGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec {
	if labelNames != nil {
		labelNames = append(labelNames, extraLabels...)
	} else {
		labelNames = extraLabels
	}

	counterVec := prometheus.NewGaugeVec(opts, labelNames)
	return counterVec
}

func newCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	if labelNames != nil {
		labelNames = append(labelNames, extraLabels...)
	} else {
		labelNames = extraLabels
	}

	counterVec := prometheus.NewCounterVec(opts, labelNames)
	return counterVec
}

func newDesc(subsystem, name, help string, labels []string, constLabels prometheus.Labels) *prometheus.Desc {
	if labels != nil {
		labels = append(labels, extraLabels...)
	} else {
		labels = extraLabels
	}

	return prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subsystem, name),
		help, labels, constLabels,
	)
}

func mustNewConstHistogram(
	ctx *context.Context,
	desc *prometheus.Desc,
	count uint64,
	sum float64,
	buckets map[float64]uint64,
	labelValues ...string,
) prometheus.Metric {
	subsystemID := ""
	if id, ok := (*ctx).Value("subsystemID").(string); ok {
		subsystemID = id
	}
	subsystemName := ""
	if name, ok := (*ctx).Value("subsystemName").(string); ok {
		subsystemName = name
	}
	hostname := ""
	if n, ok := (*ctx).Value("hostname").(string); ok {
		hostname = n
	}

	if labelValues != nil {
		extraLabels := []string{hostname, subsystemName, subsystemID}
		labelValues = append(labelValues, extraLabels...)
	} else {
		labelValues = []string{hostname, subsystemName, subsystemID}
	}

	metric := prometheus.MustNewConstHistogram(
		desc, count, sum, buckets, labelValues...
	)
	return metric
}

func mustNewConstMetric(ctx *context.Context, desc *prometheus.Desc, valueType prometheus.ValueType, value float64, labelValues ...string) prometheus.Metric {
	subsystemID := ""
	if id, ok := (*ctx).Value("subsystemID").(string); ok {
		subsystemID = id
	}
	subsystemName := ""
	if name, ok := (*ctx).Value("subsystemName").(string); ok {
		subsystemName = name
	}
	hostname := ""
	if n, ok := (*ctx).Value("hostname").(string); ok {
		hostname = n
	}

	if labelValues != nil {
		extraLabels := []string{hostname, subsystemName, subsystemID}
		labelValues = append(labelValues, extraLabels...)
	} else {
		labelValues = []string{hostname, subsystemName, subsystemID}
	}

	metric := prometheus.MustNewConstMetric(desc, valueType, value, labelValues...)

	return metric
}

func parseStatus(data sql.RawBytes) (float64, bool) {
	if bytes.Equal(data, []byte("Yes")) || bytes.Equal(data, []byte("ON")) {
		return 1, true
	}
	if bytes.Equal(data, []byte("No")) || bytes.Equal(data, []byte("OFF")) {
		return 0, true
	}
	// SHOW SLAVE STATUS Slave_IO_Running can return "Connecting" which is a non-running state.
	if bytes.Equal(data, []byte("Connecting")) {
		return 0, true
	}
	// SHOW GLOBAL STATUS like 'wsrep_cluster_status' can return "Primary" or "non-Primary"/"Disconnected"
	if bytes.Equal(data, []byte("Primary")) {
		return 1, true
	}
	if strings.EqualFold(string(data), "non-Primary") || bytes.Equal(data, []byte("Disconnected")) {
		return 0, true
	}
	if logNum := logRE.Find(data); logNum != nil {
		value, err := strconv.ParseFloat(string(logNum), 64)
		return value, err == nil
	}
	value, err := strconv.ParseFloat(string(data), 64)
	return value, err == nil
}

func parsePrivilege(data sql.RawBytes) (float64, bool) {
	if bytes.Equal(data, []byte("Y")) {
		return 1, true
	}
	if bytes.Equal(data, []byte("N")) {
		return 0, true
	}
	return -1, false
}
