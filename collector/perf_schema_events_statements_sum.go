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

// Scrape `performance_schema.events_statements_summary_by_digest`.

package collector

import (
	"context"
	"database/sql"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

const perfEventsStatementsSumQuery = `
	SELECT
		SUM(COUNT_STAR) AS SUM_COUNT_STAR,
		SUM(SUM_CREATED_TMP_DISK_TABLES) AS SUM_SUM_CREATED_TMP_DISK_TABLES,
		SUM(SUM_CREATED_TMP_TABLES) AS SUM_SUM_CREATED_TMP_TABLES,
		SUM(SUM_ERRORS) AS SUM_SUM_ERRORS,
		SUM(SUM_LOCK_TIME) AS SUM_SUM_LOCK_TIME,
		SUM(SUM_NO_GOOD_INDEX_USED) AS SUM_SUM_NO_GOOD_INDEX_USED,
		SUM(SUM_NO_INDEX_USED) AS SUM_SUM_NO_INDEX_USED,
		SUM(SUM_ROWS_AFFECTED) AS SUM_SUM_ROWS_AFFECTED,
		SUM(SUM_ROWS_EXAMINED) AS SUM_SUM_ROWS_EXAMINED,
		SUM(SUM_ROWS_SENT) AS SUM_SUM_ROWS_SENT,
		SUM(SUM_SELECT_FULL_JOIN) AS SUM_SUM_SELECT_FULL_JOIN,
		SUM(SUM_SELECT_FULL_RANGE_JOIN) AS SUM_SUM_SELECT_FULL_RANGE_JOIN,
		SUM(SUM_SELECT_RANGE) AS SUM_SUM_SELECT_RANGE,
		SUM(SUM_SELECT_RANGE_CHECK) AS SUM_SUM_SELECT_RANGE_CHECK,
		SUM(SUM_SELECT_SCAN) AS SUM_SUM_SELECT_SCAN,
		SUM(SUM_SORT_MERGE_PASSES) AS SUM_SUM_SORT_MERGE_PASSES,
		SUM(SUM_SORT_RANGE) AS SUM_SUM_SORT_RANGE,
		SUM(SUM_SORT_ROWS) AS SUM_SUM_SORT_ROWS,
		SUM(SUM_SORT_SCAN) AS SUM_SUM_SORT_SCAN,
		SUM(SUM_TIMER_WAIT) AS SUM_SUM_TIMER_WAIT,
		SUM(SUM_WARNINGS) AS SUM_SUM_WARNINGS
	FROM performance_schema.events_statements_summary_by_digest;
	`

// Metric descriptors.
var (
	performanceSchemaEventsStatementsSumTotalDesc = newDesc(
		performanceSchema, "events_statements_sum_total",
		"The total count of events statements.",
		nil, nil,
	)
	performanceSchemaEventsStatementsSumCreatedTmpDiskTablesDesc = newDesc(
		performanceSchema, "events_statements_sum_created_tmp_disk_tables",
		"The number of on-disk temporary tables created.",
		nil, nil,
	)
	performanceSchemaEventsStatementsSumCreatedTmpTablesDesc = newDesc(
		performanceSchema, "events_statements_sum_created_tmp_tables",
		"The number of temporary tables created.",
		nil, nil,
	)
	performanceSchemaEventsStatementsSumErrorsDesc = newDesc(
		performanceSchema, "events_statements_sum_errors",
		"Number of errors.",
		nil, nil,
	)
	performanceSchemaEventsStatementsSumLockTimeDesc = newDesc(
		performanceSchema, "events_statements_sum_lock_time",
		"Time in picoseconds spent waiting for locks.",
		nil, nil,
	)
	performanceSchemaEventsStatementsSumNoGoodIndexUsedDesc = newDesc(
		performanceSchema, "events_statements_sum_no_good_index_used",
		"Number of times no good index was found.",
		nil, nil,
	)
	performanceSchemaEventsStatementsSumNoIndexUsedDesc = newDesc(
		performanceSchema, "events_statements_sum_no_index_used",
		"Number of times no index was found.",
		nil, nil,
	)
	performanceSchemaEventsStatementsSumRowsAffectedDesc = newDesc(
		performanceSchema, "events_statements_sum_rows_affected",
		"Number of rows affected by statements.",
		nil, nil,
	)
	performanceSchemaEventsStatementsSumRowsExaminedDesc = newDesc(
		performanceSchema, "events_statements_sum_rows_examined",
		"Number of rows read during statements' execution.",
		nil, nil,
	)
	performanceSchemaEventsStatementsSumRowsSentDesc = newDesc(
		performanceSchema, "events_statements_sum_rows_sent",
		"Number of rows returned.",
		nil, nil,
	)
	performanceSchemaEventsStatementsSumSelectFullJoinDesc = newDesc(
		performanceSchema, "events_statements_sum_select_full_join",
		"Number of joins performed by statements which did not use an index.",
		nil, nil,
	)
	performanceSchemaEventsStatementsSumSelectFullRangeJoinDesc = newDesc(
		performanceSchema, "events_statements_sum_select_full_range_join",
		"Number of joins performed by statements which used a range search of the first table.",
		nil, nil,
	)
	performanceSchemaEventsStatementsSumSelectRangeDesc = newDesc(
		performanceSchema, "events_statements_sum_select_range",
		"Number of joins performed by statements which used a range of the first table.",
		nil, nil,
	)
	performanceSchemaEventsStatementsSumSelectRangeCheckDesc = newDesc(
		performanceSchema, "events_statements_sum_select_range_check",
		"Number of joins without keys performed by statements that check for key usage after each row.",
		nil, nil,
	)
	performanceSchemaEventsStatementsSumSelectScanDesc = newDesc(
		performanceSchema, "events_statements_sum_select_scan",
		"Number of joins performed by statements which used a full scan of the first table.",
		nil, nil,
	)
	performanceSchemaEventsStatementsSumSortMergePassesDesc = newDesc(
		performanceSchema, "events_statements_sum_sort_merge_passes",
		"Number of merge passes by the sort algorithm performed by statements.",
		nil, nil,
	)
	performanceSchemaEventsStatementsSumSortRangeDesc = newDesc(
		performanceSchema, "events_statements_sum_sort_range",
		"Number of sorts performed by statements which used a range.",
		nil, nil,
	)
	performanceSchemaEventsStatementsSumSortRowsDesc = newDesc(
		performanceSchema, "events_statements_sum_sort_rows",
		"Number of rows sorted.",
		nil, nil,
	)
	performanceSchemaEventsStatementsSumSortScanDesc = newDesc(
		performanceSchema, "events_statements_sum_sort_scan",
		"Number of sorts performed by statements which used a full table scan.",
		nil, nil,
	)
	performanceSchemaEventsStatementsSumTimerWaitDesc = newDesc(
		performanceSchema, "events_statements_sum_timer_wait",
		"Total wait time of the summarized events that are timed.",
		nil, nil,
	)
	performanceSchemaEventsStatementsSumWarningsDesc = newDesc(
		performanceSchema, "events_statements_sum_warnings",
		"Number of warnings.",
		nil, nil,
	)
)

// ScrapePerfEventsStatementsSum collects from `performance_schema.events_statements_summary_by_digest`.
type ScrapePerfEventsStatementsSum struct{}

// Name of the Scraper. Should be unique.
func (ScrapePerfEventsStatementsSum) Name() string {
	return "perf_schema.eventsstatementssum"
}

// Help describes the role of the Scraper.
func (ScrapePerfEventsStatementsSum) Help() string {
	return "Collect metrics of grand sums from performance_schema.events_statements_summary_by_digest"
}

// Version of MySQL from which scraper is available.
func (ScrapePerfEventsStatementsSum) Version() float64 {
	return 5.7
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapePerfEventsStatementsSum) Scrape(ctx context.Context, db *sql.DB, ch chan<- prometheus.Metric, logger log.Logger) error {
	// Timers here are returned in picoseconds.
	perfEventsStatementsSumRows, err := db.QueryContext(ctx, perfEventsStatementsSumQuery)
	if err != nil {
		return err
	}
	defer perfEventsStatementsSumRows.Close()

	var (
		total, createdTmpDiskTables, createdTmpTables, errors uint64
		lockTime, noGoodIndexUsed, noIndexUsed, rowsAffected  uint64
		rowsExamined, rowsSent, selectFullJoin                uint64
		selectFullRangeJoin, selectRange, selectRangeCheck    uint64
		selectScan, sortMergePasses, sortRange, sortRows      uint64
		sortScan, timerWait, warnings                         uint64
	)

	for perfEventsStatementsSumRows.Next() {
		if err := perfEventsStatementsSumRows.Scan(
			&total, &createdTmpDiskTables, &createdTmpTables, &errors,
			&lockTime, &noGoodIndexUsed, &noIndexUsed, &rowsAffected,
			&rowsExamined, &rowsSent, &selectFullJoin,
			&selectFullRangeJoin, &selectRange, &selectRangeCheck,
			&selectScan, &sortMergePasses, &sortRange, &sortRows,
			&sortScan, &timerWait, &warnings,
		); err != nil {
			return err
		}
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumTotalDesc, prometheus.CounterValue, float64(total),
		)
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumCreatedTmpDiskTablesDesc, prometheus.CounterValue, float64(createdTmpDiskTables),
		)
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumCreatedTmpTablesDesc, prometheus.CounterValue, float64(createdTmpTables),
		)
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumErrorsDesc, prometheus.CounterValue, float64(errors),
		)
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumLockTimeDesc, prometheus.CounterValue, float64(lockTime),
		)
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumNoGoodIndexUsedDesc, prometheus.CounterValue, float64(noGoodIndexUsed),
		)
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumNoIndexUsedDesc, prometheus.CounterValue, float64(noIndexUsed),
		)
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumRowsAffectedDesc, prometheus.CounterValue, float64(rowsAffected),
		)
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumRowsExaminedDesc, prometheus.CounterValue, float64(rowsExamined),
		)
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumRowsSentDesc, prometheus.CounterValue, float64(rowsSent),
		)
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumSelectFullJoinDesc, prometheus.CounterValue, float64(selectFullJoin),
		)
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumSelectFullRangeJoinDesc, prometheus.CounterValue, float64(selectFullRangeJoin),
		)
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumSelectRangeDesc, prometheus.CounterValue, float64(selectRange),
		)
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumSelectRangeCheckDesc, prometheus.CounterValue, float64(selectRangeCheck),
		)
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumSelectScanDesc, prometheus.CounterValue, float64(selectScan),
		)
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumSortMergePassesDesc, prometheus.CounterValue, float64(sortMergePasses),
		)
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumSortRangeDesc, prometheus.CounterValue, float64(sortRange),
		)
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumSortRowsDesc, prometheus.CounterValue, float64(sortRows),
		)
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumSortScanDesc, prometheus.CounterValue, float64(sortScan),
		)
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumTimerWaitDesc, prometheus.CounterValue, float64(timerWait)/picoSeconds,
		)
		ch <- mustNewConstMetric(
			&ctx,
			performanceSchemaEventsStatementsSumWarningsDesc, prometheus.CounterValue, float64(warnings),
		)
	}
	return nil
}

// check interface
var _ Scraper = ScrapePerfEventsStatementsSum{}
