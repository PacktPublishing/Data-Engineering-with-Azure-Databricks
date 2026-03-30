-- Databricks notebook source
-- DBTITLE 1,Job Monitoring via System Tables

-- COMMAND ----------

-- QUERY 1: Job Run History & Failures (Last 7 Days)
SELECT
  j.workspace_id
  ,j.name AS job_name
	,jrt.job_id
	,jrt.run_id
	,jrt.result_state
	,jrt.period_start_time AS run_start
	,jrt.period_end_time AS run_end
	,ROUND(TIMESTAMPDIFF(SECOND, jrt.period_start_time, jrt.period_end_time) / 60.0, 2) AS duration_minutes
FROM system.lakeflow.job_run_timeline jrt
INNER JOIN (
	SELECT *
	FROM system.lakeflow.jobs QUALIFY ROW_NUMBER() OVER (
			PARTITION BY workspace_id
			,job_id ORDER BY change_time DESC
			) = 1
	) j ON jrt.workspace_id = j.workspace_id
	AND jrt.job_id = j.job_id
WHERE jrt.period_start_time > current_timestamp() - INTERVAL 7 DAYS;

-- COMMAND ----------

-- QUERY 2: Daily Job Success Rate (Last 30 Days)
WITH completed_runs AS (
		SELECT workspace_id
			,run_id
			,period_end_time AS period_end_time
			,result_state AS result_state
		FROM system.lakeflow.job_run_timeline
		WHERE result_state IS NOT NULL
			AND period_start_time > current_timestamp() - INTERVAL 30 DAYS
		)
SELECT workspace_id
	,DATE (period_end_time) AS run_date
	,COUNT(*) AS total_completed_runs
	,COUNT(CASE 
			WHEN result_state = 'SUCCEEDED'
				THEN 1
			END) AS successful_runs
	,COUNT(CASE 
			WHEN result_state != 'SUCCEEDED'
				THEN 1
			END) AS failed_or_other_runs
	,ROUND(COUNT(CASE 
				WHEN result_state = 'SUCCEEDED'
					THEN 1
				END) * 100.0 / COUNT(*), 2) AS success_rate_pct
FROM completed_runs
GROUP BY workspace_id
	,DATE (period_end_time)
ORDER BY run_date DESC;

-- COMMAND ----------

-- QUERY 3: Compute Cost by SKU (Last 7 Days)
-- Note: not filtered to JOBS only - covers all compute workload types.
SELECT u.workspace_id
	,u.usage_date
	,u.sku_name AS cluster_type
	,SUM(u.usage_quantity) AS dbus_consumed
	,SUM(u.usage_quantity * lp.pricing.effective_list.DEFAULT) AS dbu_cost_usd
FROM system.billing.usage u
JOIN system.billing.list_prices lp ON u.sku_name = lp.sku_name
	AND u.usage_date >= DATE (lp.price_start_time)
	AND (
		lp.price_end_time IS NULL
		OR u.usage_date < DATE (lp.price_end_time)
		)
WHERE u.usage_type = 'COMPUTE_TIME'
	AND u.usage_date >= CURRENT_DATE () - INTERVAL 7 DAYS
GROUP BY u.workspace_id
	,u.usage_date
	,u.sku_name
ORDER BY u.workspace_id
	,u.usage_date
	,cluster_type;