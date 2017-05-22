/*********************************************************************************************
Open Query Store

Copyright:
William Durkin (@sql_williamd) / Enrico van de Laar (@evdlaar)

https://github.com/OpenQueryStore/OpenQueryStore

License: 
	This script is free to download and use for personal, educational, and internal 
	corporate purposes, provided that this header is preserved. Redistribution or sale 
	of this script, in whole or in part, is prohibited without the author's express 
	written consent.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, 
	INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
	IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES 
	OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

**********************************************************************************************/

USE [OpenQueryStore]
GO

CREATE PROCEDURE [dbo].[sp_OQS_Gather_Statistics]
	@debug INT = 0,
	@logmode INT = 0

	AS

	DECLARE @log_logrunid INT
	DECLARE @log_newplans INT
	DECLARE @log_newqueries INT

	BEGIN
		
		IF @logmode = 1
			BEGIN
				
				SET @log_logrunid = (SELECT ISNULL(MAX(Log_LogRunID),0)+1 FROM OQS_Log)

			END

		IF @logmode =  1
			BEGIN
				
				INSERT INTO OQS_Log (Log_LogRunID, Log_DateTime, Log_Message) VALUES (@log_logrunid, GETDATE(), 'OpenQueryStore capture script started...')

			END

		-- Create a new interval
		INSERT INTO OQS_Intervals
			(
			Interval_start
			)
		VALUES
			(
			GETDATE()
			)

		-- Start execution plan insertion
		-- Get plans from the plan cache that do not exist in the OQS_Plans table
		-- for the database on the current context
		INSERT INTO OQS_Plans
			(
			plan_handle,
			plan_firstfound,
			plan_database,
			plan_refcounts,
			plan_usecounts,
			plan_sizeinbytes,
			plan_type,
			plan_objecttype,
			plan_executionplan
			)
		SELECT 
			cp.plan_handle,
			GETDATE(),
			DB_NAME(qp.[dbid]),
			cp.refcounts,
			cp.usecounts,
			cp.size_in_bytes,
			cp.cacheobjtype,
			cp.objtype,
			qp.query_plan
		FROM sys.dm_exec_cached_plans cp
		CROSS APPLY sys.dm_exec_query_plan (cp.plan_handle) qp
		WHERE cacheobjtype = 'Compiled Plan'
		AND (qp.query_plan IS NOT NULL AND DB_NAME(qp.[dbid]) IS NOT NULL)
		AND cp.plan_handle NOT IN (SELECT plan_handle FROM OQS_Plans)
		AND qp.[dbid] = DB_ID()

		SET @log_newplans = @@ROWCOUNT

		IF @logmode =  1
			BEGIN
				INSERT INTO OQS_Log (Log_LogRunID, Log_DateTime, Log_Message) VALUES (@log_logrunid, GETDATE(), 'OpenQueryStore captured ' + CONVERT(varchar, @log_newplans) + ' new plan(s)...')
			END

		-- Grab all of the queries (statement level) that are connected to the plans inside the OQS
		;WITH CTE_Queries (plan_id, plan_handle)
		AS
			(
			SELECT
				plan_id,
				plan_handle
			FROM OQS_Plans
			)
		INSERT INTO OQS_Queries
			(
			plan_id,
			query_hash,
			query_statement_text,
			query_statement_start_offset,
			query_statement_end_offset
			)
		SELECT
			cte.plan_id,
			qs.query_hash,
			SUBSTRING
				(
				st.text, (qs.statement_start_offset/2)+1,   
					(
						(
						CASE qs.statement_end_offset  
							WHEN -1 THEN DATALENGTH(st.text)  
							ELSE qs.statement_end_offset  
						END - qs.statement_start_offset
						)/2
					) + 1
				) 
			AS statement_text,
			qs.statement_start_offset,
			qs.statement_end_offset  
		FROM CTE_Queries cte
		INNER JOIN sys.dm_exec_query_stats qs
		ON cte.plan_handle = qs.plan_handle
		CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
		WHERE qs.query_hash NOT IN (SELECT query_hash FROM OQS_Queries)

		SET @log_newqueries = @@ROWCOUNT

		IF @logmode =  1
			BEGIN
				INSERT INTO OQS_Log (Log_LogRunID, Log_DateTime, Log_Message) VALUES (@log_logrunid, GETDATE(), 'OpenQueryStore captured ' + CONVERT(varchar, @log_newqueries) + ' new queries...')
			END

		-- Grab the interval_id of the interval we added at the beginning
		DECLARE @Interval_ID INT = IDENT_CURRENT('OQS_Intervals')

		-- Insert runtime statistics for every query statement that is recorded inside the OQS
		INSERT INTO [dbo].[OQS_Query_Runtime_Stats]
				   ([query_id]
				   ,[interval_id]
				   ,[creation_time]
				   ,[last_execution_time]
				   ,[execution_count]
				   ,[total_elapsed_time]
				   ,[last_elapsed_time]
				   ,[min_elapsed_time]
				   ,[max_elapsed_time]
				   ,[avg_elapsed_time]
				   ,[total_rows]
				   ,[last_rows]
				   ,[min_rows]
				   ,[max_rows]
				   ,[avg_rows]
				   ,[total_worker_time]
				   ,[last_worker_time]
				   ,[min_worker_time]
				   ,[max_worker_time]
				   ,[avg_worker_time]
				   ,[total_physical_reads]
				   ,[last_physical_reads]
				   ,[min_physical_reads]
				   ,[max_physical_reads]
				   ,[avg_physical_reads]
				   ,[total_logical_reads]
				   ,[last_logical_reads]
				   ,[min_logical_reads]
				   ,[max_logical_reads]
				   ,[avg_logical_reads]
				   ,[total_logical_writes]
				   ,[last_logical_writes]
				   ,[min_logical_writes]
				   ,[max_logical_writes]
				   ,[avg_logical_writes]
				   )
		SELECT
			oqs_q.query_id,
			@Interval_ID,
			qs.creation_time,
			qs.last_execution_time,
			qs.execution_count,
			qs.total_elapsed_time,
			qs.last_elapsed_time,
			qs.min_elapsed_time,
			qs.max_elapsed_time,
			0,
			qs.total_rows,
			qs.last_rows,
			qs.min_rows,
			qs.max_rows,
			0,
			qs.total_worker_time,
			qs.last_worker_time,
			qs.min_worker_time,
			qs.max_worker_time,
			0,
			qs.total_physical_reads,
			qs.last_physical_reads,
			qs.min_physical_reads,
			qs.max_physical_reads,
			0,
			qs.total_logical_reads,
			qs.last_logical_reads,
			qs.min_logical_reads,
			qs.max_logical_reads,
			0,
			qs.total_logical_writes,
			qs.last_logical_writes,
			qs.min_logical_writes,
			qs.max_logical_writes,
			0
		FROM OQS_Queries oqs_q
		INNER join sys.dm_exec_query_stats qs
		ON (oqs_q.query_hash = qs.query_hash AND oqs_q.query_statement_start_offset = qs.statement_start_offset AND oqs_q.query_statement_end_offset = qs.statement_end_offset)

		-- DEBUG: Get current info of the QRS table
		IF @debug =  1
			BEGIN
				SELECT 'Snapshot of captured runtime statistics'
				SELECT * FROM OQS_Query_Runtime_Stats
			END

		-- Close the interval now that the statistics are in
		UPDATE OQS_Intervals
		SET Interval_end = GETDATE() WHERE Interval_id = (SELECT MAX(Interval_id)-1 FROM OQS_Intervals)

		-- Now that we have the runtime statistics inside the OQS we need to perform some calculations
		-- so we can see query performance per interval captured

		-- First thing we need is a temporary table to hold our calculated deltas
		IF OBJECT_ID('tempdb..#OQS_Runtime_Stats') IS NOT NULL
			BEGIN
				DROP TABLE #OQS_Runtime_Stats
			END

		-- Calculate deltas and insert them in the temp table
		;WITH CTE_Update_Runtime_Stats 
			(
			query_id, 
			interval_id,
			execution_count, 
			total_elapsed_time, 
			total_rows,
			total_worker_time,
			total_physical_reads,
			total_logical_reads,
			total_logical_writes
			)
			AS
				(
				SELECT
					query_id,
					interval_id,
					execution_count,
					total_elapsed_time,
					total_rows,
					total_worker_time,
					total_physical_reads,
					total_logical_reads,
					total_logical_writes
				FROM OQS_Query_Runtime_Stats
				WHERE interval_id = (SELECT MAX(Interval_id)-1 FROM OQS_Intervals)
				)
		SELECT
			cte.query_id,
			cte.interval_id,
			(qrs.execution_count - cte.execution_count) AS 'Delta Exec Count',
			(qrs.total_elapsed_time - cte.total_elapsed_time) AS 'Delta Time',
			ISNULL(((qrs.total_elapsed_time - cte.total_elapsed_time)/nullif((qrs.execution_count - cte.execution_count),0)),0) AS 'Avg. Time',
			(qrs.total_rows - cte.total_rows) AS 'Delta Total Rows',
			ISNULL(((qrs.total_rows - cte.total_rows)/nullif((qrs.execution_count - cte.execution_count),0)),0) AS 'Avg. Rows',
			(qrs.total_worker_time - cte.total_worker_time) AS 'Delta Total Worker Time',
			ISNULL(((qrs.total_worker_time - cte.total_worker_time)/nullif((qrs.execution_count - cte.execution_count),0)),0) AS 'Avg. Worker Time',
			(qrs.total_physical_reads - cte.total_physical_reads) AS 'Delta Total Phys Reads',
			ISNULL(((qrs.total_physical_reads - cte.total_physical_reads)/nullif((qrs.execution_count - cte.execution_count),0)),0) AS 'Avg. Phys reads',
			(qrs.total_logical_reads - cte.total_logical_reads) AS 'Delta Total Log Reads',
			ISNULL(((qrs.total_logical_reads - cte.total_logical_reads)/nullif((qrs.execution_count - cte.execution_count),0)),0) AS 'Avg. Log reads',
			(qrs.total_logical_writes - cte.total_logical_writes) AS 'Delta Total Log Writes',
			ISNULL(((qrs.total_logical_writes - cte.total_logical_writes)/nullif((qrs.execution_count - cte.execution_count),0)),0) AS 'Avg. Log writes'
		INTO #OQS_Runtime_Stats
		FROM CTE_Update_Runtime_Stats cte
		INNER JOIN OQS_Query_Runtime_Stats qrs
		ON cte.query_id = qrs.query_id
		WHERE qrs.interval_id = (SELECT MAX(Interval_id) FROM OQS_Intervals)

		IF @debug =  1
			BEGIN
				SELECT 'Snapshot of runtime statistics deltas'
				SELECT * FROM #OQS_Runtime_Stats
			END

		-- Update the runtime statistics of the queries captured in the previous interval
		-- with the delta runtime information
		UPDATE qrs
			SET 
				qrs.execution_count = tqrs.[Delta Exec Count],
				qrs.total_elapsed_time = tqrs.[Delta Time],
				qrs.avg_elapsed_time = tqrs.[Avg. Time],
				qrs.total_rows = tqrs.[Delta Total Rows],
				qrs.avg_rows = tqrs.[Avg. Rows],
				qrs.total_worker_time = tqrs.[Delta Total Worker Time],
				qrs.avg_worker_time = tqrs.[Avg. Worker Time],
				qrs.total_physical_reads = tqrs.[Delta Total Phys Reads],
				qrs.avg_physical_reads = tqrs.[Avg. Phys reads],
				qrs.total_logical_reads = tqrs.[Delta Total Log Reads],
				qrs.avg_logical_reads = tqrs.[Avg. Log reads],
				qrs.total_logical_writes = tqrs.[Delta Total Log Writes],
				qrs.avg_logical_writes = tqrs.[Avg. Log writes]
			FROM OQS_Query_Runtime_Stats qrs
			INNER JOIN #OQS_Runtime_Stats tqrs
			ON (qrs.interval_id = tqrs.interval_id AND qrs.query_id = tqrs.query_id)

		IF @debug =  1
			BEGIN
				SELECT 'Snapshot of runtime statistics after delta update'
				SELECT * FROM OQS_Query_Runtime_Stats
			END
	
	-- And we are done!

	IF @logmode =  1
		BEGIN
			INSERT INTO OQS_Log (Log_LogRunID, Log_DateTime, Log_Message) VALUES (@log_logrunid, GETDATE(), 'OpenQueryStore capture script finished...')
		END

	END
	
GO
