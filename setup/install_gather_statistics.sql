/*********************************************************************************************
Open Query Store
Install gather_statistics for Open Query Store
v0.8 - January 2018

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

USE [{DatabaseWhereOQSIsRunning}];
GO

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

CREATE PROCEDURE [oqs].[gather_statistics]
    @debug   int = 0,
    @logmode int = 0
AS
    DECLARE @log_logrunid int;
    DECLARE @log_newplans int;
    DECLARE @log_newqueries int;
    DECLARE @log_runtime_stats int;
    DECLARE @log_wait_stats int;
    DECLARE @execution_threshold int;
    DECLARE @collection_active bit;
	DECLARE @oqs_maximum_size_mb smallint;
    DECLARE @data_cleanup_active bit;
    DECLARE @data_cleanup_threshold tinyint;
    DECLARE @data_cleanup_throttle smallint;
    BEGIN

        SET NOCOUNT ON;

        SELECT @collection_active   = [collection_active],
			   @oqs_maximum_size_mb = [oqs_maximum_size_mb],
               @data_cleanup_active = [data_cleanup_active],
               @data_cleanup_threshold = [data_cleanup_threshold],
               @data_cleanup_throttle = [data_cleanup_throttle]
        FROM   [oqs].[collection_metadata];

        -- Data collection must be activated for us to actually collect data
        IF ( SELECT [collection_active] FROM [oqs].[collection_metadata] ) = 1
            BEGIN
                -- If we have no databases to monitor, we can skip doing anything!
                IF ( SELECT COUNT( * ) FROM [oqs].[monitored_databases] AS [MD] ) = 0
                    BEGIN
                        SET @log_logrunid = (   SELECT ISNULL( MAX( [AL].[log_run_id] ), 0 ) + 1
                                                FROM   [oqs].[activity_log] AS [AL] );


                        INSERT INTO [oqs].[activity_log] ( [log_run_id],
                                                           [log_timestamp],
                                                           [log_message] )
                        VALUES ( @log_logrunid, GETDATE(), 'No databases are registered for monitoring' );
                    END;
                ELSE
                -- We can only collect data if the maximum configured size is higher than the space currently used by OQS
                IF (@oqs_maximum_size_mb*1024 > (SELECT SUM(space_used_kb) FROM oqs.object_catalog))
                    BEGIN
                        -- Monitoring is active *and* we have at least one database to process. Let's get to work
                        BEGIN
                            IF @logmode = 1
                                BEGIN
                                    SET @log_logrunid = (   SELECT ISNULL( MAX( [AL].[log_run_id] ), 0 ) + 1
                                                            FROM   [oqs].[activity_log] AS [AL] );
                                END;

                            IF @logmode = 1
                                BEGIN
                                    INSERT INTO [oqs].[activity_log] ( [log_run_id],
                                                                       [log_timestamp],
                                                                       [log_message] )
                                    VALUES ( @log_logrunid, GETDATE(), 'OpenQueryStore capture script started...' );
                                END;

                            -- Create a new interval
                            INSERT INTO [oqs].[intervals] ( [interval_start] ) VALUES ( GETDATE());

                            -- Setup the execution_threshold. If we have no setting, we default to 2 to avoid single use plans
                            SELECT @execution_threshold = [execution_threshold]
                            FROM   [oqs].[collection_metadata];

                            IF @execution_threshold IS NULL SET @execution_threshold = 2;


                            IF OBJECT_ID( 'tempdb..#plan_dbid' ) IS NOT NULL DROP TABLE [#plan_dbid];

                            CREATE TABLE [#plan_dbid]
                                (
                                    [plan_handle] varbinary (64) NOT NULL,
                                    [dbid]        int            NOT NULL,
                                    PRIMARY KEY CLUSTERED ( [plan_handle], [dbid] )
                                );

                            -- We capture plans based on databases that are present in the monitored_databases table of the OpenQueryStore database				
                            -- First, everything goes into a temp table (we'll need this data for the next query)	
                            INSERT INTO [#plan_dbid] ( [plan_handle],
                                                       [dbid] )
                                        SELECT   [plan_handle],
                                                 CONVERT( int, [pvt].[dbid] )
                                        FROM     (   SELECT [plan_handle],
                                                            [epa].[attribute],
                                                            [epa].[value]
                                                     FROM   [sys].[dm_exec_cached_plans]
                                                            OUTER APPLY [sys].[dm_exec_plan_attributes]( [plan_handle] ) AS [epa]
                                                     WHERE  [cacheobjtype] = 'Compiled Plan'
                                                            AND [usecounts] >= @execution_threshold ) AS [ecpa]
                                        PIVOT (   MAX([value])
                                                  FOR [attribute] IN ( "dbid", "sql_handle" )) AS [pvt]
                                        WHERE    [plan_handle] NOT IN ( SELECT [PD].[plan_handle] FROM [oqs].[plan_dbid] AS [PD] )
                                                 AND [pvt].[dbid] IN (   SELECT DB_ID( [MD].[database_name] )
                                                                         FROM   [oqs].[monitored_databases] AS [MD] )
                                        ORDER BY [pvt].[sql_handle];

                            -- Next, we add the rows to the destination table
                            INSERT INTO [oqs].[plan_dbid] ( [plan_handle],
                                                            [dbid] )
                                        SELECT [plan_handle], [dbid] FROM [#plan_dbid];

                            -- Start execution plan insertion
                            -- Get plans from the plan cache that do not exist in the OQS_Plans table
                            -- for the database on the current context
                            IF OBJECT_ID( 'tempdb..#plans' ) IS NOT NULL DROP TABLE [#plans];
                        
                            SELECT TOP(0)
                                [plan_MD5],
                                [plan_handle],
                                [plan_firstfound],
                                [plan_database],
                                [plan_refcounts],
                                [plan_usecounts],
                                [plan_sizeinbytes],
                                [plan_type],
                                [plan_objecttype],
                                [plan_executionplan]
                            INTO #plans
                            FROM [oqs].[plans];

                            INSERT INTO #plans ( [plan_MD5],
                                [plan_handle],
                                [plan_firstfound],
                                [plan_database],
                                [plan_refcounts],
                                [plan_usecounts],
                                [plan_sizeinbytes],
                                [plan_type],
                                [plan_objecttype],
                                [plan_executionplan] )
                             SELECT 
                                   [plan_MD5],
                                   [plan_handle],
                                   [plan_firstfound],
                                   [plan_database],
                                   [plan_refcounts],
                                   [plan_usecounts],
                                   [plan_sizeinbytes],
                                   [plan_type],
                                   [plan_objecttype],
                                   [qp].[query_plan] AS [plan_executionplan]
                             FROM (
                                      SELECT TOP(2147483645) 
                                               ( [qs].[query_hash] + [qs].[query_plan_hash] ) AS [plan_MD5],
                                               [cp].[plan_handle],
                                               GETDATE() AS [plan_firstfound],
                                               DB_NAME( [pd].[dbid] ) AS [plan_database],
                                               [cp].[refcounts] AS [plan_refcounts],
                                               [cp].[usecounts] AS [plan_usecounts],
                                               [cp].[size_in_bytes] AS [plan_sizeinbytes],
                                               [cp].[cacheobjtype] AS [plan_type],
                                               [cp].[objtype] AS [plan_objecttype]
                                        FROM   [oqs].[plan_dbid]                                            AS [pd]
                                               INNER HASH JOIN [sys].[dm_exec_cached_plans]                 AS [cp] ON [pd].[plan_handle] = [cp].[plan_handle]
                                               INNER JOIN [sys].[dm_exec_query_stats] AS [qs] ON [pd].[plan_handle] = [qs].[plan_handle]
                                        WHERE  [cp].[cacheobjtype] = 'Compiled Plan'
                                               AND EXISTS (
                                                    SELECT * 
                                                    FROM [#plan_dbid] AS [tpdb] 
                                                    WHERE [tpdb].[plan_handle] = [pd].[plan_handle]
                                                        AND [tpdb].[dbid] = [pd].[dbid]
                                                )
                             ) AS [cp]
                             CROSS APPLY [sys].[dm_exec_query_plan]( [cp].[plan_handle] ) AS [qp]
                             WHERE ( [qp].[query_plan] IS NOT NULL );

                                                                    
                                                                    
                            INSERT INTO [oqs].[plans] ( [plan_MD5],
                                    [plan_handle],
                                    [plan_firstfound],
                                    [plan_database],
                                    [plan_refcounts],
                                    [plan_usecounts],
                                    [plan_sizeinbytes],
                                    [plan_type],
                                    [plan_objecttype],
                                    [plan_executionplan] 
                            )
                            SELECT * 
                            FROM #plans
                            WHERE [plan_MD5] NOT IN (SELECT [plan_MD5] FROM [oqs].[plans]);

                            SET @log_newplans = @@RowCount;

                            IF @logmode = 1
                                BEGIN
                                    INSERT INTO [oqs].[activity_log] ( [log_run_id],
                                                                       [log_timestamp],
                                                                       [log_message] )
                                    VALUES ( @log_logrunid, GETDATE(), 'OpenQueryStore captured ' + CONVERT( varchar, @log_newplans ) + ' new plan(s)...' );
                                END

                                -- Now that the plans are stored on disk we are going to retrieve additional plan information
                                -- from the XML plan
                                ;
                            WITH XMLNAMESPACES ( DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' )
                            UPDATE [oqs].[plans]
                            SET    [plan_optimization] = [p2].[Optimization_level],
                                   [xml_processed] = 1
                            FROM   (   SELECT [p].[plan_id],
                                              CASE WHEN [Exec_Plans].[Plans].[value]( '(/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple/@StatementOptmLevel)[1]', 'varchar' ) = 'T' THEN 'Trivial'
                                                   WHEN [Exec_Plans].[Plans].[value]( '(/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple/@StatementOptmLevel)[1]', 'varchar' ) = 'F' THEN 'Full'
                                              END AS [Optimization_level]
                                       FROM   [oqs].[plans]                                   AS [p]
                                              CROSS APPLY [plan_executionplan].[nodes]( '.' ) AS [Exec_Plans]([Plans]) ) AS [p2]
                            WHERE  [plans].[plan_id] = [p2].[plan_id]
                                   AND [xml_processed] = 0

                            -- Grab all of the queries (statement level) that are connected to the plans inside the OQS
                            ;
                            WITH [CTE_Queries] ( [plan_id], [plan_handle], [plan_MD5] )
                            AS ( SELECT [plan_id], [plan_handle], [plan_MD5] FROM [oqs].[plans] )
                            INSERT INTO [oqs].[queries] ( [plan_id],
                                                          [query_hash],
                                                          [query_plan_MD5],
                                                          [query_statement_text],
                                                          [query_statement_start_offset],
                                                          [query_statement_end_offset],
                                                          [query_creation_time] )
                                        SELECT [cte].[plan_id],
                                               [qs].[query_hash],
                                               ( [cte].[plan_MD5] + [qs].[query_hash] )                                                                                AS [Query_plan_MD5],
                                               SUBSTRING( [st].[text], ( [qs].[statement_start_offset] / 2 ) + 1, (( CASE [qs].[statement_end_offset]
                                                                                                                          WHEN-1 THEN DATALENGTH( [st].[text] )
                                                                                                                          ELSE [qs].[statement_end_offset]
                                                                                                                     END - [qs].[statement_start_offset] ) / 2 ) + 1 ) AS [statement_text],
                                               [qs].[statement_start_offset],
                                               [qs].[statement_end_offset],
                                               [qs].[creation_time]
                                        FROM   [CTE_Queries]                                             AS [cte]
                                               INNER JOIN [oqs].[query_stats]                            AS [qs] ON [cte].[plan_handle] = [qs].[plan_handle]
                                               CROSS APPLY [sys].[dm_exec_sql_text]( [qs].[sql_handle] ) AS [st]
                                        WHERE  ( [cte].[plan_MD5] + [qs].[query_hash] ) NOT IN ( SELECT [query_plan_MD5] FROM [oqs].[queries] );

                            SET @log_newqueries = @@RowCount;

                            IF @logmode = 1
                                BEGIN
                                    INSERT INTO [oqs].[activity_log] ( [log_run_id],
                                                                       [log_timestamp],
                                                                       [log_message] )
                                    VALUES ( @log_logrunid, GETDATE(), 'OpenQueryStore captured ' + CONVERT( varchar, @log_newqueries ) + ' new queries...' );
                                END;

                            -- Remove all the queries that are related to OQS plans
                            ;
                            WITH XMLNAMESPACES ( DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' )
                            DELETE FROM [oqs].[queries]
                            WHERE [plan_id] IN (   SELECT [p].[plan_id]
                                                   FROM   [oqs].[plans]                                   AS [p]
                                                          CROSS APPLY [plan_executionplan].[nodes]( '.' ) AS [Exec_Plans]([Plans])
                                                   WHERE  [Exec_Plans].[Plans].[exist]( '//ColumnReference[@Schema = "[oqs]"]' ) = 1 )

                            -- Remove all the plans that are related to OQS plans
                            ;
                            WITH XMLNAMESPACES ( DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' )
                            DELETE FROM [oqs].[plans]
                            WHERE [plan_id] IN (   SELECT [p].[plan_id]
                                                   FROM   [oqs].[plans]                                   AS [p]
                                                          CROSS APPLY [plan_executionplan].[nodes]( '.' ) AS [Exec_Plans]([Plans])
                                                   WHERE  [Exec_Plans].[Plans].[exist]( '//ColumnReference[@Schema = "[oqs]"]' ) = 1 );


                            -- Grab the interval_id of the interval we added at the beginning
                            DECLARE @Interval_ID int;
                            SET @Interval_ID = IDENT_CURRENT( '[oqs].[intervals]' );

                            -- Query Runtime Snapshot

                            -- Insert runtime statistics for every query statement that is recorded inside the OQS
                            INSERT INTO [oqs].[query_runtime_stats] ( [query_id],
                                                                      [interval_id],
                                                                      [creation_time],
                                                                      [last_execution_time],
                                                                      [execution_count],
                                                                      [total_elapsed_time],
                                                                      [last_elapsed_time],
                                                                      [min_elapsed_time],
                                                                      [max_elapsed_time],
                                                                      [avg_elapsed_time],
                                                                      [total_rows],
                                                                      [last_rows],
                                                                      [min_rows],
                                                                      [max_rows],
                                                                      [avg_rows],
                                                                      [total_worker_time],
                                                                      [last_worker_time],
                                                                      [min_worker_time],
                                                                      [max_worker_time],
                                                                      [avg_worker_time],
                                                                      [total_physical_reads],
                                                                      [last_physical_reads],
                                                                      [min_physical_reads],
                                                                      [max_physical_reads],
                                                                      [avg_physical_reads],
                                                                      [total_logical_reads],
                                                                      [last_logical_reads],
                                                                      [min_logical_reads],
                                                                      [max_logical_reads],
                                                                      [avg_logical_reads],
                                                                      [total_logical_writes],
                                                                      [last_logical_writes],
                                                                      [min_logical_writes],
                                                                      [max_logical_writes],
                                                                      [avg_logical_writes] )
                                        SELECT [oqs_q].[query_id],
                                               @Interval_ID,
                                               MIN([qs].[creation_time]),
                                               MAX([qs].[last_execution_time]),
                                               SUM([qs].[execution_count]),
                                               SUM([qs].[total_elapsed_time]),
                                               SUM([qs].[last_elapsed_time]),
                                               MIN([qs].[min_elapsed_time]),
                                               MAX([qs].[max_elapsed_time]),
                                               0,
                                               SUM([qs].[total_rows]),
                                               SUM([qs].[last_rows]),
                                               MIN([qs].[min_rows]),
                                               MAX([qs].[max_rows]),
                                               0,
                                               SUM([qs].[total_worker_time]),
                                               SUM([qs].[last_worker_time]),
                                               MIN([qs].[min_worker_time]),
                                               MAX([qs].[max_worker_time]),
                                               0,
                                               SUM([qs].[total_physical_reads]),
                                               SUM([qs].[last_physical_reads]),
                                               MIN([qs].[min_physical_reads]),
                                               MAX([qs].[max_physical_reads]),
                                               0,
                                               SUM([qs].[total_logical_reads]),
                                               SUM([qs].[last_logical_reads]),
                                               MIN([qs].[min_logical_reads]),
                                               MAX([qs].[max_logical_reads]),
                                               0,
                                               SUM([qs].[total_logical_writes]),
                                               SUM([qs].[last_logical_writes]),
                                               MIN([qs].[min_logical_writes]),
                                               MAX([qs].[max_logical_writes]),
                                               0
                                        FROM   [oqs].[queries]                AS [oqs_q]
                                               INNER JOIN [oqs].[query_stats] AS [qs] ON (   [oqs_q].[query_hash] = [qs].[query_hash]
                                                                                             AND [oqs_q].[query_statement_start_offset] = [qs].[statement_start_offset]
                                                                                             AND [oqs_q].[query_statement_end_offset] = [qs].[statement_end_offset]
                                                                                             AND [oqs_q].[query_creation_time] = [qs].[creation_time] )
                                        GROUP BY [oqs_q].[query_id];

                            -- DEBUG: Get current info of the QRS table
                            IF @debug = 1
                                BEGIN
                                    SELECT 'Snapshot of captured runtime statistics';
                                    SELECT * FROM [oqs].[query_runtime_stats] AS [QRS];
                                END;

                            -- Wait stats snapshot
                            INSERT INTO [oqs].[wait_stats] ( [interval_id],
                                                             [wait_type],
                                                             [waiting_tasks_count],
                                                             [wait_time_ms],
                                                             [max_wait_time_ms],
                                                             [signal_wait_time_ms] )
                                        SELECT @Interval_ID,
                                               [wait_type],
                                               [waiting_tasks_count],
                                               [wait_time_ms],
                                               [max_wait_time_ms],
                                               [signal_wait_time_ms]
                                        FROM   [sys].[dm_os_wait_stats]
                                        WHERE  (   [waiting_tasks_count] > 0
                                                   AND [wait_time_ms] > 0 );

                            -- DEBUG: Get current info of the wait stats table
                            IF @debug = 1
                                BEGIN
                                    SELECT 'Snapshot of wait stats';
                                    SELECT * FROM [oqs].[wait_stats] AS [WS];
                                END;

                            -- Close the previous interval now that the statistics are updated
                            UPDATE [oqs].[intervals]
                            SET    [interval_end] = GETDATE()
                            WHERE  [interval_id] = ( SELECT MAX( [interval_id] ) - 1 FROM [oqs].[intervals] );

                            -- Now that we have the runtime statistics inside the OQS we need to perform some calculations
                            -- so we can see query performance per interval captured

                            -- First thing we need is a temporary table to hold our calculated deltas
                            IF OBJECT_ID( 'tempdb..#OQS_Runtime_Stats' ) IS NOT NULL
                                BEGIN
                                    DROP TABLE [#OQS_Runtime_Stats];
                                END;

                            IF OBJECT_ID( 'tempdb..#OQS_Wait_Stats' ) IS NOT NULL
                                BEGIN
                                    DROP TABLE [#OQS_Wait_Stats];
                                END

                                -- Calculate Deltas for Runtime stats
                                ;
                            WITH [CTE_Update_Runtime_Stats] ( [query_id], [interval_id], [execution_count], [total_elapsed_time], [total_rows], [total_worker_time], [total_physical_reads], [total_logical_reads], [total_logical_writes] )
                            AS ( SELECT [QRS].[query_id],
                                        [QRS].[interval_id],
                                        [QRS].[execution_count],
                                        [QRS].[total_elapsed_time],
                                        [QRS].[total_rows],
                                        [QRS].[total_worker_time],
                                        [QRS].[total_physical_reads],
                                        [QRS].[total_logical_reads],
                                        [QRS].[total_logical_writes]
                                 FROM   [oqs].[query_runtime_stats] AS [QRS]
                                 WHERE  [QRS].[interval_id] = ( SELECT MAX( [interval_id] ) - 1 FROM [oqs].[intervals] ))
                            SELECT [cte].[query_id],
                                   [cte].[interval_id],
                                   ( [qrs].[execution_count] - [cte].[execution_count] )                                                                                            AS [Delta Exec Count],
                                   ( [qrs].[total_elapsed_time] - [cte].[total_elapsed_time] )                                                                                      AS [Delta Time],
                                   ISNULL((( [qrs].[total_elapsed_time] - [cte].[total_elapsed_time] ) / NULLIF(( [qrs].[execution_count] - [cte].[execution_count] ), 0)), 0 )     AS [Avg. Time],
                                   ( [qrs].[total_rows] - [cte].[total_rows] )                                                                                                      AS [Delta Total Rows],
                                   ISNULL((( [qrs].[total_rows] - [cte].[total_rows] ) / NULLIF(( [qrs].[execution_count] - [cte].[execution_count] ), 0)), 0 )                     AS [Avg. Rows],
                                   ( [qrs].[total_worker_time] - [cte].[total_worker_time] )                                                                                        AS [Delta Total Worker Time],
                                   ISNULL((( [qrs].[total_worker_time] - [cte].[total_worker_time] ) / NULLIF(( [qrs].[execution_count] - [cte].[execution_count] ), 0)), 0 )       AS [Avg. Worker Time],
                                   ( [qrs].[total_physical_reads] - [cte].[total_physical_reads] )                                                                                  AS [Delta Total Phys Reads],
                                   ISNULL((( [qrs].[total_physical_reads] - [cte].[total_physical_reads] ) / NULLIF(( [qrs].[execution_count] - [cte].[execution_count] ), 0)), 0 ) AS [Avg. Phys reads],
                                   ( [qrs].[total_logical_reads] - [cte].[total_logical_reads] )                                                                                    AS [Delta Total Log Reads],
                                   ISNULL((( [qrs].[total_logical_reads] - [cte].[total_logical_reads] ) / NULLIF(( [qrs].[execution_count] - [cte].[execution_count] ), 0)), 0 )   AS [Avg. Log reads],
                                   ( [qrs].[total_logical_writes] - [cte].[total_logical_writes] )                                                                                  AS [Delta Total Log Writes],
                                   ISNULL((( [qrs].[total_logical_writes] - [cte].[total_logical_writes] ) / NULLIF(( [qrs].[execution_count] - [cte].[execution_count] ), 0)), 0 ) AS [Avg. Log writes]
                            INTO   [#OQS_Runtime_Stats]
                            FROM   [CTE_Update_Runtime_Stats]             AS [cte]
                                   INNER JOIN [oqs].[query_runtime_stats] AS [qrs] ON [cte].[query_id] = [qrs].[query_id]
                            WHERE  [qrs].[interval_id] = ( SELECT MAX( [interval_id] ) FROM [oqs].[intervals] );

                            SET @log_runtime_stats = @@RowCount;

                            IF @logmode = 1
                                BEGIN
                                    INSERT INTO [oqs].[activity_log] ( [log_run_id],
                                                                       [log_timestamp],
                                                                       [log_message] )
                                    VALUES ( @log_logrunid, GETDATE(), 'OpenQueryStore captured ' + CONVERT( varchar, @log_runtime_stats ) + ' runtime statistics...' );
                                END;

                            IF @debug = 1
                                BEGIN
                                    SELECT 'Snapshot of runtime statistics deltas';
                                    SELECT * FROM [#OQS_Runtime_Stats];
                                END;

                            -- Update the runtime statistics of the queries captured in the previous interval
                            -- with the delta runtime information
                            UPDATE [qrs]
                            SET    [qrs].[execution_count] = [tqrs].[Delta Exec Count],
                                   [qrs].[total_elapsed_time] = [tqrs].[Delta Time],
                                   [qrs].[avg_elapsed_time] = [tqrs].[Avg. Time],
                                   [qrs].[total_rows] = [tqrs].[Delta Total Rows],
                                   [qrs].[avg_rows] = [tqrs].[Avg. Rows],
                                   [qrs].[total_worker_time] = [tqrs].[Delta Total Worker Time],
                                   [qrs].[avg_worker_time] = [tqrs].[Avg. Worker Time],
                                   [qrs].[total_physical_reads] = [tqrs].[Delta Total Phys Reads],
                                   [qrs].[avg_physical_reads] = [tqrs].[Avg. Phys reads],
                                   [qrs].[total_logical_reads] = [tqrs].[Delta Total Log Reads],
                                   [qrs].[avg_logical_reads] = [tqrs].[Avg. Log reads],
                                   [qrs].[total_logical_writes] = [tqrs].[Delta Total Log Writes],
                                   [qrs].[avg_logical_writes] = [tqrs].[Avg. Log writes]
                            FROM   [oqs].[query_runtime_stats]     AS [qrs]
                                   INNER JOIN [#OQS_Runtime_Stats] AS [tqrs] ON (   [qrs].[interval_id] = [tqrs].[interval_id]
                                                                                    AND [qrs].[query_id] = [tqrs].[query_id] );

                            IF @debug = 1
                                BEGIN
                                    SELECT 'Snapshot of runtime statistics after delta update';
                                    SELECT * FROM [oqs].[query_runtime_stats] AS [qrs];
                                END;

                                -- Calculate Deltas for Wait stats
						    ;
                            WITH [CTE_Update_wait_Stats] ( [interval_id], [wait_type], [waiting_tasks_count], [wait_time_ms], [max_wait_time_ms], [signal_wait_time_ms] )
                                AS ( SELECT [WS].[interval_id],
                                            [WS].[wait_type],
                                            [WS].[waiting_tasks_count],
                                            [WS].[wait_time_ms],
                                            [WS].[max_wait_time_ms],
                                            [WS].[signal_wait_time_ms]
                                     FROM   [oqs].[wait_stats] AS [WS]
                                     WHERE  [WS].[interval_id] = ( SELECT MAX( [interval_id] ) - 1 FROM [oqs].[intervals] ))
                            SELECT [cte].[wait_type],
                                   [cte].[interval_id],
                                   ( [WS].[waiting_tasks_count] - [cte].[waiting_tasks_count] ) AS [Delta Waiting Tasks Count],
                                   ( [WS].[wait_time_ms] - [cte].[wait_time_ms] )               AS [Delta Wait Time ms],
                                   ( [WS].[max_wait_time_ms] - [cte].[max_wait_time_ms] )       AS [Delta Max Wait Time ms],
                                   ( [WS].[signal_wait_time_ms] - [cte].[signal_wait_time_ms] ) AS [Delta Signal Wait Time ms]
                            INTO   [#OQS_Wait_Stats]
                            FROM   [CTE_Update_wait_Stats]       AS [cte]
                                   INNER JOIN [oqs].[wait_stats] AS [WS] ON [cte].[wait_type] = [WS].[wait_type]
                            WHERE  [WS].[interval_id] = ( SELECT MAX( [interval_id] ) FROM [oqs].[intervals] );

                            SET @log_wait_stats = @@RowCount;

                            IF @logmode = 1
                                BEGIN
                                    INSERT INTO [oqs].[activity_log] ( [log_run_id],
                                                                       [log_timestamp],
                                                                       [log_message] )
                                    VALUES ( @log_logrunid, GETDATE(), 'OpenQueryStore captured ' + CONVERT( varchar, @log_wait_stats ) + ' wait statistics...' );
                                END;

                            IF @debug = 1
                                BEGIN
                                    SELECT 'Snapshot of wait stats deltas';
                                    SELECT * FROM [#OQS_Wait_Stats];
                                END;

                            -- Update the runtime statistics of the queries captured in the previous interval
                            -- with the delta runtime information
                            UPDATE [WS]
                            SET    [WS].[waiting_tasks_count] = [tws].[Delta Waiting Tasks Count],
                                   [WS].[wait_time_ms] = [Delta Wait Time ms],
                                   [WS].[max_wait_time_ms] = [Delta Max Wait Time ms],
                                   [WS].[signal_wait_time_ms] = [Delta Signal Wait Time ms]
                            FROM   [oqs].[wait_stats]           AS [WS]
                                   INNER JOIN [#OQS_Wait_Stats] AS [tws] ON (   [WS].[interval_id] = [tws].[interval_id]
                                                                                AND [WS].[wait_type] = [tws].[wait_type] );

                            IF @debug = 1
                                BEGIN
                                    SELECT 'Snapshot of runtime statistics after delta update';
                                    SELECT * FROM [oqs].[wait_stats] AS [WS];
                                END;

                            -- Remove the wait stats delta's where 0 waits occured
                            DELETE FROM [oqs].[wait_stats]
                            WHERE (   [waiting_tasks_count] = 0
                                      AND [interval_id] = ( SELECT MAX( [interval_id] ) - 1 FROM [oqs].[intervals] ));

                            -- And we are done!
                            IF @logmode = 1
                                BEGIN
                                    INSERT INTO [oqs].[activity_log] ( [log_run_id],
                                                                       [log_timestamp],
                                                                       [log_message] )
                                    VALUES ( @log_logrunid, GETDATE(), 'OpenQueryStore capture script finished...' );
                                END;
                        END;
                    END
                ELSE
                    BEGIN
                        SET @log_logrunid = (   SELECT ISNULL( MAX( [AL].[log_run_id] ), 0 ) + 1
                                                FROM   [oqs].[activity_log] AS [AL] );

                        INSERT INTO [oqs].[activity_log] ( [log_run_id],
                                                           [log_timestamp],
                                                           [log_message] )
                        VALUES ( @log_logrunid, GETDATE(), 'The OpenQueryStore data store is full. Current configured maximum size (MB): '+CAST(@oqs_maximum_size_mb AS varchar(5)));
                    END;
            END;
        ELSE
            BEGIN
                SET @log_logrunid = (   SELECT ISNULL( MAX( [AL].[log_run_id] ), 0 ) + 1
                                        FROM   [oqs].[activity_log] AS [AL] );

                INSERT INTO [oqs].[activity_log] ( [log_run_id],
                                                   [log_timestamp],
                                                   [log_message] )
                VALUES ( @log_logrunid, GETDATE(), 'OpenQueryStore data capture not activated. Collection skipped' );
            END;

        -- Run regular OQS store cleanup if activated
        IF @data_cleanup_active = 1
            BEGIN
                IF @logmode = 1
                    BEGIN
                        INSERT INTO [oqs].[activity_log] ( [log_run_id],
                                                            [log_timestamp],
                                                            [log_message] )
                        VALUES ( @log_logrunid, GETDATE(), 'OpenQueryStore data cleanup process executed.' );
                    END;

                -- Tidy up time!
                EXEC [oqs].[data_cleanup] @data_cleanup_threshold = @data_cleanup_threshold,
                                            @data_cleanup_throttle = @data_cleanup_throttle;

            END;
    END;