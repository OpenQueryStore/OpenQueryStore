/*
Make sure you have a database named OpenQueryStore
before running this script
*/

USE [OpenQueryStore]
GO

-- Create the intervals table
CREATE TABLE [dbo].[OQS_Intervals](
	[Interval_id] [int] IDENTITY(1,1) NOT NULL,
	[Interval_start] [datetime] NULL,
	[Interval_end] [datetime] NULL
) ON [PRIMARY]
GO

-- Create plans table
CREATE TABLE [dbo].[OQS_Plans](
	[plan_id] [int] IDENTITY(1,1) NOT NULL,
	[plan_handle] [varbinary](64) NULL,
	[plan_firstfound] [datetime] NULL,
	[plan_database] [nvarchar](150) NULL,
	[plan_refcounts] [int] NULL,
	[plan_usecounts] [int] NULL,
	[plan_sizeinbytes] [int] NULL,
	[plan_type] [nvarchar](50) NULL,
	[plan_objecttype] [nvarchar](20) NULL,
	[plan_executionplan] [xml] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

-- Create the queries table
CREATE TABLE [dbo].[OQS_Queries](
	[query_id] [int] IDENTITY(1,1) NOT NULL,
	[plan_id] [int] NOT NULL,
	[query_hash] [binary](8) NULL,
	[query_statement_text] [nvarchar](max) NULL,
	[query_statement_start_offset] [int] NULL,
	[query_statement_end_offset] [int] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

-- Create the Runtime Statistics table
CREATE TABLE [dbo].[OQS_Query_Runtime_Stats](
	[query_id] [int] NULL,
	[interval_id] [int] NULL,
	[creation_time] [datetime] NULL,
	[last_execution_time] [datetime] NULL,
	[execution_count] [bigint] NULL,
	[total_elapsed_time] [bigint] NULL,
	[last_elapsed_time] [bigint] NULL,
	[min_elapsed_time] [bigint] NULL,
	[max_elapsed_time] [bigint] NULL,
	[avg_elapsed_time] [bigint] NULL,
	[total_rows] [bigint] NULL,
	[last_rows] [bigint] NULL,
	[min_rows] [bigint] NULL,
	[max_rows] [bigint] NULL,
	[avg_rows] [bigint] NULL,
	[total_worker_time] [bigint] NULL,
	[last_worker_time] [bigint] NULL,
	[min_worker_time] [bigint] NULL,
	[max_worker_time] [bigint] NULL,
	[avg_worker_time] [bigint] NULL,
	[total_physical_reads] [bigint] NULL,
	[last_physical_reads] [bigint] NULL,
	[min_physical_reads] [bigint] NULL,
	[max_physical_reads] [bigint] NULL,
	[avg_physical_reads] [bigint] NULL,
	[total_logical_reads] [bigint] NULL,
	[last_logical_reads] [bigint] NULL,
	[min_logical_reads] [bigint] NULL,
	[max_logical_reads] [bigint] NULL,
	[avg_logical_reads] [bigint] NULL,
	[total_logical_writes] [bigint] NULL,
	[last_logical_writes] [bigint] NULL,
	[min_logical_writes] [bigint] NULL,
	[max_logical_writes] [bigint] NULL,
	[avg_logical_writes] [bigint] NULL
) ON [PRIMARY]
GO

-- Create logging table
CREATE TABLE [dbo].[OQS_Log](
	[Log_LogID] [int] IDENTITY(1,1) NOT NULL,
	[Log_LogRunID] [int] NULL,
	[Log_DateTime] [datetime] NULL,
	[Log_Message] [varchar](250) NULL
) ON [PRIMARY]
GO


