-- This will remove all traces of an OQS Classic installation


-- Connect to the database where OQS Classic has been installed
USE {DatabaseWhereOQSIsRunning}
GO

-- Now perform the nuke process

IF EXISTS (   SELECT *
                FROM [sys].[services] AS [S]
               WHERE [S].[name] = 'OQSService')
BEGIN
    DROP SERVICE [OQSService];
END;

IF EXISTS (   SELECT *
                FROM [sys].[service_queues] AS [SQ]
               WHERE [SQ].[name] = 'OQSScheduler')
BEGIN
    DROP QUEUE [oqs].[OQSScheduler];
END;

IF EXISTS (   SELECT *
                FROM [sys].[procedures] AS [P]
               WHERE [P].[object_id] = OBJECT_ID(N'[oqs].[StopScheduler]'))
BEGIN
    DROP PROC [oqs].[StopScheduler];
END;

IF EXISTS (   SELECT *
                FROM [sys].[procedures] AS [P]
               WHERE [P].[object_id] = OBJECT_ID(N'[oqs].[StartScheduler]'))
BEGIN
    DROP PROC [oqs].[StartScheduler];
END;

IF EXISTS (   SELECT *
                FROM [sys].[procedures] AS [P]
               WHERE [P].[object_id] = OBJECT_ID(N'[oqs].[GatherStatistics]'))
BEGIN
    DROP PROC [oqs].[GatherStatistics];
END;

IF EXISTS (   SELECT *
                FROM [sys].[procedures] AS [P]
               WHERE [P].[object_id] = OBJECT_ID(N'[oqs].[ActivateOQSScheduler]'))
BEGIN
    DROP PROC [oqs].[ActivateOQSScheduler];
END;

IF EXISTS (   SELECT *
                FROM [sys].[procedures] AS [P]
               WHERE [P].[object_id] = OBJECT_ID(N'[oqs].[PurgeOQS]'))
BEGIN
    DROP PROC [oqs].[PurgeOQS];
END;

IF EXISTS (   SELECT *
                FROM [sys].[tables] AS [T]
               WHERE [T].[object_id] = OBJECT_ID(N'[oqs].[CollectionMetaData]'))
BEGIN
    DROP TABLE [oqs].[CollectionMetaData];
END;

IF EXISTS (   SELECT *
                FROM [sys].[tables] AS [T]
               WHERE [T].[object_id] = OBJECT_ID(N'[oqs].[Intervals]'))
BEGIN
    DROP TABLE [oqs].[Intervals];
END;

IF EXISTS (   SELECT *
                FROM [sys].[tables] AS [T]
               WHERE [T].[object_id] = OBJECT_ID(N'[oqs].[Plans]'))
BEGIN
    DROP TABLE [oqs].[Plans];
END;

IF EXISTS (   SELECT *
                FROM [sys].[tables] AS [T]
               WHERE [T].[object_id] = OBJECT_ID(N'[oqs].[Queries]'))
BEGIN
    DROP TABLE [oqs].[Queries];
END;

IF EXISTS (   SELECT *
                FROM [sys].[tables] AS [T]
               WHERE [T].[object_id] = OBJECT_ID(N'[oqs].[QueryRuntimeStats]'))
BEGIN
    DROP TABLE [oqs].[QueryRuntimeStats];
END;

IF EXISTS (   SELECT *
                FROM [sys].[tables] AS [T]
               WHERE [T].[object_id] = OBJECT_ID(N'[oqs].[ActivityLog]'))
BEGIN
    DROP TABLE [oqs].[ActivityLog];
END;

IF EXISTS (   SELECT *
                FROM [sys].[tables] AS [T]
               WHERE [T].[object_id] = OBJECT_ID(N'[oqs].[PlanDBID]'))
BEGIN
    DROP TABLE [oqs].[PlanDBID];
END;

IF EXISTS (   SELECT *
                FROM [sys].[views] AS [V]
               WHERE [V].[object_id] = OBJECT_ID(N'[oqs].[QueryStats]'))
BEGIN
    DROP VIEW [oqs].[QueryStats];
END;

IF EXISTS (   SELECT *
                FROM [sys].[server_principals] AS [SP]
               WHERE [SP].[name] = 'OpenQueryStore')
BEGIN
    DROP LOGIN [OpenQueryStore];
END;

IF EXISTS (   SELECT *
                FROM [sys].[certificates] AS [C]
               WHERE [C].[name] = 'OpenQueryStore')
BEGIN
    DROP CERTIFICATE [OpenQueryStore];
END;

IF EXISTS (   SELECT * 
                FROM [sys].[schemas] AS [S] 
               WHERE [S].[name] = 'oqs')
BEGIN
    EXEC ('DROP SCHEMA oqs');
END;
