USE [master];
GO

SET ANSI_NULLS ON;
GO

SET QUOTED_IDENTIFIER ON;
GO

CREATE PROCEDURE [dbo].[OpenQueryStoreStartup]
AS
	-- This stored procedure is used to activate Open Query Store data collection using Service Broker
	-- The procedure is called at every SQL Server startup
    BEGIN
        SET NOCOUNT ON;
        EXEC {DatabaseWhereOQSIsRunning}.[oqs].[StartScheduler];
    END;
GO



EXEC sp_procoption  @ProcName =  'dbo.OpenQueryStoreStartup' ,  @OptionName =  'startup' ,  @OptionValue =  'on' 
