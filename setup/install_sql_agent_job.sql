/*********************************************************************************************
Open Query Store
Install centralized mode SQL Agent job for Open Query Store (No schedule is attached to this job)
v0.4 - August 2017

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


USE [msdb];
GO
BEGIN TRANSACTION;
DECLARE @ReturnCode int;
SELECT @ReturnCode = 0;
IF NOT EXISTS (   SELECT [name]
                  FROM   [msdb].[dbo].[syscategories]
                  WHERE  [name] = N'Open Query Store'
                         AND [category_class] = 1
              )
    BEGIN
        EXEC @ReturnCode = [msdb].[dbo].[sp_add_category] @class = N'JOB',
                                                          @type = N'LOCAL',
                                                          @name = N'Open Query Store';
        IF ( @@Error <> 0 OR @ReturnCode <> 0 ) GOTO QuitWithRollback;

    END;

DECLARE @jobId binary (16);
EXEC @ReturnCode = [msdb].[dbo].[sp_add_job] @job_name = N'Open Query Store - Data Collection',
                                             @enabled = 1,
                                             @notify_level_eventlog = 0,
                                             @notify_level_email = 0,
                                             @notify_level_netsend = 0,
                                             @notify_level_page = 0,
                                             @delete_level = 0,
                                             @description = N'Executes the data collection stored procedure for Open Query Store running using "SQL Agent" scheduling.',
                                             @category_name = N'Open Query Store',
                                             @owner_login_name = N'{JobOwner}',
                                             @job_id = @jobId OUTPUT;
IF ( @@Error <> 0 OR @ReturnCode <> 0 ) GOTO QuitWithRollback;
EXEC @ReturnCode = [msdb].[dbo].[sp_add_jobstep] @job_id = @jobId,
                                                 @step_name = N'Execute oqs.gather_statistics',
                                                 @step_id = 1,
                                                 @cmdexec_success_code = 0,
                                                 @on_success_action = 1,
                                                 @on_success_step_id = 0,
                                                 @on_fail_action = 2,
                                                 @on_fail_step_id = 0,
                                                 @retry_attempts = 0,
                                                 @retry_interval = 0,
                                                 @os_run_priority = 0,
                                                 @subsystem = N'TSQL',
                                                 @command = N'EXECUTE [oqs].[gather_statistics] @logmode = 1, @debug = 0',
                                                 @database_name = N'{DatabaseWhereOQSIsRunning}',
                                                 @flags = 0;
IF ( @@Error <> 0 OR @ReturnCode <> 0 ) GOTO QuitWithRollback;
EXEC @ReturnCode = [msdb].[dbo].[sp_update_job] @job_id = @jobId,
                                                @start_step_id = 1;
IF ( @@Error <> 0 OR @ReturnCode <> 0 ) GOTO QuitWithRollback;
EXEC @ReturnCode = [msdb].[dbo].[sp_add_jobserver] @job_id = @jobId,
                                                   @server_name = N'(local)';
IF ( @@Error <> 0 OR @ReturnCode <> 0 ) GOTO QuitWithRollback;
COMMIT TRANSACTION;
GOTO EndSave;
QuitWithRollback:
IF ( @@TranCount > 0 ) ROLLBACK TRANSACTION;
EndSave:
GO

