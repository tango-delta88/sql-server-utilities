CREATE OR ALTER PROCEDURE [dbo].[GenerateInsertUpdateScripts]
    @SchemaName NVARCHAR(128),
    @TableName NVARCHAR(128),
    @Filter NVARCHAR(MAX),
	@InsertOnly bit,
	@DeleteExistingRecords bit
AS
BEGIN
    -- Stored procedure to build insert/update/delete statements for the given table to allow the data to be transferred to another database.
	
	SET NOCOUNT ON;

    -- Get the table's columns
    DECLARE @Columns TABLE (ColumnName NVARCHAR(128), DataType NVARCHAR(128));
    INSERT INTO @Columns (ColumnName, DataType)
    SELECT c.COLUMN_NAME, c.DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS c
    WHERE c.TABLE_SCHEMA = @SchemaName AND c.TABLE_NAME = @TableName
    ORDER BY c.ORDINAL_POSITION;

	DECLARE @FullTableName nvarchar(max) = QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName)
	DECLARE @ColumnList nvarchar(max) = (SELECT STRING_AGG(ColumnName, ', ') FROM @Columns)
	DECLARE @InsertUpdateScript nvarchar(max) = ''  
	DECLARE @sql NVARCHAR(MAX);
	DECLARE @ResultTable TABLE (InsertCommaSeparated NVARCHAR(MAX), UpdateCommaSeparated NVARCHAR(MAX), Id uniqueidentifier);
	DECLARE @InsertSelectColumns NVARCHAR(MAX) = '';
	DECLARE @UpdateSelectColumns NVARCHAR(MAX) = '';
		
	-- Validate the input parameters
	PRINT '---------Script to insert or update the records for ' + @FullTableName + ' - START---------'+ CHAR(13)+CHAR(10) ;

	IF (@SchemaName IS NULL)
		THROW 51000, 'The parameter @SchemaName has not been provided.', 1;

	IF (@TableName IS NULL)
		THROW 51000, 'The parameter @TableName has not been provided.', 1;

	IF (NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND  TABLE_NAME = 'ExportedIdsAndImportIds'))
		THROW 51000, 'The table dbo.ExportedIdsAndImportIds does not exist.', 1;

	IF (NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND  TABLE_NAME = 'ExportedIdsAndImportIds'))
		THROW 51000, 'The table dbo.ExportedIdsAndImportIds does not exist.', 1;

	IF (@InsertOnly IS NULL)
		SET @InsertOnly = 0

	IF (@DeleteExistingRecords IS NULL)
		SET @DeleteExistingRecords = 0

	IF (@Filter IS NULL)
		PRINT '-- WARN: The parameter @Filter has not been provided; all records will be replaced'+ CHAR(13)+CHAR(10) + CHAR(13)+CHAR(10) ;

	-- Build expression to convert and concatenate all columns for the insert.
	SELECT @InsertSelectColumns = STRING_AGG(
		'CASE 
			WHEN ' + QUOTENAME(ColumnName) + ' IS NULL THEN ''NULL'' 
			WHEN ''' + DataType + ''' IN (''datetime'', ''datetime2'') THEN '''''''' +  CONVERT(NVARCHAR , ' + QUOTENAME(ColumnName) + ', 121) + ''''''''
			ELSE '''''''' +  REPLACE(CONVERT(NVARCHAR(MAX) , ' + QUOTENAME(ColumnName) + '), '''''''', '''''''''''') + '''''''' END',
		' + '', '' + '
	) 
	FROM @Columns

	-- Build expression to convert and concatenate all columns for the update.
	SELECT @UpdateSelectColumns = STRING_AGG(
		'''' + QUOTENAME(ColumnName) + ' = '' + CASE 
			WHEN ' + QUOTENAME(ColumnName) + ' IS NULL THEN ''NULL'' 
			WHEN ''' + DataType + ''' IN (''datetime'', ''datetime2'') THEN '''''''' +  CONVERT(NVARCHAR , ' + QUOTENAME(ColumnName) + ', 121) + ''''''''
			ELSE '''''''' +  REPLACE(CONVERT(NVARCHAR(MAX) , ' + QUOTENAME(ColumnName) + '), '''''''', '''''''''''') + '''''''' END',
		' + '', '' + '
	) 
	FROM @Columns WHERE ColumnName != 'Id';
	
	-- Build dynamic SQL to get the inserts and updates
	SET @SQL = '
	SELECT ' + @InsertSelectColumns + ' AS InsertCommaSeparatedValues, ' + @UpdateSelectColumns + 
	' AS UpdateCommaSeparatedValues, Id ' +
	'FROM ' + @FullTableName;

	-- Filter the results (if provided).
	-- EXAMPLE 'SELECT Id FROM dbo.Orders where [ImportId] IN (''FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF'',''EEEEEEEE-EEEE-EEEE-EEEE-EEEEEEEEEEEE'')'
	IF @Filter IS NOT NULL
	BEGIN
		SET @SQL += 'WHERE Id IN ('+@Filter+')'
	END

	SET @SQL += ';'
	
	-- Insert results of dynamic SQL into table variable
	INSERT INTO @ResultTable (InsertCommaSeparated, UpdateCommaSeparated, Id)
	EXEC sp_executesql @sql;

	-- Declare a cursor to get the values for the insert and update, along with the row's id
	DECLARE @InsertRowValue NVARCHAR(MAX);
	DECLARE @UpdateRowValue NVARCHAR(MAX);
	DECLARE @IdValue uniqueidentifier;
	
	DECLARE ResultCursor CURSOR FOR
	SELECT InsertCommaSeparated, UpdateCommaSeparated, Id FROM @ResultTable;

	OPEN ResultCursor;

	-- Fetch the first row
	FETCH NEXT FROM ResultCursor INTO @InsertRowValue, @UpdateRowValue, @IdValue;

	-- Loop through the cursor to generate the script.
	WHILE @@FETCH_STATUS = 0
	BEGIN	
		-- Only generate an update statement if InsertOnly is false
		IF 	@InsertOnly = 0 
		BEGIN
			SET @InsertUpdateScript += 'UPDATE ' + @FullTableName + ' SET ' + @UpdateRowValue + 
				' WHERE Id = ''' +  CONVERT(NVARCHAR(MAX) , @IdValue) + '''' + CHAR(13)+CHAR(10) +
				'IF @@ROWCOUNT = 0'+ CHAR(13)+CHAR(10);
		END

		
		-- Ensure the record does not already exist
		IF 	@InsertOnly = 1
		BEGIN
			SET @InsertUpdateScript += 'IF NOT EXISTS (SELECT Id FROM ' + @FullTableName + ' WHERE Id = ''' +  CONVERT(NVARCHAR(MAX) , @IdValue) + ''')' + CHAR(13)+CHAR(10);
		END

		-- Generate an insert statement
		SET @InsertUpdateScript += 
			'BEGIN'+ CHAR(13)+CHAR(10) +
			'	INSERT INTO ' + @FullTableName + ' (' + @ColumnList + ') VALUES (' + @InsertRowValue +')'+ CHAR(13)+CHAR(10) +
			'END' + CHAR(13)+CHAR(10) + CHAR(13)+CHAR(10);

		-- Fetch next
		FETCH NEXT FROM ResultCursor INTO @InsertRowValue, @UpdateRowValue, @IdValue;
	END

	CLOSE ResultCursor;
	DEALLOCATE ResultCursor;
	

	-- Now that we have the script to insert / update the records, replace any ids with the records from the destination database
	-- by cross referencing to the ExportedIdsAndImportIds table.
	DECLARE @OriginalId NVARCHAR(36);
	DECLARE @ReplacementId NVARCHAR(36);

	DECLARE ImportIdsCursor CURSOR FOR
	SELECT Id, ReplacementId FROM [dbo].[ExportedIdsAndImportIds];

	OPEN ImportIdsCursor;

	-- Fetch the first row
	FETCH NEXT FROM ImportIdsCursor INTO @OriginalId, @ReplacementId;

	-- Loop through the cursor
	WHILE @@FETCH_STATUS = 0
	BEGIN

		IF @ReplacementId IS NOT NULL
		BEGIN
			SET @InsertUpdateScript = REPLACE(@InsertUpdateScript, @OriginalId, @ReplacementId)
		END

		-- Fetch next
		FETCH NEXT FROM ImportIdsCursor INTO @OriginalId, @ReplacementId;
	END

	CLOSE ImportIdsCursor;
	DEALLOCATE ImportIdsCursor;


	-- If @DeleteExistingRecords it true, delete the records from the table before we run the insert statements.
	DECLARE @DeleteRecordsScript nvarchar(max) = ''

	IF @DeleteExistingRecords = 1
	BEGIN
		SET @DeleteRecordsScript = 'DELETE FROM ' + @FullTableName;
		IF @Filter IS NOT NULL
		BEGIN
			SET @DeleteRecordsScript += ' WHERE Id IN ('+@Filter+')'
		END
		SET @DeleteRecordsScript += ';' + CHAR(13)+CHAR(10) + CHAR(13)+CHAR(10)
	END

	-- Print the delete/insert/update scripts
	
	PRINT @DeleteRecordsScript;
	PRINT @InsertUpdateScript;
	PRINT '---------Script to insert or update the records for ' + @FullTableName + ' - END---------';

END;