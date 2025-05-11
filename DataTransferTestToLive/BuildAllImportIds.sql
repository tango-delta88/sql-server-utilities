CREATE OR ALTER PROCEDURE [dbo].[BuildAllImportIds]
    @BlacklistTables NVARCHAR(MAX)
AS
BEGIN
    -- Create a temp table to hold the insert script lines
    DROP TABLE IF EXISTS #InsertScripts;
    CREATE TABLE #InsertScripts (
        ScriptLine NVARCHAR(MAX)
    );

    DECLARE @TableName NVARCHAR(128);
    DECLARE @InsertScript NVARCHAR(4000);
    DECLARE @SQL NVARCHAR(MAX);

    -- Get tables that have both Id and ImportId columns as a cursor
    DECLARE tableCursor CURSOR FOR
    SELECT QUOTENAME(s.name) + '.' + QUOTENAME(t.name)
    FROM sys.tables t
    INNER JOIN sys.schemas s on s.schema_id = t.schema_id
    INNER JOIN sys.columns c1 ON c1.object_id = t.object_id AND c1.name = 'Id'
    INNER JOIN sys.columns c2 ON c2.object_id = t.object_id AND c2.name = 'ImportId'
    WHERE QUOTENAME(s.name) + '.' + QUOTENAME(t.name) NOT IN (SELECT VALUE FROM STRING_SPLIT( @BlacklistTables, ','));

    -- Loop through the tables found and generate insert statements for each row
    OPEN tableCursor;
    FETCH NEXT FROM tableCursor INTO @TableName;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @SQL = '
            INSERT INTO #InsertScripts (ScriptLine)
            SELECT 
                ''INSERT INTO [dbo].[ExportedIdsAndImportIds] (Id, ImportId, ReplacementId, TableName) VALUES ('''''' + 
                CAST(Id AS NVARCHAR(36)) + '''''', '''''' + 
                CAST(ImportId AS NVARCHAR(36)) + '''''', (SELECT Id FROM '' + @TableName + '' WHERE ImportId = '''''' +  CAST(ImportId AS NVARCHAR(36)) + ''''''), '''''' + 
			    ''' + @TableName + ''' + '''''');''
            FROM ' + @TableName + '
            WHERE Id IS NOT NULL AND ImportId IS NOT NULL;
        ';

        EXEC sp_executesql @SQL, N'@TableName NVARCHAR(128)', @TableName = @TableName;

        FETCH NEXT FROM tableCursor INTO @TableName;
    END

    CLOSE tableCursor;
    DEALLOCATE tableCursor;

    -- Print a script to drop and create the table
    PRINT '
    ---------Script to create and populate [dbo].[ExportedIdsAndImportIds] - START---------

    DROP TABLE IF EXISTS [dbo].[ExportedIdsAndImportIds];

    CREATE TABLE [dbo].[ExportedIdsAndImportIds](
	    [Id] [uniqueidentifier] NOT NULL,
	    [ImportId] [uniqueidentifier] NOT NULL,
	    [ReplacementId] [uniqueidentifier] NULL,
	    [TableName] [nvarchar](4000) NOT NULL,
    PRIMARY KEY CLUSTERED 
    (
	    [Id] ASC
    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
    ) ON [PRIMARY];

    '

    -- Print a script to insert the records
    DECLARE scriptCursor CURSOR FOR
    SELECT ScriptLine FROM #InsertScripts;

    OPEN scriptCursor;
    FETCH NEXT FROM scriptCursor INTO @InsertScript;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        PRINT @InsertScript
        FETCH NEXT FROM scriptCursor INTO @InsertScript;
    END

    CLOSE scriptCursor;
    DEALLOCATE scriptCursor;

    PRINT '
    ---------Script to create and populate [dbo].[ExportedIdsAndImportIds] - END---------';

END