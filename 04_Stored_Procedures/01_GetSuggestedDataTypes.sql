CREATE PROCEDURE dq.usp_GetSuggestedDataTypes ( @DatabaseName nvarchar(128) = '', @SchemaName nvarchar(128) = '', @TableName nvarchar(128) ='' )
AS
BEGIN	
	
	SET NOCOUNT ON

	DECLARE @Cursor AS CURSOR;
	DECLARE @ColumnName AS NVARCHAR(128);
	DECLARE @DataType AS NVARCHAR(128);
	DECLARE @SQLCode AS NVARCHAR(MAX);

	-- Clear any old results
	DELETE FROM dq.SuggestedDataTypes WHERE DatabaseName = @DatabaseName AND SchemaName = @SchemaName AND TableName = @TableName;

	-- Prep table
	SET @SQLCode = 'SELECT
			''' + @DatabaseName + ''' as DatabaseName
			,s.name as SchemaName
			,t.name as TableName
			,c.name as ColumnName
			,case
				when tp.name like ''%char%'' then tp.name + N''('' + case when isnull(c.max_length,0) = -1 then ''max'' else cast(isnull(c.max_length,0) as nvarchar(32)) end + N'')''
				when tp.name like ''%binary%'' then tp.name + N''('' + case when isnull(c.max_length,0) = -1 then ''max'' else cast(isnull(c.max_length,0) as nvarchar(32)) end + N'')''
				when tp.name in (''decimal'', ''numeric'' ) then tp.name + N''('' + cast(isnull(c.precision,0) as nvarchar(32)) + N'','' + cast(isnull(c.scale,0) as nvarchar(32)) + N'')''
				else tp.name
			end as DataType
			,case
				when replace(c.name,''update'','''') like ''%date%'' or c.name like ''%time%'' then 1
				else 0
			end as ColumnNameDateTimeCheck
		FROM
			' + @DatabaseName + '.sys.columns c
			inner join ' + @DatabaseName + '.sys.objects t on c.object_id = t.object_id
			inner join ' + @DatabaseName + '.sys.schemas s on t.schema_id = s.schema_id
			inner join ' + @DatabaseName + '.sys.types tp on c.user_type_id = tp.user_type_id
		WHERE
			s.name = ''' + @SchemaName + '''
			AND t.name = ''' + @TableName + '''';

	INSERT INTO dq.SuggestedDataTypes ( DatabaseName, SchemaName, TableName, ColumnName, DataType, ColumnNameDateTimeCheck )
		EXECUTE sp_executesql @SQLCode;	

	-- Update Total Rows for all columns
	SET @SQLCode = 'UPDATE dq.SuggestedDataTypes SET TotalRows = a.TotalRows FROM ( SELECT COUNT(*) AS TotalRows FROM ' + @DatabaseName + '.' + @SchemaName + '.' + @TableName + ' ) a WHERE DatabaseName = '''  + @DatabaseName + ''' and SchemaName = '''  + @SchemaName + ''' and TableName = '''  + @TableName + '''' 

	EXECUTE sp_executesql @SQLCode;	

	-- Set up cursor
	SET @Cursor = CURSOR FOR
		SELECT ColumnName, DataType FROM dq.SuggestedDataTypes WHERE DatabaseName = @DatabaseName and SchemaName = @SchemaName and TableName = @TableName;

	OPEN @Cursor
	FETCH NEXT FROM @Cursor INTO @ColumnName, @DataType;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		
		-- Null Count
		SET @SQLCode = 'UPDATE dq.SuggestedDataTypes SET NullRows = a.NullRows FROM (
			SELECT
				COUNT(*) AS NullRows
			FROM ' + @DatabaseName + '.' + @SchemaName + '.' + @TableName + '
			WHERE ' + @ColumnName +' IS NULL
		) a WHERE DatabaseName = '''  + @DatabaseName + ''' and SchemaName = '''  + @SchemaName + ''' and TableName = '''  + @TableName + ''' and ColumnName = ''' + @ColumnName + '''';

		PRINT @SQLCode;

		EXECUTE sp_executesql @SQLCode;

		IF ( @DataType NOT IN ( 'uniqueidentifier', 'image', 'ntext', 'text', 'xml', 'CLR UDT', 'hierarchyid' ) )
		BEGIN

			-- Date Stats
			SET @SQLCode = 'UPDATE dq.SuggestedDataTypes SET DateRows = a.DateRows, DifferentDates = a.DifferentDates, DifferentTimes = a.DifferentTimes, YearsOutside100Years = a.YearsOutside100Years, AvgDateSeparators = a.AvgDateSeparators FROM (
				SELECT
					COUNT(' + @ColumnName + ') AS DateRows
					,COUNT(DISTINCT CAST(CAST(' + @ColumnName + ' AS DATETIME) AS TIME)) AS DifferentTimes
					,COUNT(DISTINCT CAST(CAST(' + @ColumnName + ' AS DATETIME) AS DATE)) AS DifferentDates
					,AVG(adjText.SepCount) AS AvgDateSeparators
					,COUNT(CASE WHEN YEAR(CAST(' + @ColumnName + ' AS DATETIME)) < YEAR(DATEADD(YEAR, -50, GETDATE())) OR YEAR(CAST(' + @ColumnName + ' AS DATETIME)) > YEAR(DATEADD(YEAR, 50, GETDATE())) THEN CAST(' + @ColumnName + ' AS DATETIME) ELSE NULL END) AS YearsOutside100Years
				FROM ' + @DatabaseName + '.' + @SchemaName + '.' + @TableName + '
					OUTER APPLY (
						SELECT
							LEN(CAST(' + @ColumnName + ' AS VARCHAR(100))) - LEN(REPLACE(REPLACE(REPLACE(CAST(' + @ColumnName + ' AS VARCHAR(100)),'':'',''''),''-'',''''),''/'','''')) AS SepCount

					) adjText
				WHERE
					TRY_CONVERT(DATETIME,' + @ColumnName + ') IS NOT NULL
					AND CAST(' + @ColumnName + ' AS VARCHAR(8000)) != ''''
			) a WHERE DatabaseName = '''  + @DatabaseName + ''' and SchemaName = '''  + @SchemaName + ''' and TableName = '''  + @TableName + ''' and ColumnName = ''' + @ColumnName + '''';

			PRINT @SQLCode;

			EXECUTE sp_executesql @SQLCode;

		END;

		IF ( @DataType NOT IN ( 'date', 'time', 'binary', 'varbinary', 'datetimeoffset', 'datetime2', 'timestamp', 'uniqueidentifier', 'image', 'ntext', 'text', 'xml', 'CLR UDT', 'hierarchyid' ) )
		BEGIN

			-- Numeric Stats
			SET @SQLCode = 'UPDATE dq.SuggestedDataTypes SET NumericRows = a.NumericRows, BitRows = a.BitRows, MaxPlacesLeft = a.MaxNonDecimalPlaces, MaxPlacesRight = a.MaxDecimalPlaces FROM (
				SELECT
					COUNT(' + @ColumnName + ') AS NumericRows
					,COUNT(CASE WHEN CAST(' + @ColumnName + ' AS FLOAT) IN ( 0, 1 ) THEN ' + @ColumnName + ' ELSE NULL END) AS BitRows
					,ISNULL(MAX(CASE WHEN CHARINDEX(''.'',adjValue.TrimmedNumeric) = 0 THEN LEN(adjValue.TrimmedNumeric) ELSE CHARINDEX(''.'',adjValue.TrimmedNumeric) END),0) AS MaxNonDecimalPlaces
					,ISNULL(MAX(CASE WHEN CHARINDEX(''.'',adjValue.TrimmedNumeric) = 0 THEN 0 ELSE LEN(adjValue.TrimmedNumeric) - CHARINDEX(''.'',adjValue.TrimmedNumeric) END),0) AS MaxDecimalPlaces
				FROM ' + @DatabaseName + '.' + @SchemaName + '.' + @TableName + '
					OUTER APPLY (
						SELECT
							CASE
								WHEN PATINDEX(''%[1-9]%'',CAST(' + @ColumnName + ' AS VARCHAR(40))) = 0 THEN ''0''
								ELSE SUBSTRING(CAST(' + @ColumnName + ' AS VARCHAR(40)),CASE
									WHEN PATINDEX(''%[\.]%'',CAST(' + @ColumnName + ' AS VARCHAR(40))) = 0 OR PATINDEX(''%[1-9-]%'',CAST(' + @ColumnName + ' AS VARCHAR(40))) < PATINDEX(''%[\.]%'',CAST(' + @ColumnName + ' AS VARCHAR(40))) THEN PATINDEX(''%[1-9-]%'',CAST(' + @ColumnName + ' AS VARCHAR(40)))
									ELSE PATINDEX(''%[\.]%'',CAST(' + @ColumnName + ' AS VARCHAR(40))) - 1
								END,LEN(CAST(' + @ColumnName + ' AS VARCHAR(40))) - CASE
									WHEN PATINDEX(''%[\.]%'',CAST(' + @ColumnName + ' AS VARCHAR(40))) = 0 THEN PATINDEX(''%[1-9-]%'',CAST(' + @ColumnName + ' AS VARCHAR(40)))
									WHEN PATINDEX(''%[1-9-]%'',REVERSE(CAST(' + @ColumnName + ' AS VARCHAR(40)))) > PATINDEX(''%[\.]%'',REVERSE(CAST(' + @ColumnName + ' AS VARCHAR(40)))) THEN PATINDEX(''%[\.]%'',REVERSE(CAST(' + @ColumnName + ' AS VARCHAR(40)))) + 1
									ELSE PATINDEX(''%[1-9-]%'',REVERSE(CAST(' + @ColumnName + ' AS VARCHAR(40))))
								END + 1)
							END AS TrimmedNumeric
					) adjValue
				WHERE
					TRY_CONVERT(FLOAT,' + @ColumnName + ') IS NOT NULL
					AND CAST(' + @ColumnName + ' AS VARCHAR(8000)) != ''''
			) a WHERE DatabaseName = '''  + @DatabaseName + ''' and SchemaName = '''  + @SchemaName + ''' and TableName = '''  + @TableName + ''' and ColumnName = ''' + @ColumnName + '''';

			PRINT @SQLCode;

			EXECUTE sp_executesql @SQLCode;

			-- Int Stats
			SET @SQLCode = 'UPDATE dq.SuggestedDataTypes SET NoDecimalRows = a.HasNoDecimals, MaxIntValue = a.MaxIntValue, MinIntValue = a.MinIntValue, AvgIntValue = a.AvgIntValue, StdDevIntValue = a.StdDevIntValue FROM (
				SELECT
					COUNT(CASE WHEN CAST(adjValue.TrimmedNumeric AS FLOAT) = CAST(adjValue.TrimmedNumeric AS BIGINT) THEN adjValue.TrimmedNumeric ELSE NULL END) AS HasNoDecimals
					,ISNULL(MAX(CAST(adjValue.TrimmedNumeric AS BIGINT)),0) AS MaxIntValue
					,ISNULL(MIN(CAST(adjValue.TrimmedNumeric AS BIGINT)),0) AS MinIntValue
					,CASE WHEN TRY_CONVERT(BIGINT,ISNULL(AVG(CAST(adjValue.TrimmedNumeric AS FLOAT)),0)) IS NULL THEN 0 ELSE ISNULL(CAST(AVG(CAST(adjValue.TrimmedNumeric AS FLOAT)) AS BIGINT),0) END AS AvgIntValue
					,CASE WHEN TRY_CONVERT(BIGINT,ISNULL(STDEV(CAST(adjValue.TrimmedNumeric AS FLOAT)),0)) IS NULL THEN 0 ELSE ISNULL(CAST(STDEV(CAST(adjValue.TrimmedNumeric AS FLOAT)) AS BIGINT),0) END AS StdDevIntValue
				FROM ' + @DatabaseName + '.' + @SchemaName + '.' + @TableName + '
					OUTER APPLY (
						SELECT
							CASE
								WHEN PATINDEX(''%[1-9]%'',CAST(' + @ColumnName + ' AS VARCHAR(40))) = 0 THEN ''0''
								ELSE SUBSTRING(CAST(' + @ColumnName + ' AS VARCHAR(40)),CASE
									WHEN PATINDEX(''%[\.]%'',CAST(' + @ColumnName + ' AS VARCHAR(40))) = 0 OR PATINDEX(''%[1-9-]%'',CAST(' + @ColumnName + ' AS VARCHAR(40))) < PATINDEX(''%[\.]%'',CAST(' + @ColumnName + ' AS VARCHAR(40))) THEN PATINDEX(''%[1-9-]%'',CAST(' + @ColumnName + ' AS VARCHAR(40)))
									ELSE PATINDEX(''%[\.]%'',CAST(' + @ColumnName + ' AS VARCHAR(40))) - 1
								END,LEN(CAST(' + @ColumnName + ' AS VARCHAR(40))) - CASE
									WHEN PATINDEX(''%[\.]%'',CAST(' + @ColumnName + ' AS VARCHAR(40))) = 0 THEN PATINDEX(''%[1-9-]%'',CAST(' + @ColumnName + ' AS VARCHAR(40)))
									WHEN PATINDEX(''%[1-9-]%'',REVERSE(CAST(' + @ColumnName + ' AS VARCHAR(40)))) > PATINDEX(''%[\.]%'',REVERSE(CAST(' + @ColumnName + ' AS VARCHAR(40)))) THEN PATINDEX(''%[\.]%'',REVERSE(CAST(' + @ColumnName + ' AS VARCHAR(40)))) + 1
									ELSE PATINDEX(''%[1-9-]%'',REVERSE(CAST(' + @ColumnName + ' AS VARCHAR(40))))
								END + 1)
							END AS TrimmedNumeric
					) adjValue
				WHERE
					TRY_CONVERT(FLOAT,' + @ColumnName + ') IS NOT NULL 
					AND TRY_CONVERT(BIGINT,adjValue.TrimmedNumeric) IS NOT NULL
					AND CAST(' + @ColumnName + ' AS VARCHAR(8000)) != ''''
			) a WHERE DatabaseName = '''  + @DatabaseName + ''' and SchemaName = '''  + @SchemaName + ''' and TableName = '''  + @TableName + ''' and ColumnName = ''' + @ColumnName + '''';

			PRINT @SQLCode;

			EXECUTE sp_executesql @SQLCode;

		END;

		IF ( @DataType NOT IN ( 'image') )
		BEGIN

			-- Text Stats
			SET @SQLCode = 'UPDATE dq.SuggestedDataTypes SET NonUnicodeCharacterRows = a.ValuesWithoutUnicodeChars, MaxLenValue = a.MaxChars, MinLenValue = a.MinChars, AvgLenValue = a.AvgChars, StdDevLenValue = a.StdDevChars FROM (
				SELECT
					COUNT(' + @ColumnName + ') AS TotalValues
					,COUNT(CASE WHEN CAST(LEFT(' + @ColumnName + ',4000) AS VARCHAR(4000)) = CAST(LEFT(' + @ColumnName + ',4000) AS NVARCHAR(4000)) THEN ' + @ColumnName + ' ELSE NULL END) AS ValuesWithoutUnicodeChars
					,ISNULL(MAX(LEN(' + @ColumnName + ')),0) AS MaxChars
					,ISNULL(MIN(LEN(' + @ColumnName + ')),0) AS MinChars
					,ISNULL(AVG(LEN(' + @ColumnName + ')),0) AS AvgChars
					,ISNULL(ROUND(STDEV(LEN(' + @ColumnName + ')),0),0) AS StdDevChars
				FROM ' + @DatabaseName + '.' + @SchemaName + '.' + @TableName + '
				WHERE
					' + @ColumnName + ' IS NOT NULL
			) a WHERE DatabaseName = '''  + @DatabaseName + ''' and SchemaName = '''  + @SchemaName + ''' and TableName = '''  + @TableName + ''' and ColumnName = ''' + @ColumnName + '''';

			PRINT @SQLCode;

			EXECUTE sp_executesql @SQLCode;

		END;

		FETCH NEXT FROM @Cursor INTO @ColumnName, @DataType;
	END

	CLOSE @Cursor;
	DEALLOCATE @Cursor;

	-- Determine Suggested Data Type
	UPDATE dq.SuggestedDataTypes
		SET SuggestedDataType = CASE
			WHEN sdt.NullRows = sdt.TotalRows THEN 'N/A'
			WHEN sdt.DateRows + sdt.NullRows = sdt.TotalRows AND (
				sdt.NumericRows + sdt.NullRows < sdt.TotalRows
				OR ( sdt.NumericRows + sdt.NullRows = sdt.TotalRows AND ( sdt.YearsOutside100Years = 0 OR sdt.AvgDateSeparators > 1 OR sdt.ColumnNameDateTimeCheck = 1 ) )
			) THEN CASE
				WHEN sdt.DifferentTimes = 1 THEN 'date'
				WHEN sdt.DifferentDates = 1 THEN 'time'
				ELSE 'datetime'
			END
			WHEN sdt.NumericRows + sdt.NullRows = sdt.TotalRows THEN CASE
				WHEN sdt.BitRows = sdt.NumericRows THEN 'bit'
				WHEN sdt.NoDecimalRows = sdt.NumericRows THEN CASE
					WHEN sdt.MinIntValue >= 0 AND sdt.MaxIntValue <= 255 THEN 'tinyint'
					WHEN sdt.MinIntValue >= -32768 AND sdt.MaxIntValue <= 32767 THEN 'smallint'
					WHEN sdt.MinIntValue >= -2147483648 AND sdt.MaxIntValue <= 2147483674 THEN 'int'
					WHEN sdt.MinIntValue >= -9223372036854775808 AND sdt.MaxIntValue <= 9223372036854775807 THEN 'bigint'
					ELSE 'float'
				END
				WHEN sdt.MaxPlacesLeft + sdt.MaxPlacesRight <= 38 THEN 'decimal(' + CAST( sdt.MaxPlacesLeft + sdt.MaxPlacesRight AS varchar(2)) + ',' + CAST(sdt.MaxPlacesRight AS VARCHAR(2)) + ')'
				ELSE 'float'
			END
			WHEN sdt.MaxLenValue = 0 AND sdt.MinLenValue = 0 THEN 'N/A'
			ELSE CASE
				WHEN sdt.NonUnicodeCharacterRows + sdt.NullRows = sdt.TotalRows OR sl.TotalLen > 4000 THEN CASE
					WHEN sdt.MaxLenValue = sdt.MinLenValue OR CAST(sdt.MaxLenValue AS FLOAT) <= ( CAST(sdt.AvgLenValue AS FLOAT) * 1.25 ) THEN 'char(' + CAST(sl.TotalLen AS NVARCHAR(4)) + ')'
					ELSE 'varchar(' + CAST(sl.AdvisedLen AS NVARCHAR(4)) + ')'
				END
				ELSE CASE
					WHEN sdt.MaxLenValue = sdt.MinLenValue OR CAST(sdt.MaxLenValue AS FLOAT) <= ( CAST(sdt.AvgLenValue AS FLOAT) * 1.25 ) THEN 'nchar(' + CAST(sl.TotalLen AS NVARCHAR(4)) + ')'
					ELSE 'nvarchar(' + CAST(sl.AdvisedLen AS NVARCHAR(4)) + ')'
				END
			END
		END
	FROM
		dq.SuggestedDataTypes sdt
		OUTER APPLY (
			SELECT
				sdt.MaxLenValue + sdt.StdDevLenValue AS TotalLen
				,CASE
					WHEN sdt.MaxLenValue + sdt.StdDevLenValue < 11 THEN sdt.MaxLenValue + sdt.StdDevLenValue
					WHEN sdt.MaxLenValue + sdt.StdDevLenValue BETWEEN 11 AND 15 THEN 15
					WHEN sdt.MaxLenValue + sdt.StdDevLenValue BETWEEN 16 AND 20 THEN 20
					WHEN sdt.MaxLenValue + sdt.StdDevLenValue BETWEEN 21 AND 25 THEN 25
					WHEN sdt.MaxLenValue + sdt.StdDevLenValue BETWEEN 25 AND 30 THEN 30
					WHEN sdt.MaxLenValue + sdt.StdDevLenValue BETWEEN 31 AND 40 THEN 40
					WHEN sdt.MaxLenValue + sdt.StdDevLenValue BETWEEN 41 AND 50 THEN 50
					WHEN sdt.MaxLenValue + sdt.StdDevLenValue BETWEEN 51 AND 75 THEN 75
					WHEN sdt.MaxLenValue + sdt.StdDevLenValue BETWEEN 76 AND 100 THEN 100
					WHEN sdt.MaxLenValue + sdt.StdDevLenValue BETWEEN 100 AND 125 THEN 125
					WHEN sdt.MaxLenValue + sdt.StdDevLenValue BETWEEN 126 AND 150 THEN 150
					WHEN sdt.MaxLenValue + sdt.StdDevLenValue BETWEEN 151 AND 200 THEN 200
					WHEN sdt.MaxLenValue + sdt.StdDevLenValue BETWEEN 201 AND 250 THEN 250
					WHEN sdt.MaxLenValue + sdt.StdDevLenValue BETWEEN 250 AND 1000 THEN ROUND(sdt.MaxLenValue + sdt.StdDevLenValue,-2)
					ELSE ROUND(sdt.MaxLenValue + sdt.StdDevLenValue,-3)
				END AS AdvisedLen
		) sl
	WHERE sdt.DatabaseName = @DatabaseName AND sdt.SchemaName = @SchemaName AND sdt.TableName = @TableName

	RETURN @@ERROR

END

GO