/*
---->> stored procedure >> load data from csv file into sql 
*/

create or alter procedure Bronze.load_bronze as 
BEGIN
 DECLARE @start_time datetime , @end_time datetime  ,@batch_start_time datetime ,@batch_end_time datetime;
	BEGIN TRY
	set @batch_start_time = GETDATE() ;
		print'=================================';
		print'loading bronze layer' ;
		print'=================================';
		
		print'---------------------------------';
		print'Loading CRM Tabels';
		print'---------------------------------';

	set @start_time = GETDATE() ;
		PRINT '>> Truncating Table: bronze.crm_cust_info';
			TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
			BULK INSERT bronze.crm_cust_info 
			from 'D:\SQL_ETL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			with( 
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
				);
	set @end_time = GETDATE();
	print '>> Load Duration : ' + CAST(datediff(second , @start_time , @end_time)AS nvarchar) + 'seconds' ;
	print'>>---------------------';


	set @start_time = GETDATE() ;
		PRINT '>> Truncating Table: bronze.crm_sales_info';
			TRUNCATE TABLE bronze.crm_sales_info;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
			BULK INSERT bronze.crm_sales_info
			from 'D:\SQL_ETL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			with( 
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
				);
	print '>> Load Duration : ' + CAST(datediff(second , @start_time , @end_time)AS nvarchar) + 'seconds' ;
	print'>>---------------------';

	set @start_time = GETDATE() ;
		PRINT '>> Truncating Table: bronze.crm_prd_info';
			TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
			BULK INSERT bronze.crm_prd_info 
			from 'D:\SQL_ETL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			with( 
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
				);
	print '>> Load Duration : ' + CAST(datediff(second , @start_time , @end_time)AS nvarchar) + 'seconds' ;
	print'>>---------------------';

		print'---------------------------------';
		print'Loading ERP Tabels';
		print'---------------------------------';
	set @start_time = GETDATE() ;
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
			TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
			BULK INSERT bronze.erp_cust_az12
			from 'D:\SQL_ETL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			with( 
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
				);
	print '>> Load Duration : ' + CAST(datediff(second , @start_time , @end_time)AS nvarchar) + 'seconds' ;
	print'>>---------------------';


	set @start_time = GETDATE() ;
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
			TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
			BULK INSERT bronze.erp_loc_a101
			from 'D:\SQL_ETL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			with( 
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
				);
	print '>> Load Duration : ' + CAST(datediff(second , @start_time , @end_time)AS nvarchar) + 'seconds' ;
	print'>>---------------------';


	set @start_time = GETDATE() ;
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
			TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
			BULK INSERT bronze.erp_px_cat_g1v2
			from 'D:\SQL_ETL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			with( 
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
				);
	print '>> Load Duration : ' + CAST(datediff(second , @start_time , @end_time)AS nvarchar) + 'seconds' ;
	print'>>---------------------';


	SET @batch_end_time = GETDATE();
	PRINT '=========================================='
	PRINT 'Loading Bronze Layer is Completed';
    PRINT '  >> Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
	PRINT '=========================================='

	END TRY
	BEGIN CATCH 
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH

END 
