/* 
This stored procedure perform ETL process to silver layer
usage :
  exec Silver.load_silver
*/
-- create stored procedure 
create or alter procedure silver.load_silver AS
BEGIN 
 DECLARE @start_time datetime , @end_time datetime  ,@batch_start_time datetime ,@batch_end_time datetime;
	BEGIN TRY
-- load data 
	set @batch_start_time = GETDATE() ;
PRINT'-------------------------------------------------'
PRINT'********* Load Silver Layer'
PRINT'-------------------------------------------------'
PRINT'--------------------------------'
print '>> Load CRM Tabels'
PRINT'--------------------------------'
	set @start_time = GETDATE() ;
		print '>> TRUNCATE Silver.crm_cust_info'
		truncate table Silver.crm_cust_info;
		print '>> INSERT INTO >>Silver.crm_cust_info'
		insert into Silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date 
			)
		select 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname ,
		TRIM(cst_lastname) AS cst_lastname ,
		case when upper(TRIM(cst_marital_status)) = 'S' then 'Single' 
			 when upper(TRIM(cst_marital_status)) = 'M' then 'Married' 
			 else 'n/a'
		end cst_marital_status ,
		case when upper(TRIM(cst_gndr)) = 'F' then 'Female' 
			 when upper(TRIM(cst_gndr)) = 'M' then 'Male' 
			 else 'n/a'
		end cst_gndr ,
		cst_create_date
		from(
		select *,
			ROW_NUMBER() over (PARTITION BY cst_id ORDER BY cst_create_date DESC ) as flag_Last 
		from Bronze.crm_cust_info 
		)t where flag_Last = 1
	set @end_time = GETDATE();
	print '>> Load Duration : ' + CAST(datediff(second , @start_time , @end_time)AS nvarchar) + 'seconds' ;
	print'>>---------------------';

		--
	set @start_time = GETDATE() ;
		print '>> TRUNCATE Silver.crm_prd_info'
		truncate table Silver.crm_prd_info;
		print '>> INSERT INTO >>Silver.crm_prd_info'
		insert into  Silver.crm_prd_info(
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt)
		select 
			prd_id ,
			replace(SUBSTRING(prd_key , 1 ,5 ),'-' ,'_') as cat_id ,
			SUBSTRING(prd_key , 7 , len(prd_key)) as prd_key ,
			prd_nm,
			ISNULL(prd_cost ,0) as prd_cost,
			case UPPER(trim(prd_line))
				 when   'M' then 'Mountain'
				 when	'R' then 'Road'
				 when   'S' then 'Other Sales'
				 when   'T' then 'Touring'
				 else 'n/a'
			End prd_line ,

			 cast(prd_start_dt as date ) as prd_start_dt,
			 cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) -1 as date)As prd_end_dt
		from Bronze.crm_prd_info
	set @end_time = GETDATE();
	print '>> Load Duration : ' + CAST(datediff(second , @start_time , @end_time)AS nvarchar) + 'seconds' ;
	print'>>---------------------';

		--
	set @start_time = GETDATE() ;
		print '>> TRUNCATE Silver.crm_sales_info'
		truncate table Silver.crm_sales_info;
		print '>> INSERT INTO >>Silver.crm_sales_info'
		insert into Silver.crm_sales_info(
			sls_ord_num,
			sls_prd_key ,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_quantity,
			sls_price,
			sls_sales
			)
		select 
			sls_ord_num,
			sls_prd_key ,
			sls_cust_id,
			--order date 
			case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null 
				else CAST(CAST(sls_order_dt as varchar) as date)
			END AS sls_order_dt ,
			--ship date 
			case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null 
				else CAST(CAST(sls_ship_dt as varchar) as date)
			END AS sls_ship_dt ,
			-- due date 
			case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null 
				else CAST(CAST(sls_due_dt as varchar) as date)
			END AS sls_due_dt ,

			sls_quantity,
			--sls_sales
			case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
				 then  sls_quantity * abs(sls_price)
				 else sls_sales
			END as sls_sales ,  
			--sls_price
				case when sls_sales is null or sls_sales <= 0 
				 then  abs(sls_price)
				 else sls_price  / nullif(sls_quantity , 0)
			END as sls_price  

		from Bronze.crm_sales_info

	set @end_time = GETDATE();
	print '>> Load Duration : ' + CAST(datediff(second , @start_time , @end_time)AS nvarchar) + 'seconds' ;
	print'>>---------------------';

		-- load erp 
	PRINT'--------------------------------'
		print '>> Load ERP Tabels'
	PRINT'--------------------------------'
	set @start_time = GETDATE() ;
		print '>> TRUNCATE Silver.erp_cust_az12'
		truncate table Silver.erp_cust_az12;
		print '>> INSERT INTO >>Silver.erp_cust_az12'
		insert into Silver.erp_cust_az12(
			cid , 
			bdate,
			gen
			)

		select 
		--cid
			case when cid like 'NAS%' then SUBSTRING(cid , 4 , len(cid)) 
				else cid 
			end AS cid ,
		--bdate
			case when bdate > GETDATE() then null
			else bdate
			end as bdate ,

		--gen
			case when upper(trim(gen)) in ('Male' , 'M') then 'Male'
				 when upper(trim(gen)) in ('Female' , 'F') then 'Female'
			Else 'n/a'
			end as gen

		from bronze.erp_cust_az12

	set @end_time = GETDATE();
	print '>> Load Duration : ' + CAST(datediff(second , @start_time , @end_time)AS nvarchar) + 'seconds' ;
	print'>>---------------------';


		--
	set @start_time = GETDATE() ;
		print '>> TRUNCATE silver.erp_loc_a101'
		truncate table silver.erp_loc_a101;
		print '>> INSERT INTO >>silver.erp_loc_a101'
		insert into silver.erp_loc_a101(
			cid ,
			cntry )
		select 
			REPLACE(cid , '-','')  cid ,
			case when TRIM(cntry) = 'DE' THEN 'Germany'
			 when trim(cntry) in ('US','USA') THEN 'United States'
			 when trim(cntry) = '' or cntry is null then 'n/a'
			 else TRIM(cntry)
		end as cntry
		from  Bronze.erp_loc_a101
	set @end_time = GETDATE();
	print '>> Load Duration : ' + CAST(datediff(second , @start_time , @end_time)AS nvarchar) + 'seconds' ;
	print'>>---------------------';

		--
	set @start_time = GETDATE() ;
		print '>> TRUNCATE Silver.erp_px_cat_g1v2'
		truncate table Silver.erp_px_cat_g1v2 ;
		print '>> INSERT INTO >>Silver.erp_px_cat_g1v2'
		insert into Silver.erp_px_cat_g1v2(
			id , 
			cat,
			subcat,
			maintenance )

		select 
			id , 
			cat,
			subcat,
			maintenance
		from Bronze.erp_px_cat_g1v2
	set @end_time = GETDATE();
	print '>> Load Duration : ' + CAST(datediff(second , @start_time , @end_time)AS nvarchar) + 'seconds' ;
	print'>>---------------------';

	SET @batch_end_time = GETDATE();
	PRINT '=========================================='
	PRINT 'Loading Silver Layer is Completed';
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


