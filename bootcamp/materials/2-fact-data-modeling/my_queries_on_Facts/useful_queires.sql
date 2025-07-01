-- Details able table column 
-- System Table = information_schema.columns
SELECT 
-- * 
 column_name, data_type, is_nullable, data_type
FROM information_schema.columns
WHERE table_name = '<table_name>';



-- DATELIST_INT : Monthly, Weekly and Daily 
with user_browser as (
						select *
						from user_devices_cumulated
						where record_date = date('2023-01-31')
					  ),
series as (
			select *
			from generate_series(
									date('2023-01-01'), 
									date('2023-01-31'), 
									Interval '1 day'
								) as series_date
), placeholder_int as (
						select 
						date(s.series_date)
						-- , device_activity_datelist @> ARRAY[date(series_date)]
						-- , record_date - date(s.series_date) as dates_since_active
						, case 
							when device_activity_datelist @> ARRAY[date(series_date)]
								then cast (
											pow(2, 32 - (record_date - date(s.series_date)))
											as BIGINT
										  ) 
							else
								0
						  end as placeholder_int_value
						,u.* 
						from user_browser u
							cross join series s
), datelist as (
				select 
				user_id,
				-- sum(placeholder_int_value),
				cast(cast(sum(placeholder_int_value) as BIGINT) as BIT(32)) as datelist_int
				-- , BIT_COUNT(
				-- 		cast(cast(sum(placeholder_int_value) as BIGINT) as BIT(32))
				-- 		)
				from placeholder_int
				group by user_id
)
select user_id, 
	datelist_int,
	BIT_COUNT(datelist_int) > 0  as Dim_is_Monthly_Active_User
	, BIT_COUNT(
			cast ('11111110000000000000000000000000' as BIT(32)) & 
			cast(datelist_int as BIT(32))
			) > 0 as Dim_is_Weekly_Active
	, BIT_COUNT(
			cast ('10000000000000000000000000000000' as BIT(32)) & 
			cast(datelist_int as BIT(32))
			) > 0 as Dim_is_Daily_Active
from datelist
	;