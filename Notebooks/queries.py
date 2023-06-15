lending_features = r"""
WITH past_6_mo_txn as (
  		SELECT id AS transaction_id, BUSINESS_ID , amount, TYPE, TRANSACTION_DATE,  status, running_balance,
              CASE WHEN TYPE = 'credit' and DESCRIPTION  ILIKE ANY(
	                    '%AMAZON%'
	                    , '%AMZN%'
	                    , '%STRIPE%'
	                    , '%SQUARE INC%'
	                    , '%SHOPIFY%'
	                    , '%SHOPPAY%'
	                    )
                    AND NOT(
	                    description ILIKE ANY(
	                        '%LYFT'
	                        , '%OfferUp%'
	                        , '%Gumroad%'
	                        , '%FB%Fundrai%'
	                        , '%Verify%'
	                        , '%CASH%'
	                        , '%PAYROLL%'
	                        , '%VRFY%'
	                        , '%CAPITAL%'
	                        , '%REFUND%'
	                    ) ) THEN 1 ELSE 0 END as PD_DEPOSIT_TXN,
                CASE WHEN TYPE ='debit' AND MEDIUM  ILIKE ANY('%external withdrawal%', '%iat withdrawal%') THEN 1 ELSE 0 END AS ach_debit_txn,
                CASE WHEN type = 'credit'  AND medium ILIKE ANY('%external deposit%', '%iat deposit%') THEN 1 ELSE 0 END AS ach_credit_txn,
                CASE WHEN medium = 'POS Withdrawal' THEN 1 ELSE 0 END AS card_withdrawal_txn,
                CASE WHEN TYPE = 'credit' AND description='Mobile Check Deposit' THEN 1 ELSE 0 END AS mrdc_credit_txn,
                datediff(day, TRANSACTION_DATE, current_timestamp) AS daySinceLoanTaken
FROM FIVETRAN_DB.PROD_NOVO_API_PUBLIC."TRANSACTIONS"  
where  status = 'active'  AND  daySinceLoanTaken <=180 AND daySinceLoanTaken > 0
),
cal_dates_1 AS (
			SELECT DATEADD(DAY, SEQ4(), '2021-06-01') AS CAL_DATE
			 FROM TABLE(GENERATOR(ROWCOUNT=>640)) 
			 ORDER BY CAL_DATE DESC),
biz_dates_1 AS (
			SELECT a.BUSINESS_ID, MIN(a.transaction_date) AS FIRST_TXN_DATE
			from FIVETRAN_DB.PROD_NOVO_API_PUBLIC."TRANSACTIONS"  a 
			GROUP BY a.BUSINESS_ID),
PILOT_BIZ_DATES_TEMP AS (
		SELECT *
		FROM  cal_dates_1 A LEFT JOIN
		biz_dates_1 B ON
		date(A.CAL_DATE) >= date(B.FIRST_TXN_DATE)
		ORDER BY CAL_DATE ASC)
,PILOT_BIZ_TXN_TEMP AS (
		SELECT   Business_id ,TRANSACTION_DATE,running_balance
		FROM
		        (SELECT *, RANK () OVER (PARTITION BY BUSINESS_ID,TRANSACTION_DATE ORDER BY TRANSACTION_DATE DESC) RANKS
		         FROM FIVETRAN_DB.PROD_NOVO_API_PUBLIC."TRANSACTIONS" 
                 where 1=1
                 and status='active'
		         )
		WHERE RANKS=1
		order by Business_id, TRANSACTION_DATE)
,PILOT_DAILY_BALANCES_1  AS (
		SELECT BUSINESS_ID,FIRST_TXN_DATE, CAL_DATE,TRANSACTION_DATE,RUNNING_BALANCE,
		first_value(RUNNING_BALANCE) OVER (PARTITION BY BUSINESS_ID,GROUPER ORDER BY cal_date asc, TRANSACTION_DATE desc, RUNNING_BALANCE asc nulls last) as RUNNING_BALANCE_2
		FROM (
		SELECT A.BUSINESS_ID,A.CAL_DATE,A.FIRST_TXN_DATE,
		        B.TRANSACTION_DATE,B.RUNNING_BALANCE,
		         COUNT(running_balance) OVER (PARTITION BY a.BUSINESS_ID ORDER BY cal_date asc) as grouper
		        FROM PILOT_BIZ_DATES_TEMP A LEFT JOIN
		        PILOT_BIZ_TXN_TEMP B ON
		        A.BUSINESS_ID=B.BUSINESS_ID AND
		        date(A.CAL_DATE)= date(B.TRANSACTION_DATE)
		        ORDER BY A.CAL_DATE ASC)
		ORDER BY BUSINESS_ID,CAL_DATE)
,final_daily_running_balance AS (
		 SELECT distinct a.business_id, datediff(day, a.cal_date, current_timestamp) as daySinceLoanTaken, cal_date, running_balance_2 AS running_balance_daily
		           FROM  PILOT_DAILY_BALANCES_1  a   
		          WHERE daySinceLoanTaken > 0 AND daySinceLoanTaken <= 180
		ORDER BY a.business_id, daySinceLoanTaken)
,oneMonth_rb AS (	
				SELECT business_id, min(cal_date) as min_txn_date_1mo_rb, max(cal_date) as max_txn_date_1mo_rb,
				               sum(CASE WHEN running_balance_daily < 0 THEN  1 ELSE 0 end) AS od_count_1m, 
				               sum(CASE WHEN running_balance_daily = 0 THEN  1 ELSE 0 end) AS zero_balance_count_1m,
				               AVG(running_balance_daily) as Avg_running_balance_1M,
  							   Median(running_balance_daily) as Median_running_balance_1M,
							   STDDEV(running_balance_daily) as STDDEV_running_balance_1M
				FROM final_daily_running_balance
				WHERE daySinceLoanTaken <=30
				GROUP BY business_id),
threeMonths_rb AS (	
				SELECT business_id, min(cal_date) as min_txn_date_3mo_rb, max(cal_date) as max_txn_date_3mo_rb,
				               sum(CASE WHEN running_balance_daily < 0 THEN  1 ELSE 0 end) AS od_count_3m, 
				               sum(CASE WHEN running_balance_daily = 0 THEN  1 ELSE 0 end) AS zero_balance_count_3m,
				               AVG(running_balance_daily) as Avg_running_balance_3M,
  							   Median(running_balance_daily) as Median_running_balance_3M,
							   STDDEV(running_balance_daily) as STDDEV_running_balance_3M
				FROM final_daily_running_balance
				WHERE daySinceLoanTaken <=90
				GROUP BY business_id),
 sixMonths_rb AS (	
				SELECT business_id, min(cal_date) as min_txn_date_6mo_rb, max(cal_date) as max_txn_date_6mo_rb,
				               sum(CASE WHEN running_balance_daily < 0 THEN  1 ELSE 0 end) AS od_count_6m, 
				               sum(CASE WHEN running_balance_daily = 0 THEN  1 ELSE 0 end) AS zero_balance_count_6m,
				               AVG(running_balance_daily) as Avg_running_balance_6M,
  							   Median(running_balance_daily) as Median_running_balance_6M,
							   STDDEV(running_balance_daily) as STDDEV_running_balance_6M
				FROM final_daily_running_balance
				WHERE daySinceLoanTaken <=180
				GROUP BY business_id),
aggregate_1_3_6_rb as (
SELECT business_id, zero_balance_count_1m, od_count_3m, COALESCE(Median_running_balance_6M, 0) as Median_running_balance_6M, 
       max_txn_date_6mo_rb, min_txn_date_6mo_rb, max_txn_date_3mo_rb, min_txn_date_3mo_rb, max_txn_date_1mo_rb, min_txn_date_1mo_rb,
       STDDEV_RUNNING_BALANCE_6M
FROM
sixMonths_rb AS a
FULL OUTER JOIN 
threeMonths_rb AS b
using (BUSINESS_ID)
FULL OUTER JOIN 
oneMonth_rb AS c
USING (BUSINESS_ID)
),
past_3_mo_txn AS (
SELECT * FROM past_6_mo_txn WHERE daySinceLoanTaken <= 90
),
past_1mo_txn as(
SELECT * FROM PAST_6_MO_txn WHERE daySinceLoanTaken <= 30
),
past_6_mo_agg AS (
SELECT BUSINESS_ID, min(transaction_date) as min_txn_date_6mo, max(transaction_date) as max_txn_date_6mo,
			  sum(CASE WHEN ach_credit_txn=1 THEN abs(amount) ELSE 0 end) AS ach_credit_amt_180,
			  sum(CASE WHEN ach_debit_txn=1 THEN abs(amount) ELSE 0 end) AS ach_debit_amt_180,
			  sum(CASE WHEN ach_credit_txn=1 AND abs(amount) > 100 THEN 1 ELSE 0 end) AS distinct_ach_c_txns_100_6m,
			  COALESCE(median(CASE WHEN ach_debit_txn=1 THEN abs(amount) ELSE NULL end), 0) AS median_amount_ach_d_6m,
			  COALESCE(sum(CASE WHEN ach_debit_txn=1 THEN abs(amount) ELSE 0 end) / NULLIF(sum(CASE WHEN ach_credit_txn=1 THEN abs(amount) ELSE 0 end), 0), 0) AS ratio_ach_amt_debit_credit_6m,
              COALESCE(stddev(CASE WHEN ach_credit_txn=1 THEN abs(amount) ELSE null end), 0) AS stddev_amount_ach_c_6m,
              COALESCE(sum(ach_debit_txn) / NULLIF(sum(ach_credit_txn), 0), 0) AS ratio_ach_freq_debit_credit_6m,
              COALESCE(sum(CASE WHEN card_withdrawal_txn=1 THEN abs(amount) ELSE 0 end), 0) as card_withdrawal_amt_180,
              COALESCE(sum(CASE WHEN TYPE='debit' THEN 1 ELSE 0 end) /  NULLIF(sum(CASE WHEN TYPE='credit' THEN 1 ELSE 0 end), 0), 0) as ratio_total_freq_debit_credit_6m
FROM past_6_mo_txn GROUP BY BUSINESS_ID 
),
past_3_mo_agg AS (
SELECT BUSINESS_ID, min(transaction_date) as min_txn_date_3mo, max(transaction_date) as max_txn_date_3mo,
              sum(CASE WHEN ach_credit_txn=1 THEN abs(amount) ELSE 0 end) AS ach_credit_amt_90,
			  sum(CASE WHEN ach_debit_txn=1 THEN abs(amount) ELSE 0 end) AS ach_debit_amt_90,
              COALESCE(sum(CASE WHEN TYPE='debit' THEN abs(amount) ELSE 0 end) / NULLIF(sum(CASE WHEN TYPE='credit' THEN abs(amount) ELSE 0 end), 0), 0) AS ratio_debit_credit_3m,
              COALESCE(median(CASE WHEN TYPE='debit' THEN abs(amount) ELSE null end), 0) as median_amount_debited_3m,
              COALESCE(sum(CASE WHEN card_withdrawal_txn=1 THEN abs(amount) ELSE 0 end), 0) AS card_withdrawal_amt_90,
              COALESCE(median(CASE WHEN TYPE='credit' THEN abs(amount) ELSE null end), 0) as median_amount_credited_3m,
              COALESCE(sum(ach_debit_txn), 0) as distinct_ach_d_txns_3m
FROM past_3_mo_txn GROUP BY BUSINESS_ID 
),
past_1_mo_agg AS (
SELECT BUSINESS_ID, min(transaction_date) as min_txn_date_1mo, max(transaction_date) as max_txn_date_1mo,
               STDDEV(CASE WHEN ach_credit_txn=1 THEN abs(AMOUNT) ELSE null end) AS stddev_amount_ach_c_1m,
               sum(mrdc_credit_txn) AS distinct_mrdc_txns_1m,
               COALESCE(sum(ach_debit_txn) / NULLIF(sum(ach_credit_txn), 0), 0) as ratio_ach_freq_debit_credit_1m,
               COALESCE(sum(CASE WHEN TYPE='debit' THEN abs(amount) ELSE 0 end) / NULLIF(sum(CASE WHEN TYPE='credit' THEN abs(amount) ELSE 0 end), 0), 0) AS ratio_debit_credit_1m,
               COALESCE(sum(CASE WHEN TYPE='debit' THEN abs(amount) ELSE null end), 0) as stddev_amount_debited_1m,
               COALESCE(sum(ach_debit_txn), 0) as distinct_ach_d_txns_1m,
               COALESCE(sum(CASE WHEN ach_debit_txn=1 THEN abs(amount) ELSE 0 end) / NULLIF(sum(CASE WHEN ach_credit_txn=1 THEN abs(amount) ELSE 0 end), 0), 0) AS ratio_ach_amt_debit_credit_1m,
               COALESCE(sum(CASE WHEN card_withdrawal_txn=1 and abs(amount) > 100 THEN 1 ELSE 0 end), 0) AS distinct_card_txns_100_1m
               FROM past_1mo_txn
               GROUP BY business_id
),

business_info AS (
SELECT b.BUSINESS_ID, NUMBER_OF_EMPLOYEES::int as NUMBER_OF_EMPLOYEES, 
               datediff(MONTH, to_date(DATE_OF_ESTABLISHMENT, 'YYYY-MM'), current_timestamp) AS business_age_months, 
               datediff(month, ACCOUNT_create_date, current_timestamp) AS months_on_book
FROM  PROD_DB.DATA.businesses b 
      LEFT JOIN PROD_DB.DATA.APPLICATIONS c ON c.application_id = b.application_id
)
		
SELECT BUSINESS_ID, od_count_3m, zero_balance_count_1m, 
			   COALESCE(ach_credit_amt_90/ NULLIF(ach_credit_amt_180, 0), 0) AS ratio_ach_credit_amt_90_180,
			   COALESCE(ach_debit_amt_90/ NULLIF(ach_debit_amt_180, 0), 0) AS ratio_ach_debit_amt_90_180, 
			   COALESCE(stddev_amount_ach_c_1m, 0) as stddev_amount_ach_c_1m, COALESCE(distinct_ach_c_txns_100_6m, 0) as distinct_ach_c_txns_100_6m, 
			   COALESCE(median_amount_ach_d_6m, 0) as median_amount_ach_d_6m, 
			   COALESCE(distinct_mrdc_txns_1m, 0) as distinct_mrdc_txns_1m,
			   COALESCE(ratio_ach_amt_debit_credit_6m, 0) as ratio_ach_amt_debit_credit_6m, COALESCE(ratio_ach_freq_debit_credit_1m, 0) as ratio_ach_freq_debit_credit_1m, 
			   COALESCE(ratio_debit_credit_1m, 0) as ratio_debit_credit_1m, COALESCE(ratio_debit_credit_3m, 0) as ratio_debit_credit_3m, 
			   median_running_balance_6m, median_amount_debited_3m, stddev_amount_debited_1m, stddev_amount_ach_c_6m, distinct_ach_d_txns_1m,
               ratio_ach_amt_debit_credit_1m, ratio_ach_freq_debit_credit_6m, 
               COALESCE(card_withdrawal_amt_90/ NULLIF(card_withdrawal_amt_180, 0), 0) AS ratio_card_withdrawal_amt_90_180,
               median_amount_credited_3m, distinct_ach_d_txns_3m, distinct_card_txns_100_1m,
               ratio_total_freq_debit_credit_6m, STDDEV_RUNNING_BALANCE_6M, 
               months_on_book, business_age_months, number_of_employees
FROM aggregate_1_3_6_rb 
	LEFT JOIN  past_6_mo_agg using(business_id)
	LEFT JOIN past_3_mo_agg using(business_id)
	LEFT JOIN past_1_mo_agg using(business_id)
    LEFT JOIN business_info using(business_id) 
"""
knockout_flags = r"""
    WITH active_businesses_ids as 
    (
    select  business_id, 
            APPLICATION_ID,
            account_create_date,
            account_close_Date,
            account_status,
            last_transaction_date,
            1 as active_till_date,
            CASE WHEN datediff(day,account_create_date,current_timestamp) >= 180 THEN 1 ELSE 0 END as is_age_180Days_plus,
            CASE WHEN datediff(month,last_transaction_Date,current_timestamp) <= 2 THEN 1 ELSE 0 END as is_transaction_in_past2months
        from prod_db.data.businesses
        where account_create_date < current_timestamp
        and (account_close_date is null or account_close_date > current_timestamp)
        and account_status = 'active'
    ),

credit_revenue as
    (
    WITH revenues as (
        Select business_id, sum(amount) as business_revenue
          FROM PROD_DB.DATA.TRANSACTIONS
        where  datediff(days,transaction_date,current_timestamp) <= 180
        and type = 'credit'-- all credit
        and medium != 'POS Withdrawl'--- removing those failed pos
        and status != 'pending' ---- only active or processed
        group by business_id
    )
        select business_id, 
               business_revenue, 
               1 as is_business_revenue_min_2000_6months 
        from revenues where business_revenue > 2000
    ),
    
    application_rejected_180days as
        (
            WITH application_180days as
            (
                select APPLICATION_ID, 
                    application_Start_datetime,
                    ACCOUNT_OPENED_DATETIME
                from prod_db.data.applications
                where datediff(days,application_Start_datetime,current_timestamp) <= 180
            
            )
            select business_id, 
                1 as is_app_rejected_180days
            from active_businesses_ids bus 
            inner join 
                application_180days app 
                on bus.APPLICATION_ID = app.APPLICATION_ID
            where application_Start_datetime is not null 
            and ACCOUNT_OPENED_DATETIME is null
        )
    
    ,businesses_IN_pilot as
        (
            select bus.business_id , 
                1 as is_in_pilot
            from active_businesses_ids bus
            where business_id in (select distinct business_id from "METABASE_DB"."LENDING"."LOAN_AGREEMENTS")
        ),
        
    final_query1 as (Select 
            meta.business_id,
            meta.APPLICATION_ID,
            meta.account_create_date, 
            meta.last_transaction_date,
            meta.account_close_date,
            meta.account_status,
            ZEROIFNULL(active_till_date) as active_till_date,
            ZEROIFNULL(is_age_180Days_plus) as is_age_180Days_plus,
            ZEROIFNULL(is_transaction_in_past2months) as is_transaction_in_past2months,
            ZEROIFNULL(business_revenue) as business_revenue,
            ZEROIFNULL(is_business_revenue_min_2000_6months) as is_business_revenue_min_2000_6months,
            ZEROIFNULL(is_app_rejected_180days) as is_app_rejected_180days,
            ZEROIFNULL(is_in_pilot) as is_in_pilot
            from prod_db.data.businesses meta 
            left join active_businesses_ids bus 
            on meta.business_id = bus.business_id
            left join credit_revenue db_rev
            on bus.business_id = db_rev.business_id
            left join application_rejected_180days app_rej 
            on bus.business_id = app_rej.business_id
            left join businesses_IN_pilot pil 
            on bus.business_id = pil.business_id),

    final_query as (select *,CASE WHEN 
            active_till_date = 1 AND 
            is_age_180Days_plus = 1 AND 
            is_transaction_in_past2months = 1 AND 
            is_business_revenue_min_2000_6months = 1 AND 
            is_app_rejected_180days = 0 AND 
            is_in_pilot = 0  
            THEN 0 ELSE 1 END AS FINAL_KNOCKOUT from final_query1 )


    SELECT * FROM final_query 
"""

revenue_estimator = r"""
WITH active_businesses_ids as 
(
    select  business_id, 
            APPLICATION_ID,
            account_create_date,
            account_close_Date,
            account_status,
            last_transaction_date,
            1 as active_till_oct,
            CASE WHEN datediff(day,account_create_date,current_timestamp) >= 180 THEN 1 ELSE 0 END as is_age_180Days_plus,
            CASE WHEN datediff(month,last_transaction_Date,current_timestamp) <= 2 THEN 1 ELSE 0 END as is_transaction_in_past2months
        from prod_db.data.businesses
        where account_create_date < current_timestamp
        and (account_close_date is null or account_close_date > current_timestamp)
        and account_status = 'active'
),
external_active_account_names AS (
SELECT t1.id AS plaid_item_id, t1.business_id, lower(regexp_replace(t2.value, '(inc|inc.|llc|\\,|\\.|co|corp.|co.|corp)', '', 1,0,'i')) AS external_acc_company_name  
FROM FIVETRAN_DB.PROD_NOVO_API_PUBLIC.plaid_items t1, LATERAL FLATTEN(input => "IDENTITY":names) t2
WHERE status = 'active'
),
target_businesses AS (
SELECT DISTINCT a.id AS business_id, a.TYPE AS business_type, 
               lower(regexp_replace(company_name, '(inc|inc.|llc|\\,|\\.|co|corp.|co.|corp)', '', 1,0,'i')) AS company_name, 
               CASE WHEN dba IS NOT NULL THEN lower(regexp_replace(dba, '(inc|inc.|llc|\\,|\\.|co|corp.|co.|corp)', '', 1,0,'i')) ELSE NULL end AS dba 
FROM  FIVETRAN_DB.PROD_NOVO_API_PUBLIC.BUSINESSES a INNER JOIN  FIVETRAN_DB.PROD_NOVO_API_PUBLIC.PLAID_TRANSACTIONS b ON a.id=b.business_id
WHERE a.status='active' AND a.id IN (
		select business_id from
		PROD_DB.DATA.BUSINESSES 
        where IS_ACCOUNT_FUNDED = true AND  datediff(month, ACCOUNT_FUNDED_AT, current_timestamp) <= 6) 
),
extrenal_accounts_non_sol_prop AS (
SELECT c.business_id, a.external_acc_company_name, 
              b.EXTERNAL_ACCOUNT_ID, business_type, company_name  
              FROM external_active_account_names a
			  LEFT JOIN FIVETRAN_DB.PROD_NOVO_API_PUBLIC.PLAID_EXTERNAL_ACCOUNTS b ON a.plaid_item_id = b.PLAID_ITEM_ID 
			  LEFT JOIN target_businesses c ON a.BUSINESS_ID  = c.business_id
WHERE c.business_type != 'sole_proprietorship'  
			  AND ( (company_name = external_acc_company_name) OR 
			              (SOUNDEX(company_name) = SOUNDEX(external_acc_company_name)) OR
			              (dba IS NOT NULL AND dba = external_acc_company_name) OR
			              (dba IS NOT NULL AND SOUNDEX(dba)=SOUNDEX(external_acc_company_name))
			             )
			  AND c.business_id IN (SELECT business_id FROM active_businesses_ids)
),
extrenal_non_sol_prop_revenue AS (
SELECT a.business_id, amount,TRANSACTION_id, DATEDIFF(month, a.DATE, current_timestamp) AS txn_month, 
          CASE WHEN CATEGORIES ILIKE '%Insurance%' and TYPE = 'credit' THEN 1 ELSE 0 END as INSURANCE_CREDIT_TXN,
          CASE WHEN CATEGORIES ILIKE '%Loan%' and TYPE = 'credit' THEN 1 ELSE 0 END as LOAN_CREDIT_TXN,
          CASE WHEN CATEGORIES ILIKE '%Tax%' AND CATEGORIES NOT ILIKE '%Taxi%' and TYPE = 'credit' THEN 1 ELSE 0 END as TAX_CREDIT_TXN,
          CASE WHEN CATEGORIES ILIKE '%Insurance%' and TYPE = 'credit' THEN AMOUNT ELSE 0 END as INSURANCE_CREDIT_AMT,
          CASE WHEN CATEGORIES ILIKE '%Loan%' and TYPE = 'credit' THEN AMOUNT ELSE 0 END as LOAN_CREDIT_AMT,
          CASE WHEN CATEGORIES ILIKE '%Tax%' AND CATEGORIES NOT ILIKE '%Taxi%' and TYPE = 'credit' THEN AMOUNT ELSE 0 END as TAX_CREDIT_AMT
FROM FIVETRAN_DB.PROD_NOVO_API_PUBLIC.PLAID_TRANSACTIONS  a INNER JOIN extrenal_accounts_non_sol_prop b
ON a.EXTERNAL_ACCOUNT_ID = b.EXTERNAL_ACCOUNT_ID
WHERE  datediff(MONTH, a.DATE, current_timestamp) <= 6 and datediff(MONTH, a.DATE, current_timestamp) > 0
       and a.pending = FALSE AND a.TYPE = 'credit'
),
all_credits_plaid as(
SELECT BUSINESS_ID, txn_month, sum(-amount)  AS total_credit_amount_plaid, 
               sum(-LOAN_CREDIT_AMT) AS total_loan_credit_amt_plaid, 
               sum(-INSURANCE_CREDIT_AMT) AS total_insurance_credit_amt_plaid, 
               sum(-TAX_CREDIT_AMT) AS total_tax_credit_amt_plaid
FROM extrenal_non_sol_prop_revenue 
GROUP BY BUSINESS_ID, txn_month
),
txns_with_categories AS (
select business_id, DATEDIFF(month, TRANSACTION_DATE, current_timestamp) AS txn_month, amount, description,
			REGEXP_REPLACE(REGEXP_REPLACE(lower(description), ':',' '), '[^a-z ]', '') AS description_processed, transaction_date,
			            CASE WHEN description_processed ILIKE any(
			            						'% earninactivehours %', 
                                                'earninactivehours %',
                                                '% earninactivehours',
                                                'earninactivehours',
			            						'% activehours %', 
                                                'activehours %', 
                                                '% activehours', 
                                                'activehours', 
			            						'% navchek %', 
                                                'navchek %', 
                                                '% navchek', 
                                                'navchek', 
			            						'% loan %', 
                                                'loan %',
                                                '% loan',
                                                'loan',
			            						'% earnin %',
                                                'earnin %',
                                                '% earnin',
                                                'earnin', 
			            						'% advance %',
                                                '% advance',
                                                'advance %',
                                                'advance'
			            ) THEN 1 ELSE 0 END AS LOANS,
			            CASE WHEN description_processed ILIKE any(
			            						'% hcclaimpmt %',
                                                '% hcclaimpmt',
                                                'hcclaimpmt %',
                                                'hcclaimpmt',
												 '% unitedhealthcare %',
                                                 'unitedhealthcare %',
                                                 '% unitedhealthcare',
                                                 'unitedhealthcare',
												 '% bcbs %',
                                                 '% bcbs',
                                                 'bcbs %',
                                                 'bcbs',
												 '% cigna %',
                                                 'cigna %',
                                                 '% cigna',
                                                 'cigna',
												 '% govt %',
                                                 '% govt',
                                                 'govt %',
                                                 'govt',
												 '% humana %',
                                                 '% humana',
                                                 'humana %',
                                                 'humana',
												 '% aetna %',
                                                 'aetna %',
                                                 '% aetna',
                                                 'aetna',
												 '% insurance %',
                                                 'insurance %',
                                                 '% insurance',
                                                 'insurance',
												 '% life %',
                                                 'life %',
                                                 '% life',
                                                 'life',
												 '% blue %',
                                                 '% blue',
                                                 'blue %',
                                                 'blue'
						) THEN 1 ELSE 0 END AS INSURANCE,
						CASE WHEN description_processed ILIKE any(
												'% sbtpg %',
                                                '% sbtpg',
                                                'sbtpg %',
                                                'sbtpg', 
												'% xxtaxeip %',
                                                'xxtaxeip %',
                                                '% xxtaxeip',
                                                'xxtaxeip', 
												'% tax %', 
                                                'tax %', 
                                                '% tax', 
                                                'tax', 
												'% irs %', 
                                                '% irs', 
                                                'irs %', 
                                                'irs', 
												'% treas %', 
                                                '% treas', 
                                                'treas %', 
                                                'treas', 
												'% taxrfd %',
                                                '% taxrfd',
                                                'taxrfd %',
                                                'taxrfd'
						) THEN 1 ELSE 0 END AS TAX
from fivetran_db.PROD_NOVO_API_PUBLIC.transactions 
		   WHERE business_id IN (SELECT business_id FROM active_businesses_ids)
           AND datediff(MONTH ,TRANSACTION_DATE,current_timestamp) <= 6 and datediff(MONTH ,TRANSACTION_DATE,current_timestamp) > 0
           AND status='active' AND TYPE = 'credit'     
)
,all_credits AS (
SELECT a.BUSINESS_ID, txn_month,
               sum(amount)  AS total_credit_amount, 
               sum(CASE WHEN LOANS = 1 THEN amount ELSE 0 end) AS total_loan_credit_amount, 
               sum(CASE WHEN INSURANCE = 1 THEN amount ELSE 0 END) AS total_insurance_credit_amount, 
               sum(CASE WHEN TAX = 1 THEN amount ELSE 0 end) AS total_tax_credit_amount
FROM  txns_with_categories a
GROUP BY a.BUSINESS_ID, txn_month
),
all_txn_credits AS (
SELECT business_id, txn_month, 
               COALESCE(total_credit_amount, 0) AS total_credit_amount,
               COALESCE(total_loan_credit_amount, 0) AS total_loan_credit_amount,
               COALESCE(total_insurance_credit_amount, 0) AS total_insurance_credit_amount,
               COALESCE(total_tax_credit_amount, 0) AS total_tax_credit_amount,
               COALESCE(TOTAL_CREDIT_AMOUNT_PLAID, 0) AS TOTAL_CREDIT_AMOUNT_PLAID,
               COALESCE(TOTAL_LOAN_CREDIT_AMT_PLAID, 0) AS TOTAL_LOAN_CREDIT_AMT_PLAID,
               COALESCE(TOTAL_INSURANCE_CREDIT_AMT_PLAID, 0) AS TOTAL_INSURANCE_CREDIT_AMT_PLAID,
               COALESCE(TOTAL_TAX_CREDIT_AMT_PLAID, 0) AS TOTAL_TAX_CREDIT_AMT_PLAID
FROM all_credits LEFT JOIN all_credits_plaid using(business_id, txn_month)
)
,last_6mnth_revenue as (
SELECT business_id, 
       CASE WHEN txn_month = 1 THEN 'past_1month_rev'
            WHEN txn_month = 2 THEN 'past_2month_rev'
            WHEN txn_month = 3 THEN 'past_3month_rev'
            WHEN txn_month in (4,5,6) THEN 'past_furthest_3months'
       END as month,
       ((total_credit_amount-total_loan_credit_amount-total_insurance_credit_amount-total_tax_credit_amount)
	           + (TOTAL_CREDIT_AMOUNT_PLAID - TOTAL_LOAN_CREDIT_AMT_PLAID - TOTAL_INSURANCE_CREDIT_AMT_PLAID - TOTAL_TAX_CREDIT_AMT_PLAID)) AS revenue
FROM all_txn_credits
where txn_month <= 6)

,pivot_revenue as (
select * from last_6mnth_revenue
    pivot (sum(revenue) for month in ('past_1month_rev', 'past_2month_rev', 'past_3month_rev', 'past_furthest_3months'))
      as p (business_id, past_1month_rev, past_2month_rev, past_3month_rev, past_furthest_3months)

)

select *, 
       ZEROIFNULL(past_1month_rev)*0.1508
      +ZEROIFNULL(past_2month_rev)*0.1154
      +ZEROIFNULL(past_3month_rev)*0.0854
      +ZEROIFNULL(past_furthest_3months/3)*0.2136
      +460.6110
      as avg_predicted_revenue,
      6*avg_predicted_revenue AS predicted_revenue_next_6mo
from pivot_revenue
"""
