create or replace database EARNINGS;

create or replace schema ANALYSIS;

create or replace TABLE CALL_SUMMARY (
	STOCK_SYMBOL VARCHAR(16777216),
	FISCAL_QUARTER NUMBER(38,0),
	FISCAL_YEAR NUMBER(38,0),
	QUERY_DATE DATE,
	CALL_SUMMARY VARCHAR(16777216)
);
create or replace TABLE EARNINGS_FOR_SEMANTIC (
	STOCK_SYMBOL VARCHAR(16777216),
	FISCAL_QUARTER VARCHAR(16777216),
	FISCAL_YEAR VARCHAR(16777216),
	RELEASE_DATE VARCHAR(16777216),
	GRADE VARCHAR(16777216),
	ADDRESS VARCHAR(16777216),
	CITY VARCHAR(16777216),
	STATE VARCHAR(16777216),
	ZIP VARCHAR(16777216),
	COUNTRY VARCHAR(16777216),
	PHONE VARCHAR(16777216),
	COMPANY_NAME VARCHAR(16777216),
	CEO VARCHAR(16777216),
	INDUSTRY VARCHAR(16777216),
	SECTOR VARCHAR(16777216),
	EXCHANGE VARCHAR(16777216),
	EXCHANGE_SHORT_NAME VARCHAR(16777216),
	ISIN VARCHAR(16777216),
	CUSIP VARCHAR(16777216),
	IPO_DATE DATE,
	IS_ACTIVELY_TRADING BOOLEAN,
	FULL_TIME_EMPLOYEES VARCHAR(16777216),
	BETA FLOAT,
	PRICE FLOAT,
	DCF FLOAT,
	DCF_DIFF FLOAT,
	CHANGES FLOAT,
	LAST_DIV FLOAT,
	MARKET_CAP NUMBER(38,0),
	VOL_AVG NUMBER(38,0),
	RANGE VARCHAR(16777216),
	CIK VARCHAR(16777216),
	RISK_FREE_RETURN NUMBER(1,0),
	MARKET_RETURN NUMBER(2,1)
);
create or replace TABLE EARNINGS_SUMMARIES (
	STOCK_SYMBOL VARCHAR(16777216),
	FISCAL_QUARTER VARCHAR(16777216),
	FISCAL_YEAR VARCHAR(16777216),
	RELEASE_DATE VARCHAR(16777216),
	QUERY_DATE DATE,
	TITLE VARCHAR(16777216),
	SUMMARY_JSON VARIANT,
	GRADE VARIANT,
	EARNINGS VARIANT,
	PROFILE VARIANT,
	CREATED_AT TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
);
create or replace materialized view EARNINGS_SUMMARIES_MV(
	STOCK_SYMBOL,
	FISCAL_QUARTER,
	FISCAL_YEAR,
	RELEASE_DATE,
	QUERY_DATE,
	TITLE,
	INTRO,
	ANALYSIS,
	POSITIVES,
	NEGATIVES,
	CONCLUSION,
	GRADE,
	REASON,
	ADDRESS,
	CITY,
	STATE,
	ZIP,
	COUNTRY,
	PHONE,
	COMPANY_NAME,
	CEO,
	INDUSTRY,
	SECTOR,
	DESCRIPTION,
	EXCHANGE,
	EXCHANGE_SHORT_NAME,
	ISIN,
	CUSIP,
	IPO_DATE,
	IS_ACTIVELY_TRADING,
	FULL_TIME_EMPLOYEES,
	BETA,
	PRICE,
	DCF,
	DCF_DIFF,
	CHANGES,
	LAST_DIV,
	MARKET_CAP,
	VOL_AVG,
	RANGE,
	CIK,
	IMAGE,
	CREATED_AT
) COMMENT='MV of earnings summaries table to ease of readibility'
 as
select STOCK_SYMBOL, FISCAL_QUARTER, FISCAL_YEAR, RELEASE_DATE, QUERY_DATE

,TITLE
,parse_json(summary_json)[0].section_body::string as intro
,parse_json(summary_json)[1].section_body::string as Analysis
,parse_json(summary_json)[2].section_body::string as positives
,parse_json(summary_json)[3].section_body::string as negatives
,parse_json(summary_json)[4].section_body::string as conclusion

,parse_json(grade):grade::string as grade
,parse_json(grade):reason::string as reason

,TO_VARCHAR(parse_json(profile):address) AS address
,TO_VARCHAR(parse_json(profile):city) AS city
,TO_VARCHAR(parse_json(profile):state) AS state
,TO_VARCHAR(parse_json(profile):zip) AS zip
,TO_VARCHAR(parse_json(profile):country) AS country
,TO_VARCHAR(parse_json(profile):phone) AS phone
,TO_VARCHAR(parse_json(profile):companyName) AS company_name
,TO_VARCHAR(parse_json(profile):ceo) AS ceo
,TO_VARCHAR(parse_json(profile):industry) AS industry
,TO_VARCHAR(parse_json(profile):sector) AS sector
,TO_VARCHAR(parse_json(profile):description) AS description
,TO_VARCHAR(parse_json(profile):exchange) AS exchange
,TO_VARCHAR(parse_json(profile):exchangeShortName) AS exchange_short_name
,TO_VARCHAR(parse_json(profile):isin) AS isin
,TO_VARCHAR(parse_json(profile):cusip) AS cusip
,TRY_TO_DATE(parse_json(profile):ipoDate::STRING) AS ipo_date
,TO_BOOLEAN(parse_json(profile):isActivelyTrading) AS is_actively_trading
,TO_VARCHAR(parse_json(profile):fullTimeEmployees) AS full_time_employees
,TO_DOUBLE(parse_json(profile):beta) AS beta
,TO_DOUBLE(parse_json(profile):price) AS price
,TO_DOUBLE(parse_json(profile):dcf) AS dcf
,TO_DOUBLE(parse_json(profile):dcfDiff) AS dcf_diff
,TO_DOUBLE(parse_json(profile):changes) AS changes
,TO_DOUBLE(parse_json(profile):lastDiv) AS last_div
,TO_NUMBER(parse_json(profile):mktCap) AS market_cap
,TO_NUMBER(parse_json(profile):volAvg) AS vol_avg
,TO_VARCHAR(parse_json(profile):range) AS range
,TO_VARCHAR(parse_json(profile):cik) AS cik
,TO_VARCHAR(parse_json(profile):image) AS image
,created_at

from earnings.analysis.earnings_summaries
;
create or replace TABLE SUMMARY_TRANSLATIONS (
	STOCK_SYMBOL VARCHAR(16777216),
	FISCAL_QUARTER NUMBER(38,0),
	FISCAL_YEAR NUMBER(38,0),
	QUERY_DATE DATE,
	SUMMARY_TRANSLATE VARCHAR(16777216)
);

create or replace schema PUBLIC;

create or replace TABLE FINETUNE_EARNINGS (
	STOCK_SYMBOL VARCHAR(16777216),
	PROMPT VARCHAR(16777216),
	COMPLETE VARCHAR(16777216),
	SOURCE VARCHAR(16777216)
);
create or replace TABLE FINE_TUNE (
	COMPLETION VARCHAR(16777216),
	PROMPT VARCHAR(16777216),
	SOURCE VARCHAR(16777216)
);
create or replace TABLE SUMMARY_SCORE (
	STOCK_SYMBOL VARCHAR(16777216),
	FISCAL_QUARTER NUMBER(38,0),
	FISCAL_YEAR NUMBER(38,0),
	SCORE VARCHAR(16777216)
);
create or replace TABLE TRANSCRIPTS (
	STOCK_SYMBOL VARCHAR(16777216),
	FISCAL_QUARTER NUMBER(38,0),
	FISCAL_YEAR NUMBER(38,0),
	RELEASE_DATE DATE,
	QUERY_DATE DATE,
	TRANSCRIPT VARCHAR(16777216),
	CREATED_AT TIMESTAMP_LTZ(9)
);
create or replace TABLE TRANSCRIPT_CHUNKS (
	STOCK_SYMBOL VARCHAR(16777216),
	FISCAL_QUARTER NUMBER(38,0),
	FISCAL_YEAR NUMBER(38,0),
	RELEASE_DATE DATE,
	QUERY_DATE DATE,
	CHUNK_ID NUMBER(38,0),
	TRANSCRIPT_CHUNK VARCHAR(1000),
	EMBEDDING VECTOR(FLOAT, 768)
);
create or replace view TRANSCRIPT_CHUNKS_STOCK_VW(
	STOCK_SYMBOL,
	FISCAL_QUARTER,
	FISCAL_YEAR,
	RELEASE_DATE,
	QUERY_DATE,
	CHUNK_ID,
	TRANSCRIPT_CHUNK,
	EMBEDDING,
	TRANSCRIPT_CHUNKS_STOCK
) as select *,
concat(stock_symbol,' ', transcript_chunk) as transcript_chunks_stock
from EARNINGS.PUBLIC.TRANSCRIPT_CHUNKS;
create or replace view TRANSCRIPT_CHUNKS_VIEW(
	STOCK_SYMBOL,
	FISCAL_QUARTER,
	FISCAL_YEAR,
	RELEASE_DATE,
	QUERY_DATE,
	CHUNK_ID,
	TRANSCRIPT_CHUNK,
	EMBEDDING,
	TRANSCRIPT_CHUNKS_STOCK_CONCAT
) as select *,
concat(stock_symbol,' ', transcript_chunk) as transcript_chunks_stock_concat
from EARNINGS.PUBLIC.TRANSCRIPT_CHUNKS;
CREATE OR REPLACE FUNCTION "GENERIC_URL"("API_URL" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('requests','pandas')
HANDLER = 'get_earnings'
EXTERNAL_ACCESS_INTEGRATIONS = (FMP_ACCESS_INTEGRATION)
AS '
import requests
import pandas as pd
import json

def get_earnings(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        return "Error: " + str(response.status_code)
';
CREATE OR REPLACE FUNCTION "GET_EARNINGS_DATA"()
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('requests')
HANDLER = 'get_earnings'
EXTERNAL_ACCESS_INTEGRATIONS = (FMP_ACCESS_INTEGRATION)
AS '
import requests

def get_earnings():
    url = "https://wwww.URL.com...."
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        return "Error: " + str(response.status_code)
';
CREATE OR REPLACE FUNCTION "GET_EARNINGS_DATA"("START_DATE" VARCHAR(16777216), "END_DATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('requests')
HANDLER = 'get_earnings'
EXTERNAL_ACCESS_INTEGRATIONS = (FMP_ACCESS_INTEGRATION)
AS '
import requests

def get_earnings(start_date, end_date):
    url = f"https://www.URL.com"
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        return "Error: " + str(response.status_code)
';
create or replace task CORTEX_GRADE_EARNINGS
	warehouse=TRANSCRIPT_WH
	USER_TASK_TIMEOUT_MS=13600000
	after EARNINGS.PUBLIC.SAVE_EARNINGS_CALLS_TASK
	as EXECUTE NOTEBOOK "EARNINGS"."PUBLIC"."Grade Earnings Transcripts"();
create or replace task EMBED_EARNINGS
	warehouse=TRANSCRIPT_WH
	USER_TASK_TIMEOUT_MS=13600000
	after EARNINGS.PUBLIC.CORTEX_GRADE_EARNINGS
	as EXECUTE NOTEBOOK "EARNINGS"."PUBLIC"."Embedding Earnings"();
create or replace task SAVE_EARNINGS_CALLS_TASK
	warehouse=TRANSCRIPT_WH
	schedule='USING CRON 0 1 * * * America/New_York'
	USER_TASK_TIMEOUT_MS=13600000
	as EXECUTE NOTEBOOK "EARNINGS"."PUBLIC"."Get & Save Earnings Transcripts"();


create or replace streamlit W5XCU44HH8B9B__1
	root_location='@EARNINGS.PUBLIC."W5XCU44HH8B9B__1 (Stage)"
	main_file='/streamlit_app.py'
	query_warehouse='TRANSCRIPT_WH'
	comment='{"lastUpdatedUser":"8255659894663","lastUpdatedTime":1729637732074}'
	title='Financial Advisor (Full Demo)';