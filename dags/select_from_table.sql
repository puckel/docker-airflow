SELECT DATE, STORE_LOCATION, ROUND((SUM(SP) - SUM(CP)), 2) AS lc_profit
FROM clean_store_transactions
WHERE DATE = '2019-11-26'
GROUP BY STORE_LOCATION
ORDER BY lc_profit
DESC INTO OUTFILE '/store_files_mysql/location_wise_profit.csv' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';

SELECT DATE, STORE_ID, ROUND((SUM(SP) - SUM(CP)), 2) AS st_profit
FROM clean_store_transactions
WHERE DATE = '2019-11-26'
GROUP BY STORE_ID
ORDER BY st_profit
DESC INTO OUTFILE '/store_files_mysql/store_wise_profit.csv' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
