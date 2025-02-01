1. Install mysql server with jdbc connector. Start server and fill [sql_cfg.json](sql_cfg.json) file to be able make connection to sql from spark
2. Run [loadToSQL.py](loadToSQL.py), that loads instacart data to local path. Then loads that dataframes to spark, which sends data to sql server
3. Check analysis inside [sql.md](sql.md). There are written sql queries and analysis of table with checking different hypothesis
4. Look at [spark.ipynb](spark.ipynb) to analysis using spark