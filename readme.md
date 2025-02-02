1. Install mysql server with jdbc connector. Start server and fill [sql_cfg.json](sql_cfg.json) file to be able make connection to sql from spark
1. Install dependencies
```bash
conda env create -f environment.yml
```
3. Run [loadToSQL.py](loadToSQL.py), that loads instacart data to local path. Then loads that dataframes to spark, which sends data to sql server
1. Check analysis inside [sql.md](sql.md). There are written sql queries and analysis of table with checking different hypothesis. All visualizations made by [Grafana](https://grafana.com/)
1. Look at [spark.ipynb](spark.ipynb) to analysis using spark