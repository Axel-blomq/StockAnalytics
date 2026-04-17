# StockAnalytics
Just some code for analysing stocks with data from Yfinance.

# How to setup:
open a command line inside the folder, and run the following:

	docker pull cassandra:latest 
	docker network create cassandra-net
	docker run --rm -d --name cassandra-axel -p 9042:9042 --hostname cassandra-host --network cassandra-net cassandra
	docker cp "./data.cql" cassandra-axel:/data.cql

wait until the Cassandra Database is fully up and running before doing the next command, you will get errors if it is not.

	docker exec cassandra-axel cqlsh -f /data.cql

optional, to verify the server is up and running:

	docker run --rm -it --network cassandra-net nuvo/docker-cqlsh cqlsh cassandra-axel 9042 --cqlversion=3.4.7
	SELECT * FROM stocks.companies;
	
then write "exit" to get back to the regular CMD.

the venv for the streamlit dash needs python Version 3.10 and OpenJDK version 17. 

commands for Conda:

	conda create -n streamlit-env python=3.10
	conda activate streamlit-env
	conda install -c conda-forge openjdk=17
	pip install -r requirements.txt

Then you can start the program with: 

	streamlit run Dashboard.py