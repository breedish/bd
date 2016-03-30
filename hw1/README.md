# How to run
For the simplicity crawling is performed on small subset of urls. Input data is located in `{project.dir}/data/seed.input.txt`

To run the following steps should be performed within project directory:

* `mvn clean install`
* `chmod +x target/execute`
* `sh ./target/execute`

*execute* file consists of the following steps:

* `hdfs dfs -mkdir -p /apps/bdc/hw1/`
* `hdfs dfs -rm /apps/bdc/hw1/hw1-app.jar`
* `hdfs dfs -rm /apps/bdc/hw1/seed.input.txt`
* `hdfs dfs -rm /apps/bdc/hw1/output.seed.txt`
* `hdfs dfs -put target/hw1-app.jar /apps/bdc/hw1/hw1-app.jar`
* `hdfs dfs -put target/seed.input.txt /apps/bdc/hw1/seed.input.txt`
* `yarn jar target/hw1-app.jar hdfs://sandbox.hortonworks.com:8020/apps/bdc/hw1/seed.input.txt hdfs://sandbox.hortonworks.com:8020/apps/bdc/hw1/seed.output.txt hdfs://sandbox.hortonworks.com:8020/apps/bdc/hw1/hw1-app.jar`
 
# Expected result

_hdfs://sandbox.hortonworks.com:8020/apps/bdc/hw1/seed.output.txt_ is created and contains the following data:

```
ID	Keyword Value	Keyword Status	Pricing Type	Keyword Match Type	Destination URL
282163091263	$16.99,$usd,2016,across,all,automobile,cancel,categories,choose,cleaning	ON	CPC	BROAD	http://www.miniinthebox.com/oil-pollution-cleaning-automobile-engine-pipe-with-reinigungspistole-spray-gun-tool_p4815979.html
282163090732	cars,$42.99,$usd,2016,across,all,and,autophix®,cancel,categories	ON	CPC	BROAD	http://www.miniinthebox.com/obdmate-diagnostic-tool-obd2-obdii-eobd-code-reader-om510-gasoline-cars-and-a-part-of-diesel-cars_p1516838.html
282163096883	obd,$usd,(41165),2016,accessories,across,all,apple,cancel,categories	ON	CPC	BROAD	http://www.miniinthebox.com/obd_c9859
282163094005	accessories,ds,nintendo,$usd,(41165),2016,across,all,apple,cancel	ON	CPC	BROAD	http://www.miniinthebox.com/nintendo-ds-accessories_c623
```

# Possible issues
* IP Block
* Runs on a single container in multiple threads
* All dependencies are distributed with an uber jar
