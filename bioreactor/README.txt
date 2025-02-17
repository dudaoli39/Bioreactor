This is a BIOREACTOR emulator that will generate random data for 4 different sensors (temperature, pressure, humidity and oxigen levels) and save it as a timeseries database.

Infrastructure needed:
-> Python 3
	-> virtual environment created with all required libs from REQUIREMENTS installed
-> MongoDB 8 in a docker server
-> KAFKA in a docker server

MongoDB
After installed, set it up as follow:
-> access mongodb
	"docker exec -it mongodb_container_name bash"
-> access bash
	mongosh
-> create DB "bioreactorDB"
	use bioreactorDB
-> create the collections for each sensors
	db.createCollection(
		"temperature",
		{
		   timeseries: {
			  timeField: "timestamp",
			  metaField: "metadata",
			  granularity: "seconds"
		   }
		}
	)
	db.createCollection(
		"pressure",
		{
		   timeseries: {
			  timeField: "timestamp",
			  metaField: "metadata",
			  granularity: "seconds"
		   }
		}
	)
	db.createCollection(
		"humidity",
		{
		   timeseries: {
			  timeField: "timestamp",
			  metaField: "metadata",
			  granularity: "seconds"
		   }
		}
	)
	db.createCollection(
		"oxigen",
		{
		   timeseries: {
			  timeField: "timestamp",
			  metaField: "metadata",
			  granularity: "seconds"
		   }
		}
	)


BIOREACTOR configuration
All configuration files needed are in the reactor_config folder
-> Update "connections.json" with the KAFKA and MONGODB servers information for connection
-> "configurations.json" have the information related to the sensors.
		-> "FREQUENCE": 1,					- how often in seconds the sensors will generate data
		-> "ACCEPTED_VARIANCE": 0.03,			- accepted % variance out of the MIN / MAX values
		-> "RANDOM_IMPACT": 0.5,			- how high the TEMPERATURE will be impacted
		-> "TEMPERATURE_MIN": 30.0,			- minimum TEMPERATURE (C) acceptable
		-> "TEMPERATURE_MAX": 37.0,			- maximum TEMPERATURE (C) acceptable
		-> "PRESSURE_MIN": 1.0,				- minimum PRESSURE (atm) acceptable
		-> "PRESSURE_MAX": 1.2,				- maximum PRESSURE (atm) acceptable
		-> "HUMIDITY_MIN": 0.8,				- minimum HUMIDITY (%) acceptable
		-> "HUMIDITY_MAX": 0.94,			- maximum HUMIDITY (%) acceptable
		-> "OXIGEN_MIN": 0.3,				- minimum OXIGEN (%) acceptable
		-> "OXIGEN_MAX": 0.5,				- maximum OXIGEN (%) acceptable
		-> "TEMPERATURE_OXIGEN": -0.015, 	- how much OXIGEN (%) is impacted for each 1 C TEMPERATURE change
		-> "TEMPERATURE_HUMIDITY": 0.007, 	- how much HUMIDITY (%) is impacted for each 1 C TEMPERATURE change
		-> "HUMIDITY_PRESSURE": 0.1,		- how much PRESSURE (atm) is impacted for each 1% HUMIDITY change
		-> "PRESSURE_OXIGEN": 1				- how much OXIGEN (%) is impacted for each 1 atm PRESSURE change


The relationship between the sensors is explained in the RELATIONSHIP.TXT support files

Once everything is up and running, execute each of the CONSUMERS (order doesn't matter):
	bioreactor_con_temperature.py
	bioreactor_con_pressure.py
	bioreactor_con_humidity.py
	bioreactor_con_oxigen.py

Finally, update the file bioreactor.py with the name of the reactor (if needed) in:
	reactor_name = "01" -> 01 is the default for the reactor name

And execute the PRODUCER to generate the data
	bioreactor.py
	

To finish, cancel the PRODUCER first and then all CONSUMERS

**********************************************************************************************
Reading data

There is a support file to easily consumption and validation of the data in
	mongo_reading.py

Update the desired sensor, reactor name and date interval you want to check and execute.
	reactor_name = "01"					- 01 is default
	topic = 1							- [1] temperature, [2] pressure, [3] humidity, [4] oxigen
	start = "2025-02-16 07:30:00"		- interval start
	finish = "2025-02-16 07:31:00"		- interval finish







