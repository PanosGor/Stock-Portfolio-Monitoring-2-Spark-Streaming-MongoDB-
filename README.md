# Stock-Portfolio-Monitoring-2-Spark-Streaming-MongoDB-
This a Spark Streaming process 
Server.py imitates stock trades
App1.py reads the data in 15 seconds intervals (DStreams) and produces some metrics(min,max,average per stock received) per interval. 
Additionaly it uses a window every 2 minutes and calculates the SPREAD for each stock received in that window.
The results are saved in a Mongo Database

App2.py receives some input dates by the user and queries the mongo DB  to bring back 
The Average,Max,Min price and SPREAD per share
Last update per share
The Minimum Spread

This was developed in a virtual machine that uses Ubuntu Linux as an operating system. It is recomended to run thiscode on a similar operating system
