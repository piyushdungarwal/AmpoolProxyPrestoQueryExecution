Executing queries using Python: pythonPrestoClient.py
===============================================

This is a sample python script showing how to run queries on Ampool Proxy Presto query engine.

Prerequisites:
--------------
Install python presto client on the machine from you want to execute queries.
Use command: 'pip install presto-client'

Usage:
------

Copy .py file to any machine from where you want to execute query.
Before running the script, you have to update connection details in the script.

'host': Ampool Proxy engine master node ip
'user': Instance name provided while registering secret key on Ampool proxy
'catalog': ampoolproxy
'schema': Presto schema for registered tables within Ampool proxy. Default: 'default'

'http_scheme': 'https' for secure communication
'auth': Update Presto user name and password with Instance name and secret key (got from Ampool proxy) respectively. 

After above updates, script can be simply run as: 'python pythonPrestoClient.py'

======================================================================================================================================

Executing queries using JDBC: JDBCPresto.java
=============================================

This is a sample java class file showing how to run queries on Ampool Proxy Presto query engine via JDBC.

Prerequisites:
--------------
- Java 8
- Presto JDBC driver. This can be downloaded from: https://s3-us-west-2.amazonaws.com/ampool-ae2.3/AE-2.3.B25/ADM3/presto-jdbc-330.jar

Usage:
------

Copy .java file to any machine from where you want to execute query.
Before running the java program, you have to update connection details in the file.

"user" property: Instance name provided while registering secret key on Ampool proxy
"password" property: Secret key generated on Ampool proxy
Connection url "jdbc:presto://<AE proxy master node>:9295/ampoolproxy/default": Provide Ampool Proxy master node ip.
Query string "sql": "show catalogs"

After above updates, Java program can be executed as follows:

javac JDBCPresto.java
java -cp $CLASSPATH:<Path to downloaded Presto JDBC jar>/presto-jdbc-330.jar JDBCPresto

======================================================================================================================================

Executing queries using Presto CLI
==================================

Prerequisites:
--------------
- Java 8
- Presto CLI executable jar. This can be downloaded from: https://s3-us-west-2.amazonaws.com/ampool-ae2.3/AE-2.3.B25/ADM3/presto-cli-330-executable.jar

Usage:
------

Execute following command which will open Presto CLI.

java -jar <Path to downloaded Presto CLI jar>/presto-cli-executable.jar --server https://<AE proxy master node hostname>:9295 --insecure --catalog ampoolproxy --schema default --user <Instance name provided while registering secret key on Ampool proxy> --password

Note: On executing above command, a prompt would ask for Password. Here, provide Secret key generated on Ampool proxy.
=====================================================================================================================================
