# Hu-Fu: Efficient and Secure Spatial Queries over Data Federation

## Features

Hu-Fu is the first system for efficient and secure spatial queries over data federation based on secure multi-party computation technique. The system can parse the federated spatial query written in SQL, decompose the query into (plaintext/secure) operators and collect the query result securely.  In particular, the features of Hu-Fu are summarized as follows.

* **Supporting five federated spatial queries:** Hu-Fu supports five mainstream federated spatial queries as below.
  * Federated Range Query
  * Federated Range Counting
  * Federated kNN
  * Federated Distance Join
  * Federated kNN Join

* **A user-friendly SQL interface:** User can issue a federated spatial query by a SQL statement.
* **Multiple parties(>=2):** Hu-Fu can support multiple parties in data federation.
* **Heterogeneous underlying spatial database:** Hu-Fu can adapt to heterogeneous underlying spatial database, e.g. PostgreSQL(PostGIS), Simba, GeoMesa and SpatialHadoop, MySQL, SpatiaLite.
* **High efficiency:** Compared with existing systems (SMCQL and Conclave),  Hu-Fu has a better performance in both running time and communication cost.

## Requirements

* ubuntu 18.04
* Apache Maven 3.6.0+
* Java 8
* PostgreSQL 10
* PostGIS 2.4
* Python 3.6+

## Installation

* Install PostgreSQL (PostGIS)

  ``````bash
  sudo apt-get install postgresql-10
  sudo apt-get install postgis
  ``````

* Create user for postgreSQL and create database

* Add postGIS support for the database

  ```bash
  psql -d {Database Name} -f {Path of PostGIS}/postgis.sql 
  psql -d {Database Name} -f {Path of PostGIS}/spatial_ref_sys.sql
  ```

* Install Java , Maven and Python

  ```bash
  sudo apt-get install openjdk-8-jdk
  sudo apt-get install maven
  sudo apt-get install python3.6
  ```

* Clone the git repository

  ```bash
  git clone xxx.git
  ```

* Compile and package the source code

  ```bash
  cd {Path of Repository}/
  ./package.sh
  ```

## Setup

### Executing example queries

* Import the data

  ```bash
  cd {Path of Repository}/hufu-example/data-importer/postgresql
  python importer.py  # If a package is missing, install it leveraging 'pip'.
  ```

* Replace the strings within `< >` in `hufu-example/data-importer/postgresql/schema.json` and `hufu-example/server/configx.json`

* Start up servers

  ```bash
  cd {Path of Repository}/hufu-example
  ./start_server.sh 1 2 3 4
  ```

* Start up CLI

  ```bash
  cd {Path of Repository}/hufu-core
  ./start.sh
  ```

  * Federated Range Query

    ```sql
    hufu> SELECT id FROM tablea WHERE DWithin(location, Point(116.5, 39.9), 0.2);
    ```

  * Federated Range Counting

    ```sql
    hufu> SELECT COUNT(*) FROM tablea WHERE DWithin(location, Point(116.5, 39.9), 0.2);
    ```

  * Federated kNN

    ```sql
    hufu> SELECT id FROM tablea WHERE KNN(location, Point(116.5, 39.9), 8);
    ```

  * Federated Distance Join

    ```sql
    hufu> SELECT R.id, S.id FROM tableb R JOIN tablea S ON DWithin(R.location, S.location, 0.02);
    ```

  * Federated kNN Join

    ```sql
    hufu> SELECT R.id, S.id FROM tableb R JOIN tablea S ON KNN(R.location, S.location, 8);
    ```

* Stop servers

  ```bash
  cd {Path of Repository}/hufu-example
  ./stop_server.sh
  ```

### Executing  queries on your own spatial data

If you want to execute queries on your own spatial data, you should modify some configuration files as follows.

* `hufu-example/data-importer/postgresql/schema.json`:  You can modify this file to change the **original spatial data and table schema**.
* `hufu-example/server/configx.json`:  You can modify the **secure level** of each table in servers.
* `hufu-core/src/main/resources/model.json`:   You can change **the number of silos** in the data federation by modifying this file.

### Running Hu-Fu on different physical machines

It is very easy to deploy Hu-Fu on different physical machines. You only need to start up the server on their respective machines using the script `hufu-example/server/start_server.sh` and modify the ip address in `hufu-core/src/main/resources/model.json`. Note that the server port should be open on each machine.

### Running Hu-Fu on heterogeneous underlying spatial database

To simplify the installation process, the underlying databases of all silos are PostgreSQL(PostGIS) in example. Recall that Hu-Fu can support heterogeneous underlying spatial database. We also provide the adapters for other five spatial databases. You can install these spatial databases by referring to the documentations as below.

* [simba](http://www.cs.utah.edu/~dongx/simba/)
* [GeoMesa](https://www.geomesa.org/)
* [SpatialHadoop](http://spatialhadoop.cs.umn.edu/)
* [MySQL](https://dev.mysql.com/doc/refman/8.0/en/spatial-types.html)
* [SpatiaLite](https://www.gaia-gis.it/fossil/libspatialite/home)

## References

1. **[Hu-Fu](Hu-Fu2021.pdf):** 
2. **SMCQL:** Johes Bater, Gregory Elliott, Craig Eggen, Satyender Goel, Abel N. Kho, and Jennie Rogers. SMCQL: Secure Query Processing for Private Data Networks.*PVLDB* 10, 6 (2017), 673–684.
3. **Conclave:** Nikolaj Volgushev, Malte Schwarzkopf, Ben Getchell, Mayank Varia, Andrei Lapets, and Azer Bestavros. 2019. Conclave: secure multi\-party computation onbig data. In *EuroSys*. 3:1–3:18.
