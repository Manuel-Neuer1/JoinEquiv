# JoinEquiv

# Bug List

This section details the bugs identified by our framework JoinEquiv. Each reported issue includes a link to the corresponding bug report or GitHub issue where available.

### MySQL Bugs

*   **[MySQL Bug #118544](https://bugs.mysql.com/bug.php?id=118544)**

*   **[MySQL Bug #118548](https://bugs.mysql.com/bug.php?id=118548) Intended**

*   **[MySQL Bug #118684](https://bugs.mysql.com/bug.php?id=118684)**

*   **[MySQL Bug #118689](https://bugs.mysql.com/bug.php?id=118689) Intended**

*   **[MySQL Bug #118710](https://bugs.mysql.com/bug.php?id=118710) Fixed**

*   **[MySQL Bug #118857](https://bugs.mysql.com/bug.php?id=118857)**

*   **[MySQL Bug #118858](https://bugs.mysql.com/bug.php?id=118858)**

*   **[MySQL Bug #118949](https://bugs.mysql.com/bug.php?id=118949)**

*   **[MySQL Bug #119032](https://bugs.mysql.com/bug.php?id=119032)**

*   **[MySQL Bug #119059](https://bugs.mysql.com/bug.php?id=119059)**

### TiDB Bugs
*   **[TiDB Bug #62380](https://github.com/pingcap/tidb/issues/62380)**

*   **[TiDB Bug #62444](https://github.com/pingcap/tidb/issues/62444) Fixed**

*   **[TiDB Bug #62456](https://github.com/pingcap/tidb/issues/62456) Fixed**

*   **[TiDB Bug #62459](https://github.com/pingcap/tidb/issues/62459)**

*   **[TiDB Bug #62460](https://github.com/pingcap/tidb/issues/62460)**

*   **[TiDB Bug #62644](https://github.com/pingcap/tidb/issues/62644) Fixed**

*   **[TiDB Bug #62645](https://github.com/pingcap/tidb/issues/62645) Fixed**

*   **[TiDB Bug #62689](https://github.com/pingcap/tidb/issues/62689)**

*   **[TiDB Bug #63596](https://github.com/pingcap/tidb/issues/63596)**

*   **[TiDB Bug #63601](https://github.com/pingcap/tidb/issues/63601) tlp**

*   **[TiDB Bug #63635](https://github.com/pingcap/tidb/issues/63635)**

*   **[TiDB Bug #63636](https://github.com/pingcap/tidb/issues/63636)**

*   **[TiDB Bug #63736](https://github.com/pingcap/tidb/issues/63736)**

### Percona Bugs
*   **[Percona #10124](https://perconadev.atlassian.net/browse/PS-10124)**

*   **[Percona #10127](https://perconadev.atlassian.net/browse/PS-10127)**

*   **[Percona #535](https://perconadev.atlassian.net/browse/DISTMYSQL-535)**

# 🚀 Getting Started JoinEquiv
## 🧩 Minimum Requirements
* Java 11 or above
* [Maven](https://maven.apache.org/)
* Ubuntu 20.04.6 LTS or above
* [Docker](https://docs.docker.com/get-docker/) (recommended for setting up database environments)

## 🐳 Recommended Setup: Run Databases via Docker
We recommend using **Docker** to quickly deploy the tested DBMSs used by JoinEquiv.  
The following examples demonstrate how to pull and run **MySQL**, **Percona**, and **TiDB** using official Docker images.

### 🐬 MySQL
- **Docker Hub:** [https://hub.docker.com/_/mysql](https://hub.docker.com/_/mysql)

```bash
# Pull the MySQL 9.2.0 image
docker pull mysql:9.2.0

# Run the MySQL container
docker run -d \
  --name mysql920 \
  -e MYSQL_ROOT_PASSWORD=1234 \
  -p 3306:3306 \
  mysql:9.2.0
run --name my-mysql -e MYSQL_ROOT_PASSWORD=1234 -p 3306:3306 -d mysql:latest
```
This will start a MySQL server accessible at localhost:3306 with username root and password 1234.

### 🐬 Percona
- **Docker Hub:** [https://hub.docker.com/r/percona/percona-server](https://hub.docker.com/r/percona/percona-server)

```bash
# Pull the Percona Server 8.4.5 image
docker pull percona/percona-server:8.4.5

# Run the Percona container
docker run -d \
  --name percona845 \
  -e MYSQL_ROOT_PASSWORD=1234 \
  -p 3309:3306 \
  percona/percona-server:8.4.5
```
This will start a Percona Server instance accessible at localhost:3309 with username root and password 1234

### 🐬 TiDB
- **Docker Hub:** [https://hub.docker.com/r/pingcap/tidb](https://hub.docker.com/r/pingcap/tidb)

```bash
# Pull the TiDB 7.5.1 image
docker pull pingcap/tidb:v7.5.1

# Run the TiDB container
docker run -d \
  --name tidb751 \
  -e MYSQL_ROOT_PASSWORD=1234 \
  -p 4000:4000 \
  pingcap/tidb:v7.5.1
```
This will start a TiDB instance accessible at localhost:4000 with username root and password 1234


Before using JoinEquiv, you need to download the repository to obtain JoinEquiv-13CB.zip, then follow these commands to create the JAR and start JoinEquiv
```shell
unzip JoinEquiv-13CB.zip
cd joinequiv
mvn clean package -DskipTests
cd target
java -jar joinequiv-*.jar --num-threads 4 --username root --password your_password --host <database_host> --port <database_listen_port> <database_name> --oracle JOIN
# e.g.: java -jar joinequiv-*.jar --num-threads 4  --username root --password 1234 --host localhost --port 3306 mysql --oracle JOIN
# e.g.: java -jar joinequiv-*.jar --num-threads 4  --username root --password 1234 --host localhost --port 4000 tidb --oracle JOIN
# e.g.: java -jar joinequiv-*.jar --num-threads 4  --username root --password 1234 --host localhost --port 3309 percona --oracle JOIN
```
JoinEquiv stores logs in the target/logs subdirectory, results in every SQL statement that is sent to the DBMS being logged. The corresponding file names are postfixed with `-cur.log`. In addition, if JoinEquiv detects a logic bug(Sometimes joinEquiv throws an exception because the generated sql statement asserts incorrectly, which is not a DBMS bug.), it creates a file with the extension `.log`, in which the statements to reproduce the bug are logged, including only the last query that was executed along with the other statements to set up the database state.

Alternatively, you can run the script file `./findbug.sh <dbms>` to automatically detect the full path of the `.log` files where JoinEquiv found potential bugs.
```bash
./findbug <dbms> # e.g., ./findbug mysql
```
