# Práctica de Laboratorio: Ecosistema Hadoop

**Asignatura:** Arquitectura de Servicios de Red  
**Universidad:** Universidad Pontificia Comillas (ICAI)  
**Fecha:** 24 de noviembre de 2026  
**Alumno:** Andrés Sánchez (`ASJL`)  
**Profesor responsable:** Guillermo Muñoz Salgado (`gmunoz@comillas.edu`)  

---

## 1. Descubriendo y analizando el entorno de trabajo (Sección 3)

### 3.1 Hadoop GUI / API (Ambari): Clúster `asr-hadoop`

**Evidencias guardadas:**
- [`logs/01_cluster_info.txt`](logs/01_cluster_info.txt)
- [`logs/02_ambari_hosts.json`](logs/02_ambari_hosts.json)
- [`logs/02_hosts_summary.txt`](logs/02_hosts_summary.txt)
- [`logs/03_namenode_ha_state.txt`](logs/03_namenode_ha_state.txt)

#### Pregunta 1: ¿Qué nodos componen el clúster (nombre), qué rol desempeñan y qué configuración hardware (CPU, RAM, Disco) tienen?

El clúster `asr-hadoop` (HDInsight 5.1.8.42, Ubuntu 18, dominio común `.scjzde1xinlelkhkjfhbkggqif.bx.internal.cloudapp.net`) está compuesto por **17 nodos** agrupados en tres roles:

| Rol | Nodos (cantidad) | CPU (Cores) | RAM | Disco (`/` + `/mnt`) | Función principal |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Head / Master (`hn`)** | **2 nodos:**<br>• `hn0-asr-ha...` (`10.0.0.17`)<br>• `hn1-asr-ha...` (`10.0.0.18`) | 4 | 31.35 GB | **221.74 GB**<br>(`/` 123.87 GB + `/mnt` 97.87 GB) | Nodos maestros en Alta Disponibilidad (HA). Ejecutan los servicios de control y gestión (NameNode, ResourceManager, HiveServer2, Hive Metastore, Oozie, History Server, WebHCat, etc.). |
| **Worker / Slave (`wn`)** | **12 nodos:**<br>• `wn0` (`10.0.0.4`), `wn1` (`10.0.0.6`), `wn2` (`10.0.0.8`), `wn3` (`10.0.0.9`), `wn6` (`10.0.0.24`), `wn10` (`10.0.0.15`), `wn11` (`10.0.0.23`), `wn12` (`10.0.0.13`), `wn13` (`10.0.0.25`), `wn14` (`10.0.0.5`), `wn15` (`10.0.0.7`), `wn16` (`10.0.0.22`) | 8 | 62.80 GB | **319.67 GB**<br>(`/` 123.87 GB + `/mnt` 195.80 GB) | Nodos trabajadores encargados del almacenamiento distribuido (**DataNode**) y del procesamiento/cómputo (**NodeManager**), además de alojar clientes (Hive, Oozie, Tez). |
| **ZooKeeper (`zk`)** | **3 nodos:**<br>• `zk0-asr-ha...` (`10.0.0.10`)<br>• `zk2-asr-ha...` (`10.0.0.16`)<br>• `zk3-asr-ha...` (`10.0.0.14`) | 2 | 3.83 GB | **143.39 GB**<br>(`/` 123.87 GB + `/mnt` 19.52 GB) | Nodos de coordinación y quórum (**ZooKeeper Server**) y registro compartido de ediciones de HDFS (**JournalNode**) para la alta disponibilidad. |

---

#### Pregunta 2: ¿Sobre qué nodos están desplegados cada uno de los componentes que conforman HDFS, YARN y Hive?

- **HDFS:**
  - **Active NameNode:** `hn1-asr-ha...` *(verificado mediante `hdfs haadmin -getAllServiceState`)*
  - **Standby NameNode:** `hn0-asr-ha...`
  - **ZKFailoverController (`ZKFC`):** `hn0-asr-ha...` y `hn1-asr-ha...`
  - **JournalNode (3):** `zk0-asr-ha...`, `zk2-asr-ha...`, `zk3-asr-ha...`
  - **DataNode (12):** los 12 nodos worker (`wn0`, `wn1`, `wn2`, `wn3`, `wn6`, `wn10`–`wn16`)
  - **HDFS Client (2):** `hn0-asr-ha...` y `hn1-asr-ha...`

- **YARN:**
  - **Active ResourceManager:** `hn1-asr-ha...`
  - **Standby ResourceManager:** `hn0-asr-ha...`
  - **App Timeline Server (`APP_TIMELINE_SERVER`):** `hn1-asr-ha...` (`STARTED`) y `hn0-asr-ha...` (`INSTALLED`)
  - **NodeManager (12):** los 12 nodos worker (`wn0`, `wn1`, `wn2`, `wn3`, `wn6`, `wn10`–`wn16`)
  - **YARN Client (2):** `hn0-asr-ha...` y `hn1-asr-ha...`

- **Hive:**
  - **Hive Metastore (2):** `hn0-asr-ha...` y `hn1-asr-ha...`
  - **HiveServer2 (`HIVE_SERVER`) (2):** `hn0-asr-ha...` y `hn1-asr-ha...`
  - **Hive Client (14):** `hn0-asr-ha...`, `hn1-asr-ha...` y los 12 nodos worker (`wn0`–`wn16`)

---

### 3.2 Hadoop CLI (Command Line Interface)

**Evidencia guardada:** [`logs/04_core_processes.txt`](logs/04_core_processes.txt)

#### Pregunta 3: ¿Cómo se llaman y dónde corren los procesos que componen el CORE de Hadoop (HDFS + YARN)?

Conectando por SSH con `asr-lab1-bastion_key.pem` y saltando entre un nodo de cada tipo (`hn0-asr-ha`, `hn1-asr-ha`, `wn0-asr-ha`, `zk0-asr-ha`) mediante `ps -ef | grep java`, se observan los siguientes procesos Java del CORE:

| Nodos | Subsistema | Usuario | Nombre del proceso (`-Dproc_...` / clase principal Java) |
| :--- | :--- | :---: | :--- |
| **`hn0-asr-ha` y `hn1-asr-ha`** | **HDFS** | `hdfs` | • `namenode` (`org.apache.hadoop.hdfs.server.namenode.NameNode`)<br>• `zkfc` (`org.apache.hadoop.hdfs.tools.DFSZKFailoverController`) |
| **`hn0-asr-ha` y `hn1-asr-ha`** | **YARN** | `yarn` | • `resourcemanager` (`org.apache.hadoop.yarn.server.resourcemanager.ResourceManager`) |
| **`hn1-asr-ha`** | **YARN** | `yarn` | • `timelineserver` (`org.apache.hadoop.yarn.server.applicationhistoryservice.HDInsightApplicationHistoryServer`) |
| **Workers (`wn0`–`wn16`)** *(verificado en `wn0`)* | **HDFS** | `hdfs` | • `datanode` (`org.apache.hadoop.hdfs.server.datanode.DataNode`) |
| **Workers (`wn0`–`wn16`)** *(verificado en `wn0`)* | **YARN** | `yarn` | • `nodemanager` (`org.apache.hadoop.yarn.server.nodemanager.NodeManager`) |
| **ZooKeeper (`zk0`, `zk2`, `zk3`)** *(verificado en `zk0`)* | **HDFS** | `hdfs` | • `journalnode` (`org.apache.hadoop.hdfs.qjournal.server.JournalNode`) |

---

### 3.3 Configuración de HDFS

**Evidencia guardada:** [`logs/05_hdfs_config.txt`](logs/05_hdfs_config.txt) (consultado con `hdfs getconf -confKey ...` y `/etc/hadoop/conf/hdfs-site.xml`).

#### Pregunta 4: ¿Qué tamaño de bloque tiene el clúster configurado?
- **`dfs.blocksize = 134217728` bytes**, equivalente a **`128 MB`** ($134\,217\,728 / 1024^2 = 128\text{ MB}$).

#### Pregunta 5: ¿Cuántas réplicas por bloque? ¿Es el número que esperabas? ¿A qué puede ser debido?
- **`dfs.replication = 1`** (con `dfs.replication.max = 50`).
- **¿Es el esperado?** **No.** En un clúster Hadoop estándar con 12 DataNodes, el valor habitual es **3 réplicas** por bloque para garantizar tolerancia a fallos y disponibilidad.
- **¿A qué puede ser debido?** Al ser un clúster en **Azure HDInsight** orientado a prácticas/entorno cloud:
  - Se reduce el factor de replicación a `1` para **ahorrar espacio en disco** en los DataNodes y **evitar tráfico de red innecesario** al escribir bloques.
  - La persistencia y redundancia duradera en HDInsight suele delegarse en almacenamiento cloud externo (Azure Storage / `wasb://`), dejando HDFS para uso temporal o secundario.

#### Pregunta 6: ¿Qué período de tiempo para el envío de heartbeats por parte de los datanodos está configurado?
- **`dfs.heartbeat.interval = 3` segundos** (valor por defecto en HDFS).

---

## 2. Carga de Datos y Análisis con Hive (Sección 4)

### 4.1 Carga en HDFS

**Evidencia guardada:** [`logs/06_hdfs_upload.txt`](logs/06_hdfs_upload.txt)

El fichero local `/home/sshuser/HadoopLab/PM_clean.csv` (`124 MB`, `1,011,957` líneas, delimitado por `;` y con línea de cabecera) se ha subido a HDFS en `/HadoopLab/AndresSanchez/`:

```bash
hdfs dfs -mkdir -p /HadoopLab/AndresSanchez
hdfs dfs -chmod 777 /HadoopLab/AndresSanchez
hdfs dfs -put -f /home/sshuser/HadoopLab/PM_clean.csv /HadoopLab/AndresSanchez/
hdfs dfs -ls -h /HadoopLab/AndresSanchez/
# -rw-r--r--   1 sshuser supergroup    123.3 M 2026-11-24 07:34 /HadoopLab/AndresSanchez/PM_clean.csv
```

---

### 4.2 Hive: Creación de base de datos, tablas y comparativa CSV vs ORC

**Evidencias guardadas:**
- Script inicial: [`logs/setup_asjl.hql`](logs/setup_asjl.hql) → Salida: [`logs/07_hive_setup.txt`](logs/07_hive_setup.txt)
- Pregunta 7: [`logs/pregunta7.hql`](logs/pregunta7.hql) → Salida: [`logs/08_hive_pregunta7.txt`](logs/08_hive_pregunta7.txt)
- Pregunta 8: [`logs/pregunta8.hql`](logs/pregunta8.hql) → Salida: [`logs/09_hive_pregunta8.txt`](logs/09_hive_pregunta8.txt)

Se creó la base de datos **`ASJL`**, la tabla externa `pm_csv_ext` (`STORED AS TEXTFILE`, `FIELDS TERMINATED BY ';'`, `LOCATION '/HadoopLab/AndresSanchez/'`, `TBLPROPERTIES ('skip.header.line.count'='1')`) y la tabla `pm_orc` mediante `CREATE TABLE pm_orc AS SELECT * FROM pm_csv_ext;`.

Al inspeccionar `DESCRIBE FORMATTED pm_orc;`, se comprueba que en Hive 3.1.2 (HDInsight) la tabla se crea automáticamente en formato **`ORC`** (`OrcSerde` / `OrcInputFormat`), ocupando **`20.75 MB`** (`21,760,170` bytes en `311` ficheros) para las `1,011,956` filas, frente a los `123.3 MB` del CSV original.

---

#### Pregunta 7: Agregación de una columna en CSV (`pm_csv_ext`) vs ORC (`pm_orc`)

Consulta ejecutada: `SELECT AVG(rx_drop_ratio) FROM <tabla>;` (resultado: `763282.1371766367`).

| Tabla | Formato (`SerDe`) | Tamaño | Tiempo ejecución Hive (`Time taken`) | Tiempo total cliente |
| :--- | :--- | :---: | :---: | :---: |
| **`pm_csv_ext`** | `TEXTFILE` (CSV `;`) | `123.3 MB` (`1` fichero) | **`39.367 s`** | `40.787 s` |
| **`pm_orc`** | `ORC` (`OrcSerde`) | `20.75 MB` (`311` ficheros) | **`13.102 s`** | `14.571 s` |

- **¿Qué consulta tarda menos y por qué?**
  La consulta sobre **`pm_orc`** tarda casi **3 veces menos** (`13.102 s` frente a `39.367 s`):
  1. **Almacenamiento columnar:** En `pm_csv_ext` (orientado a filas en texto plano), Hive debe leer y parsear las 25 columnas de cada línea para extraer `rx_drop_ratio`. En **ORC**, al ser columnar, solo lee los bloques correspondientes a `rx_drop_ratio` (*column projection*).
  2. **Menor volumen de E/S:** La tabla ORC ocupa ~6 veces menos (`20.75 MB` frente a `123.3 MB`), reduciendo el tráfico de lectura desde el almacenamiento.
  3. **Vectorización y metadatos internos:** ORC almacena estadísticas por *stripe* y permite ejecución vectorizada nativa sin tener que convertir texto a `DOUBLE` en cada registro.

---

#### Pregunta 8: Generación de un par de tablas grandes para observar diferencias de tamaño y tiempo

Para apreciar mejor la diferencia tanto en **tamaño** como en **tiempo de consulta**, se generaron dos tablas ampliadas a **`5,059,780` filas** (multiplicando `pm_csv_ext` × 5 mediante `LATERAL VIEW EXPLODE(ARRAY(1, 2, 3, 4, 5))`):
1. **`pm_text_big`**: `STORED AS TEXTFILE` con separador `;`.
2. **`pm_orc_big`**: `STORED AS ORC` con compresión explícita `TBLPROPERTIES ('orc.compress'='ZLIB')`.

Resultados obtenidos al ejecutar `SELECT AVG(rx_drop_ratio), COUNT(*) FROM <tabla>;`:

| Métrica | `pm_text_big` (`TEXTFILE`) | `pm_orc_big` (`ORC` + `ZLIB`) | Mejora |
| :--- | :---: | :---: | :---: |
| **Filas (`numRows`)** | `5,059,780` | `5,059,780` | Idénticas |
| **Ficheros (`numFiles`)** | `8` | `39` | — |
| **Tamaño total (`totalSize`)** | **`641,593,745` B (`611.87 MB`)** | **`40,658,478` B (`38.77 MB`)** | **15.8× más pequeño** (−93.7% espacio) |
| **Tiempo ejecución Hive** | **`35.867 s`** | **`4.684 s`** | **7.66× más rápido** |
| **Tiempo total cliente** | `43.454 s` | `6.721 s` | **6.47× más rápido** |

Se observa de forma contundente cómo el formato columnar comprimido (`ORC` + `ZLIB`) reduce el espacio en casi un **94%** y acelera la consulta de agregación en **7.66 veces**.

---

## 3. Comentarios generales (Sección 5)

#### Pregunta 9: Parte de la práctica que menos te ha aportado
La parte que menos me ha aportado (y que se hace más tediosa) ha sido la de **consultar e inventariar los recursos del clúster** (revisar nodo por nodo la configuración de CPU, RAM, disco, qué componentes corren en cada máquina y listar los procesos Java con `ps -ef`). Aunque sirve para hacerse una idea inicial de la topología de Hadoop/HDInsight en alta disponibilidad, resulta muy repetitiva y mecánica (sobre todo al haber 12 nodos *worker* idénticos) y no aporta tanto valor práctico como trabajar directamente con los datos.

#### Pregunta 10: Parte de la práctica que más te ha aportado
La parte más interesante y útil ha sido la de **trabajar con las tablas en Hive y comparar los formatos de almacenamiento (`TEXTFILE` frente a `ORC`)**. Ver de forma práctica cómo al pasar de un fichero plano CSV a un formato columnar comprimido como ORC (y especialmente al escalar la tabla a ~5 millones de filas) el espacio ocupado se reduce casi **16 veces** (`611.87 MB` → `38.77 MB`) y el tiempo de una consulta de agregación baja de **`35.867 s` a `4.684 s`** (casi **8 veces más rápido**) permite entender de verdad por qué el formato de almacenamiento es clave en arquitecturas Big Data.
