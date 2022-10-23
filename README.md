# notebook911

from pyspark.sql import SparkSession
path="/FileStore/tables/police911-1.csv"
spark=SparkSession.builder.appName("911Datas").getOrCreate()
dataDF=spark.read.option("Header",True).option("inferSchema",True).csv(path)
dataDF.show()
+----------+--------------------+--------+--------+----------------+----------+-------------------+--------------------+
|  recordId|        callDateTime|priority|district|     description|callNumber|   incidentLocation|            location|
+----------+--------------------+--------+--------+----------------+----------+-------------------+--------------------+
|   2166363|01/18/2017 11:08:...|  Medium|      NE|      DISORDERLY|P170180959|    6700 HARFORD RD|   6700 HARFORD RD\n|
| BALTIMORE|                  MD|    null|    null|            null|      null|               null|                null|
|(39.363752|        -76.551576)"|    null|    null|            null|      null|               null|                null|
|   3185563|01/25/2018 02:51:...|  Medium|      NW|NARCOTICSOutside|P180251626|3800 W BELVEDERE AV|3800 W BELVEDERE ...|
| BALTIMORE|                  MD|    null|    null|            null|      null|               null|                null|
|(39.344787|        -76.681868)"|    null|    null|            null|      null|               null|                null|
|   3185458|01/25/2018 02:09:...|     Low|      CD|     INVESTIGATE|P180251510|     1500 ARGYLE AV|    1500 ARGYLE AV\n|
| BALTIMORE|                  MD|    null|    null|            null|      null|               null|                null|
| (39.30145|        -76.633917)"|    null|    null|            null|      null|               null|                null|
|   2167138|01/18/2017 04:20:...|     Low|      CD|   DESTRUCT PROP|P170181854|      1200 EUTAW PL|     1200 EUTAW PL\n|
| BALTIMORE|                  MD|    null|    null|            null|      null|               null|                null|
| (39.30315|        -76.625416)"|    null|    null|            null|      null|               null|                null|
|   3185682|01/25/2018 03:43:...|    High|      SD|    SILENT ALARM|P180251791|  1100 NANTICOKE ST| 1100 NANTICOKE ST\n|
| BALTIMORE|                  MD|    null|    null|            null|      null|               null|                null|
|(39.281211|        -76.630329)"|    null|    null|            null|      null|               null|                null|
|   2166404|01/18/2017 11:27:...|     Low|      NW|  FALSE PRETENSE|P170181008|     5100 WABASH AV|    5100 WABASH AV\n|
| BALTIMORE|                  MD|    null|    null|            null|      null|               null|                null|
|(39.342074|         -76.68704)"|    null|    null|            null|      null|               null|                null|

dataDF.printSchema()
root
 |-- recordId: string (nullable = true)
 |-- callDateTime: string (nullable = true)
 |-- priority: string (nullable = true)
 |-- district: string (nullable = true)
 |-- description: string (nullable = true)
 |-- callNumber: string (nullable = true)
 |-- incidentLocation: string (nullable = true)
 |-- location: string (nullable = true)

newDataDF=dataDF.withColumn("recordId_Integer",col("recordId").cast("Integer"))
from pyspark.sql.functions import *
newDataDF.printSchema()
root
 |-- recordId: string (nullable = true)
 |-- callDateTime: string (nullable = true)
 |-- priority: string (nullable = true)
 |-- district: string (nullable = true)
 |-- description: string (nullable = true)
 |-- callNumber: string (nullable = true)
 |-- incidentLocation: string (nullable = true)
 |-- location: string (nullable = true)
 |-- recordId_Integer: integer (nullable = true)

newDataDF_2=newDataDF.drop("recordId","location")
newDataDF_2.printSchema()
root
 |-- callDateTime: string (nullable = true)
 |-- priority: string (nullable = true)
 |-- district: string (nullable = true)
 |-- description: string (nullable = true)
 |-- callNumber: string (nullable = true)
 |-- incidentLocation: string (nullable = true)
 |-- recordId_Integer: integer (nullable = true)

DF1,DF2=newDataDF_2.randomSplit([0.1,0.9])
DF1.count()
Out[25]: 876913
dataDFNonNull=dataDF.dropna()
dataDFNonNull.show()
+--------+--------------------+--------+--------+----------------+----------+--------------------+--------------------+
|recordId|        callDateTime|priority|district|     description|callNumber|    incidentLocation|            location|
+--------+--------------------+--------+--------+----------------+----------+--------------------+--------------------+
| 2166363|01/18/2017 11:08:...|  Medium|      NE|      DISORDERLY|P170180959|     6700 HARFORD RD|   6700 HARFORD RD\n|
| 3185563|01/25/2018 02:51:...|  Medium|      NW|NARCOTICSOutside|P180251626| 3800 W BELVEDERE AV|3800 W BELVEDERE ...|
| 3185458|01/25/2018 02:09:...|     Low|      CD|     INVESTIGATE|P180251510|      1500 ARGYLE AV|    1500 ARGYLE AV\n|
| 2167138|01/18/2017 04:20:...|     Low|      CD|   DESTRUCT PROP|P170181854|       1200 EUTAW PL|     1200 EUTAW PL\n|
| 3185682|01/25/2018 03:43:...|    High|      SD|    SILENT ALARM|P180251791|   1100 NANTICOKE ST| 1100 NANTICOKE ST\n|
| 2166404|01/18/2017 11:27:...|     Low|      NW|  FALSE PRETENSE|P170181008|      5100 WABASH AV|    5100 WABASH AV\n|
| 3185683|01/25/2018 03:38:...|     Low|      SD|  INVEST TROUBLE|P180251766|   200 FRANKFURST AV| 200 FRANKFURST AV\n|
| 3185520|01/25/2018 02:39:...|  Medium|      ED|  COMMON ASSAULT|P180251592|     2700 E CHASE ST|   2700 E CHASE ST\n|
| 2167498|01/18/2017 06:19:...|  Medium|      SW|      DISORDERLY|P170182265|1000 POPLAR GROVE ST|1000 POPLAR GROVE...|
| 1241136|03/04/2016 07:21:...|    High|      CD|    Traffic Stop|P160642513|E LAFAYETTE AV/ST...|E LAFAYETTE AV ST...|
|  416724|05/27/2015 04:57:...|  Medium|      WD|  COMMON ASSAULT|P151472211|   1300 N CALHOUN ST| 1300 N CALHOUN ST\n|
|  409521|05/25/2015 09:33:...|     Low|     TRU|  LARCENY F/AUTO|P151450652|     6600 PIONEER DR|   6600 PIONEER DR\n|
| 3185725|01/25/2018 03:51:...|  Medium|      WD|  COMMON ASSAULT|P180251808|     300 N PAYSON ST|   300 N PAYSON ST\n|
|  562162|07/13/2015 10:51:...|     Low|      CD|   AUTO ACCIDENT|P151941017|W LOMBARD ST/S GR...|W LOMBARD ST S GR...|
|  409970|05/25/2015 01:20:...|    High|      NE|   AUDIBLE ALARM|P151451131|       4000 WILKE AV|     4000 WILKE AV\n|
|  702350|08/27/2015 01:51:...|  Medium|      CW|     Lab Request|P152391557|   100 N MONTFORD AV| 100 N MONTFORD AV\n|
|  702625|08/27/2015 03:26:...|  Medium|      ED|  INJURED PERSON|P152391887|     1800 ORLEANS ST|   1800 ORLEANS ST\n|
|  702458|08/27/2015 02:31:...|  Medium|      SW|NARCOTICS ONVIEW|P152391693|  1900 BLK HARMAN AV|1900 BLK HARMAN AV\n|
|  702339|08/27/2015 01:47:...|  Medium|      ED|   911/NO  VOICE|P152391553|     3800 PULASKI HY|      3800 PULASKI\n|
|  702532|08/27/2015 02:56:...|    High|      ED|  AGGRAV ASSAULT|P152391782|2300 BLK E OLIVER ST|2300 BLK E OLIVER...|
+--------+--------------------+--------+--------+----------------+----------+--------------------+--------------------+
only showing top 20 rows

dataDFNonNull.count()
Cancelled
CountedDFByPriority=dataDFNonNull.groupBy("priority").count()
CountedDFByPriority.display()
Cancelled
dataDFNonNull.withColumn("newDateTime",to_date("callDateTime")).printSchema()
root
 |-- recordId: string (nullable = true)
 |-- callDateTime: string (nullable = true)
 |-- priority: string (nullable = true)
 |-- district: string (nullable = true)
 |-- description: string (nullable = true)
 |-- callNumber: string (nullable = true)
 |-- incidentLocation: string (nullable = true)
 |-- location: string (nullable = true)
 |-- newDateTime: date (nullable = true)

dataDFwithTimeStamp=dataDFNonNull.withColumn("newDateTime", to_timestamp("callDateTime","MM/dd/yyyy hh:mm:ss a")).select("newDateTime","callDateTime")
dataDFwithTimeStamp.display()
dataDFwithDateTime=dataDFwithTimeStamp.withColumn("date_time",to_date("newDateTime") )
dataDFwithDateTime.display()
dataDFwithDateTime.groupBy("date_time").count().show()
dataDFwithDateTime.groupBy("date_time").count().orderBy("count",ascending=False).show()
dataDFwithTimeStamp.withColumn("hour_time",hour("newDateTime")).display()
dataDFWithHour=dataDFwithTimeStamp.withColumn("hour_time",hour("newDateTime"))
dataDFwithTimeStamp.withColumn("year_time",year("newDateTime")).select("year_time").groupBy("year_time").count().display()
dataDFwithTimeStamp.withColumn("dayOfWeekTime",dayofweek("newDateTime")).select("dayOfWeekTime").display()
dataDFwithTimeStamp.withColumn("quarter_time",quarter("newDateTime")).select("quarter_time").display()
