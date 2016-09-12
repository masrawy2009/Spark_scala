

//Create a SQL Context from Spark context
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

//............................................................................
////   Working with Data Frames
//............................................................................

//Create a data frame from a JSON file
val empDf = sqlContext.read.json("customerData.json")
empDf.show()
empDf.printSchema()

//Do SQL queries
empDf.select("name").show()
empDf.filter(empDf("age") === 40 ).show()
empDf.groupBy("gender").count().show()
empDf.groupBy("deptid").agg(avg("salary"), max("age")).show()

//create a data frame from a list
val deptList = Array("{'name': 'Sales', 'id': '100'}",
     "{ 'name':'Engineering','id':'200' }")
//Convert list to RDD
val deptRDD = sc.parallelize(deptList)
//Load RDD into a data frame
val deptDf = sqlContext.read.json(deptRDD)
deptDf.show()
 
//join the data frames
 empDf.join(deptDf, empDf("deptid") === deptDf("id")).show()
 
//cascading operations
empDf.filter(empDf("age") >30).join(deptDf, 
        empDf("deptid") === deptDf("id")).
        groupBy("deptid").
        agg(avg("salary"), max("age")).show()

//register a data frame as table and run SQL statements against it
empDf.registerTempTable("employees")
sqlContext.sql("select * from employees where salary > 4000").show()

//............................................................................
////   Working with Databases
//............................................................................
//Make sure that the spark classpaths are set appropriately in the 
//spark-defaults.conf file to include the driver files
    
val demoDf = sqlContext.read.format("jdbc").options(
    Map("url" -> "jdbc:mysql://localhost:3306/demo",
    "driver" -> "com.mysql.jdbc.Driver",
    "dbtable" -> "demotable",
    "user" ->"root",
    "password" -> "")).load()
    
demoDf.show()

//............................................................................
////   Creating data frames from RDD
//............................................................................

val lines = sc.textFile(datadir + "/auto-data.csv")
//remove the first line
val datalines = lines.filter(x =>  ! x.contains("FUELTYPE"))
datalines.count()

val rowRDD = datalines.map(x => x.split(",")).map(
                    x => (x(0),x(4),x(7)) )

val autoDF = rowRDD.toDF("make","type","hp")
autoDF.select("make","hp").show()
