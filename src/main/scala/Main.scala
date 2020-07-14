import com.databricks.spark.xml._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}



case class countyPlace(ns2referenceTypeName: String, ns2referenceTypeCode :String, ns2resourceIdentifier: String )
case class ExchangeRateArea(ns3objectIdentifier:String,
                            ns3lastModified:String,
                            ns3lastModifiedBy:String,
                            ns3deleted:String,
                            ns3objectCreationDateTime:String,
                            ns2countyPlace:countyPlace,
                            ns2zipCode:String,
                            ns2statePostalCode:String
                           )

object Main extends App {
  var file: String = "/home/deshbandhu/MCIT_BigData/XMLtoCSV/resource/data/ExchangeRateArea/1248393.xml"


  val countyPlace_schema =  StructType(
    List(
      StructField("ns2:referenceTypeName",StringType,nullable = true),
      StructField("ns2:referenceTypeCode",StringType,true),
      StructField("ns2:resourceIdentifier",StringType,true)
    ))
  /*
   .add("ns2:referenceTypeName", StringType)
   .add("ns2:referenceTypeCode", StringType)
   .add("ns2:resourceIdentifier", StringType)
*/
  val usedZipCountyStatePlace_schema = new StructType()
    .add("ns3:objectIdentifier", StringType)
    .add("ns3:lastModified", StringType)
    .add("ns3:lastModifiedBy", StringType)
    .add("ns3:deleted", StringType)
    .add("ns3:objectCreationDateTime", StringType)
    .add("ns2:countyPlace", countyPlace_schema)
    .add("ns2:zipCode", StringType)
    .add("ns2:statePostalCode", StringType)

  val exchangeRateArea_schema = new StructType()
    .add("base:objectIdentifier", StringType)
    .add("b:lastModified", StringType)
    .add("base:lastModifiedBy", StringType)
    .add("base:deleted", StringType)
    .add("base:objectCreationDateTime", StringType)
    .add("exchangeRateAreaIndetifier", StringType)
    .add("exchangeRateAreaName", StringType)
    .add("exchangeRateAreaStartDate", StringType)
    .add("exchangeRateAreaEndDate", StringType)
    .add("constrainingInsuranceMarketLevelType", StringType)



  //=====================================================


  val spark: SparkSession = SparkSession.builder().master("local[*]").appName("XMLtoCSV").getOrCreate()
  import spark.implicits._
  val exchangeRateArea_df = spark.read.option("excludeAttribute","false").option("rowTag", "exchangeRateArea").schema(exchangeRateArea_schema).xml(file) //
  val usedZipCountyStatePlace_df = spark.read.option("excludeAttribute","false").option("rowTag", "usedZipCountyStatePlace").schema(usedZipCountyStatePlace_schema).xml(file) //

  val exchangeRateArea_df_final = exchangeRateArea_df.select(
    $"base:objectIdentifier".as("baseobjectIdentifier"),
    $"b:lastModified".as("blastModified"),
    $"base:lastModifiedBy".as("baselastModifiedBy"),
    $"base:deleted".as("basedeleted"),
    $"base:objectCreationDateTime".as("baseobjectCreationDateTime"),
    $"exchangeRateAreaIndetifier".as("exchangeRateAreaIndetifier"),
    $"exchangeRateAreaName".as("exchangeRateAreaName"),
    $"exchangeRateAreaStartDate".as("exchangeRateAreaStartDate"),
    $"exchangeRateAreaEndDate".as("exchangeRateAreaEndDate"),
    $"constrainingInsuranceMarketLevelType".as("constrainingInsuranceMarketLevelType"))


  val usedZipCountyStatePlace_df_final = usedZipCountyStatePlace_df.select(
    $"ns3:objectIdentifier".as("ns3objectIdentifier"),
    $"ns3:lastModified".as("ns3lastModified"),
    $"ns3:lastModifiedBy".as("ns3lastModifiedBy"),
    $"ns3:deleted".as("ns3deleted"),
    $"ns3:objectCreationDateTime".as("ns3objectCreationDateTime"),
    $"ns2:countyPlace".getField("ns2:referenceTypeName").as("ns2referenceTypeName"),
    $"ns2:countyPlace".getField("ns2:referenceTypeCode").as("ns2referenceTypeCode"),
    $"ns2:countyPlace".getField("ns2:resourceIdentifier").as("ns2resourceIdentifier"),
    $"ns2:zipCode".as("ns2zipCode"),
    $"ns2:statePostalCode".as("ns2statePostalCode"))

val finaltable = exchangeRateArea_df_final.crossJoin(usedZipCountyStatePlace_df_final)//.show(100)


  //exchangeRateArea_df_final.show()
  //usedZipCountyStatePlace_df_final.show()

  //exchangeRateArea_df_final.printSchema()
  //usedZipCountyStatePlace_df_final.printSchema()
  /*
    val list = df1.select($"ns2countyPlace").collectAsList()
    list.forEach(x=>x.get(0))*/
  finaltable.write.format("csv").option("header","True").mode("overwrite").option("sep",",").save("/home/deshbandhu/MCIT_BigData/XMLtoCSV/resource/out1/")
  //usedZipCountyStatePlace_df_final.write.format("csv").option("header","True").mode("overwrite").option("sep",",").save("/home/deshbandhu/MCIT_BigData/XMLtoCSV/resource/out2/")
}
