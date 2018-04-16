import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) throws AnalysisException {

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("recommendations")
                .getOrCreate();

        Dataset events = spark.read().option("header", "true").csv("/home/konstantina/Documents/big_data/retailrocket-recommender-system-dataset/events.csv");

        events.show();
        events.createTempView("Events");
        //spark.sql("Select count(*) As Views From Events").show();

        //JavaRDD<String> rdd = data.toJavaRDD();

        //rdd.map(r -> r.split(" "));

        //events.createTempView("Kwnna");

        //spark.sql("select distinct count('categoryid') from Kwnna");


    }
}
