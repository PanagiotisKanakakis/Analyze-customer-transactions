import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) throws AnalysisException {

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("recommendations")
                .getOrCreate();

        String events_csv = "/home/myrto/Documents/bigData/datasets/events.csv";
        String category_tree_csv = "/home/myrto/Documents/bigData/datasets/category_tree.csv";

        Dataset events = spark.read().option("header", "true").csv(events_csv);
        Dataset category_tree = spark.read().option("header", "true").csv(category_tree_csv);

        events.createTempView("Events");

        //Statistics stats = new Statistics(spark, events, category_tree);
        //stats.countActions();
        //stats.categoriesByParent();
        //stats.findRootCategories();

        ALS_Recommendation als = new ALS_Recommendation(spark, events);
        als.createVisitorItemRatings();
        als.printVisitorItemRatings();
        als.createRatingsFile();
        als.ALS();
        als.saveAndLoadModel();
    }
}
