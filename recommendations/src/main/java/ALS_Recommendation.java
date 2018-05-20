import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

public class ALS_Recommendation {
    SparkSession spark;
    Dataset events;

    JavaPairRDD<Tuple2<String, String>, Double> visitor_item_ratings;
    JavaRDD<Rating> ratings;

    MatrixFactorizationModel model;

    ALS_Recommendation(SparkSession spark, Dataset events) {
        this.spark = spark;
        this.events = events;
    }


    void createVisitorItemRatings() {
        StructType schema = this.events.schema();
        JavaRDD<Row> rdd = this.events.toJavaRDD();
        //Dataset<Row> dataset = spark.createDataFrame(rdd, schema);


        JavaPairRDD<Tuple2<String, String>, Double> visitor_item_ratings = rdd.map(line -> line.toString().split(",")).mapToPair(line -> {
            Tuple2<String, String> tuple = new Tuple2<>(line[1], line[3]);
            //JavaPairRDD key = new JavaPairRDD<String, String>(tuple);
            if (line[2].equals("view")) {
                return new Tuple2<Tuple2<String, String>, Double>(tuple, 0.3);
            } else if (line[2].equals("view")) {
                return new Tuple2<Tuple2<String, String>, Double>(tuple, 0.7);
            } else {
                return new Tuple2<Tuple2<String, String>, Double>(tuple, 1.0);
            }
        })
                .reduceByKey((a, b) -> a + b);

        this.visitor_item_ratings = visitor_item_ratings;
    }

    void printVisitorItemRatings() {
        this.visitor_item_ratings.take(20).forEach(s -> {
            Tuple2<String, String> key = s._1;
            System.out.println("visitor: "+key._1 + ", item: " + key._2 + " - " + s._2);
        });
    }

    void createRatingsFile() {
        JavaRDD<Rating> ratings = this.visitor_item_ratings.map(s -> {
            Tuple2<String, String> key = s._1;
            return new Rating(Integer.parseInt(key._1), Integer.parseInt(key._2), s._2);
        });
        this.ratings = ratings;
    }

    void ALS() {
        // Build the recommendation model using ALS
        int rank = 10;
        int numIterations = 10;
        this.model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);

        // Evaluate the model on rating data
        JavaRDD<Tuple2<Object, Object>> userProducts =
                ratings.map(r -> new Tuple2<>(r.user(), r.product()));
        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
                this.model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD()
                        .map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating()))
        );
        JavaRDD<Tuple2<Double, Double>> ratesAndPreds = JavaPairRDD.fromJavaRDD(
                ratings.map(r -> new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating())))
                .join(predictions).values();
        double MSE = ratesAndPreds.mapToDouble(pair -> {
            double err = pair._1() - pair._2();
            return err * err;
        }).mean();
        System.out.println("Mean Squared Error = " + MSE);
    }

    void saveAndLoadModel() {
        SparkContext sc = this.spark.sparkContext();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

        // Save and load model
        this.model.save(jsc.sc(), "target/tmp/myCollaborativeFilter");
        MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(jsc.sc(),
                "target/tmp/myCollaborativeFilter");
    }

}
