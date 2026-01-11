/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package sparkcar;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 *
 * @author giuli
 *  MINICHIELLO GIULIA 
 * 0622702127
 */


public class SparkBMWDriver {
    public static void main(String[] args) {
        
        
        // path dei file di input e output
        String inputPath  = args[0];
        String outputPath = args[1];

        // configurazione Spark
        SparkConf conf = new SparkConf().setAppName("Regioni GREEN");
        // configurazione contesto
        JavaSparkContext sc = new JavaSparkContext(conf);

        // inserimento file di input nell'RDD
        JavaRDD<String> lines = sc.textFile(inputPath);
        // applicazione del filtro che seleziona le auto in base all'anno (>= 2015)
        JavaRDD<String> car2015 = lines.filter(logLine -> {
            String[] p = logLine.split(",", -1);
            if (p.length < 11) return false;
            return Integer.parseInt(p[1].trim()) >= 2015;
        });



        // filtro per individuare le auto elettriche tra quelle dal 2015
        JavaRDD<String> electric = car2015.filter(logLine -> {
            String[] p = logLine.split(",", -1);
            return p[4].trim().toLowerCase().contains("electric");
        });

        // Creazione delle coppie (regione, vendite) 
        //implementato con la trasformazione mapToPair
        JavaPairRDD<String, Integer> electricByRegion = electric.mapToPair(logLine -> {
            String region;
            int sales=0;
            String[] p= logLine.split(",", -1);
            region = p[2].trim();
            sales = Integer.parseInt(p[9].trim());
            return new Tuple2<String,Integer>(region,sales);
        });

        // somma vendite per regione
        // implementata con la trasformazione reduceByKey
        JavaPairRDD<String, Integer> electricByRegionReduced = electricByRegion
            .reduceByKey((i1,i2) -> i1 + i2);

        // inversione Stringa intero per fare la sort
        // usata per poter usare la sortByKey per la nuova PairRDD
        JavaPairRDD<Integer, String> electricByRegionSorted = electricByRegionReduced
            .mapToPair((Tuple2<String, Integer> inPair) -> new Tuple2<>(inPair._2(), inPair._1()));

        // uso della trasformazione sortByKey per ordinare in ordine decrescente
        JavaPairRDD<Integer, String> electricByRegionSortedDesc = electricByRegionSorted
            .sortByKey(false);

            // salvataggio sul file di output
        electricByRegionSortedDesc.saveAsTextFile(outputPath);
        System.out.println("ho salvato il file di output in " + outputPath);

        // chiusura del context Spark
        sc.close();
        
    }
}