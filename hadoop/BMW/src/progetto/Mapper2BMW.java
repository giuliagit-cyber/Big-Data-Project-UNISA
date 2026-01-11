/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package progetto;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 *
 * @author giuli
 */
public class Mapper2BMW extends Mapper< 
        Text, //Modello
        Text,
        NullWritable,   
        ModelSalesVolume>
        {
    

    private int k;
    private List<ModelSalesVolume> topK;

    @Override
    protected void setup(Context context){
        Configuration conf = context.getConfiguration();
        //passaggio di k
        String kp = conf.get("top.k");
        k= Integer.parseInt(kp);
        // Lista dei modelli più venduti
        topK = new LinkedList<>();
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException{
        // lettura dei record
        String model = key.toString();
        // vulume di vendita
        int total = Integer.parseInt(value.toString());
        ModelSalesVolume curr = new ModelSalesVolume(model,total);
        // inserimento del modello
        topK.add(curr);
        // sorting della lista 
        topK.sort(new ModelSalesVolumeComparator());

        // se nella lista ci sono piu di k elementi quelli in piu vengono rimossi
        if(topK.size() > k){
            topK.subList(k, topK.size()).clear();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException{
        // scrittura dei risultati
        for(ModelSalesVolume value : topK){
            context.write(NullWritable.get(), value);
        }

    }
    
}
