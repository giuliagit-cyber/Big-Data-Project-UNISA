/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package progetto;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 *
 * @author giuli
 */
public class Reducer2BMW extends  
        Reducer<NullWritable,
            ModelSalesVolume, 
            Text,
            IntWritable>{
            
    private int k;
    @Override
    protected void setup(Context context){
        Configuration conf = context.getConfiguration();
        String K = conf.get("top.k");
        k = Integer.parseInt(K);
    }
    @Override 
    protected void reduce(NullWritable key, Iterable<ModelSalesVolume> values, Context context ) throws IOException, InterruptedException
    {
        List<ModelSalesVolume> topKModels = new LinkedList<>();
        for (ModelSalesVolume value : values) {
            topKModels.add(new ModelSalesVolume(value.getModel(), value.getSalesVolume()));
            topKModels.sort(new ModelSalesVolumeComparator());
        }

        if (topKModels.size() > k) {
            topKModels.subList(k, topKModels.size()).clear();
        }

        for (ModelSalesVolume d : topKModels) {
            context.write(new Text(d.getModel()), new IntWritable(d.getSalesVolume()));
        }
    }
}
    
