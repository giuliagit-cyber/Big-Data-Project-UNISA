/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package progetto;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 *
 * @author giuli
 */
public class Reducer1BMW extends  
        Reducer<Text, 
            IntWritable,
            Text,
            IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int total = 0;
        for (IntWritable value : values){
            total = total + value.get();
        }
        
        context.write(new Text(key),new IntWritable(total));
    }
}
    

