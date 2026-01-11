/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package progetto;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 *
 * @author giuli
 */
public class Mapper1BMW extends Mapper< 
        LongWritable, 
        Text,
        Text,   
        IntWritable>
        {
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        // Ignora l'intestazione
        if(key.get() == 0) return;
        
        String s = value.toString().trim();
        String[] field = s.split(",", -1);
        // check sul numero di campi
        if(field.length< 11) return;
        String model = field[0].trim();
        int sales_volume = Integer.parseInt(field[9].trim());
    
        context.write(new Text(model),new IntWritable(sales_volume));


    }
    
}
