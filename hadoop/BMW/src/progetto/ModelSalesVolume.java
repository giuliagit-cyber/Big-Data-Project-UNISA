/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package progetto;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author giuli
 */
public class ModelSalesVolume implements Writable{
    // la classe definisce la coppia modello volume di vendita
    private String model;
    private int sales_volume;
    
    public  ModelSalesVolume(){
        this.model ="";
        this.sales_volume = 0;
    }

    public ModelSalesVolume(String model, int sales_volume){
    
        this.model = model;
        this.sales_volume = sales_volume;
    }
    
    public String getModel(){
        return model;
    }
    
    
    public int getSalesVolume(){
        return sales_volume;
    }
    
    public void setModel(String model){
    
        this.model = model;
    }
    
    public void setSalesVolume(int sales_volume){
        this.sales_volume= sales_volume;
    }
    
    
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeUTF(model);
        d.writeInt(sales_volume);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        model = di.readUTF();
        sales_volume = di.readInt();
       
    }

    @Override
    public String toString() {
        return model + "\t" + sales_volume;
    }
    
    
    
}
