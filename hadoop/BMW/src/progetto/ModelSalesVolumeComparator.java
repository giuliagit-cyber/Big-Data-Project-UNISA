/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package progetto;

import java.util.Comparator;

/**
 *
 * @author giuli
 */
public class ModelSalesVolumeComparator implements Comparator<ModelSalesVolume>{
    // comparator per la classe ModelSalesVolume
    @Override
    public int compare(ModelSalesVolume o1, ModelSalesVolume o2) {
        int cmp = Integer.compare(o2.getSalesVolume(), o1.getSalesVolume());
        if(cmp!=0) return cmp;
        
        return o1.getModel().compareToIgnoreCase(o2.getModel());
    }
    
}
