package service;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class StringToKVForPatientStatus extends DoFn<String, KV<String, String>> {
    @DoFn.ProcessElement
    public void processElement(ProcessContext c){
        String input = c.element().trim();
       if (!input.equals("patient_id;facility_number")){
           String [] list = input.split(";");
           c.output(KV.of(list[0], list[1]));
       }

    }

}
