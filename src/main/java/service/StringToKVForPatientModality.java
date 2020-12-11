package service;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class StringToKVForPatientModality extends DoFn<String, KV<String, String>> {
    @DoFn.ProcessElement
    public void processElement(ProcessContext c){
        String input = c.element();

        if (!input.equals("modality_code;patient_id")){
            String [] list = input.split(";");
            c.output(KV.of(list[1], list[0]));
    }
    }
}
