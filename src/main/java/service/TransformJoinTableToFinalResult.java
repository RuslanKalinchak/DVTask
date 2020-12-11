package service;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class TransformJoinTableToFinalResult extends DoFn<String, KV<String, String>> {
    @DoFn.ProcessElement
    public void processElement(ProcessContext c){
        String input = c.element();
        String [] list = input.split(";");
        c.output(KV.of(list[1], list[2]));
    }
}
