package utils;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class PipelineUtil {
    public static Pipeline createPipeline (String[] args){
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);
        return pipeline;
    }


    public static PCollection<String> readCSVFile (Pipeline pipeline, String path){
        PCollection pCollection = pipeline.apply(
                TextIO.read().from(path));
        return pCollection;
    }

    public static PCollection<KV<String, String>> transformToKVPCollection(PCollection<String> pCollection, DoFn<String, KV<String, String>> doFn){
        PCollection<KV<String, String>> pTransformCollection = pCollection.apply(ParDo.of(doFn));
        return pTransformCollection;
    }
    public static void writeToCSVFile (PCollection<String> pCollection, String path){
        pCollection.apply(TextIO.write().to(path)
                .withNumShards(1).withSuffix(".csv"));
    }
}
