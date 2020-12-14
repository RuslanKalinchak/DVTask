import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import service.StringToKVForPatientModality;
import service.StringToKVForPatientStatus;
import service.TransformJoinTableToFinalResult;
import utils.PipelineUtil;

public class Main {
    // розбити клініки на групи в яких обслуговуються пацієнти з однаковими модаліті
    // як побудувати запрос в якому найти всі клініки в яких всі пацієнти мають такі ж рівні модаліті

    public static void main(String[] args) {

        Pipeline pipeline = PipelineUtil.createPipeline(args);
        String pathToPatientModalityDoc = "F:\\Idea Projects\\DVTask\\PATIENT_MODALITY.csv";
        String pathToPatientStatusDoc = "F:\\Idea Projects\\DVTask\\PATIENT_STATUS.csv";

        // step 1 - create 2 origin PCollection
        PCollection<String> patientModality = PipelineUtil.readCSVFile(pipeline, pathToPatientModalityDoc);
        PCollection<String> patientStatus = PipelineUtil.readCSVFile(pipeline, pathToPatientStatusDoc);

        // step 2 - create and ordering PCollection <KV<Integer, Integer>>
        PCollection<KV<String, String>> patientModalityKV = PipelineUtil.transformToKVPCollection(patientModality, new StringToKVForPatientModality());
        PCollection<KV<String, String>> patientStatusKV = PipelineUtil.transformToKVPCollection(patientStatus,new StringToKVForPatientStatus());

        // step 3 - join many to one PCollection
        final TupleTag<String> patientModalityTupleTag = new TupleTag<>();
        final TupleTag<String> patientStatusTupleTag = new TupleTag<>();
        PCollection<KV<String, CoGbkResult>> tableWithCoGbkResult
                = KeyedPCollectionTuple.of(patientModalityTupleTag, patientModalityKV)
                .and(patientStatusTupleTag, patientStatusKV)
                .apply(CoGroupByKey.<String>create());

        PCollection<String> joinTable = tableWithCoGbkResult.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>(){
            @DoFn.ProcessElement
            public void processElement (ProcessContext c){
                String strKey = String.valueOf(c.element().getKey());
                CoGbkResult valObject = c.element().getValue();
                Iterable<String> patientModalityTable = valObject.getAll(patientModalityTupleTag);
                Iterable<String> patientStatusTable  = valObject.getAll(patientStatusTupleTag);
                for (String patientModalityNumber: patientModalityTable) {
                    for (String facilityNumber: patientStatusTable) {
                        c.output(strKey+";"+patientModalityNumber+";"+facilityNumber); }
                }
            }
        }));

        //step 4 - formatted and group final PCollection
        PCollection<KV<String, Iterable<String>>> resultTable = joinTable.apply(ParDo.of(new TransformJoinTableToFinalResult()))
                .apply(GroupByKey.<String, String>create());
        final TupleTag<Iterable<String>> resultTupleTag = new TupleTag<>();

        PCollection<KV<String, CoGbkResult>> castTable
                = KeyedPCollectionTuple.of(resultTupleTag, resultTable)
                .apply(CoGroupByKey.<String>create());

        PCollection<String> resultReadyToPrintTable = castTable.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>(){

            @DoFn.ProcessElement
            public void processElement (ProcessContext c){
                String strKey = String.valueOf(c.element().getKey());
                CoGbkResult valObject = c.element().getValue();
                Iterable<Iterable<String>> resultPeriodTable = valObject.getAll(resultTupleTag);

                for (Iterable<String> var: resultPeriodTable) {
                    c.output(strKey+";"+var); }
            }
        }));
        String resultFilePath = "F:\\Idea Projects\\DVTask\\result.csv";
        PipelineUtil.writeToCSVFile(resultReadyToPrintTable, resultFilePath);
        pipeline.run();}
}

