Two .csv files serve as input for the abstraction. Stages of the program:
1. Read data from both files in separate PCollection <String>
2. Convert the previous PCollection <String> to PCollection <KV <Integer, Iterable <Integer> >> and group by key (patient id)
3. join two tables into one PCollection <String>
4. Convert PCollection <String> to PCollection <KV <Integer, Iterable <Integer> >> without one column patient_id and group by key (patient id).
