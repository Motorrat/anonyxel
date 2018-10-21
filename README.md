# anonyxel
<pre> usage: anonyxel_cmd.py [-h] [-d DATA_FILE] [-w WORKSHEET] [-i ID_COLUMN]
                       [-o OUTCOME_COLUMN]

Excel Sheet Data Anonymizer for Machine Learning.

Takes DATA_FILE.xlsx with ID, Outcome and some data columns 
in the DATA worksheet. 

<img src=https://github.com/Motorrat/anonyxel/blob/master/Screenshot-AdultDataset-Original.png>

Hashes IDs, keeps Outcome(0/1) cleartext. 
Numbers(integers) excluding float are first hashed to a string.
Afterwards all categorical and string columns are encoded into levels. 
Floats are scaled. 

The Results are written to DATA_FILE.anon.xlsx. 

<img src=https://github.com/Motorrat/anonyxel/blob/master/Screenshot-AdultDataset-Anonymized.png>

An ID map for joining back to the original data is written 
into DATA_FILE.id_map.xlsx

optional arguments:
  -h, --help            show this help message and exit
  -d DATA_FILE, --data_file DATA_FILE
                        Excel File to anonymize with a single worksheet called DATA.
  -w WORKSHEET, --worksheet WORKSHEET
                        Excel worksheet to use. If omitted we will try to use the DATA worksheet.
  -i ID_COLUMN, --id_column ID_COLUMN
                        A column that represents a Unique ID to identify your cases, 
                        must not repeat within the dataset.
  -o OUTCOME_COLUMN, --outcome_column OUTCOME_COLUMN
                        A 0/1 column that represents the outcome of your cases. Empty cells 
                        represent rows/cases that you want to predict. It will not be anonymized 
                        because we need it in clear text. 
</pre>
