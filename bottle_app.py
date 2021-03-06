
# A very simple Bottle Hello World app for you to get started with...
from bottle import default_app

from bottle import route, request, response, get, static_file
import os, pandas as pd, numpy as np
from sklearn import preprocessing
from io import BytesIO
# binami sudo pip install pandas xlrd openpyxl sklearn tables xlsxwriter
# pyhthonanywhere.com pip install --user pandas xlrd openpyxl sklearn tables xlsxwriter

def anonyxel(excel_df,worksheet,id_column,outcome_column):
###########################################################
# Anonymisation happens here
###########################################################

    #set non-data columns aside for later
    holdout_df=excel_df.loc[:,[id_column,outcome_column]]

    #drop non-data columns from the dataset
    dataframe=excel_df.drop(columns=[id_column,outcome_column])

    # helper function for hashing strings and integers
    #https://docs.python.org/2/library/hashlib.html
    import hashlib # TODO check what hash function is optimal
    salt=os.urandom(hashlib.blake2b.SALT_SIZE)
    def hash_to_string(cell_value):

        try:
            m = hashlib.blake2b(salt) # we need to initialize it every time
            m.update(cell_value.encode('utf8')) # string
            return m.hexdigest()
        except:
            m = hashlib.blake2b(salt) # we need to initialize it every time
            m.update(bytes(cell_value)) # integer or float
            return m.hexdigest()

    # numbers(integers) excluding float-> are first hashed to a string,
    # afterwards categorical/string columns are encoded, floats are scaled.
    # TODO add tokenisation for the text cells
    for col in dataframe.select_dtypes(include=[np.number]
    , exclude=[np.float64]).columns:
            dataframe[col] = dataframe[col].map(hash_to_string).astype(np.object)

    for col in dataframe.select_dtypes(exclude=[np.number]).columns:
            dataframe[col] = dataframe[col].astype('category').cat.codes

    for col in dataframe.select_dtypes(include=[np.float64]).columns:
            dataframe[col] = pd.DataFrame(preprocessing.scale(dataframe[col].values)
                                      ,index=dataframe[col].index, columns=[col,])

    # and fill all missing data values with the most frequent value
    dataframe=dataframe.fillna(dataframe.mode().iloc[0])
    # rename columns
    dataframe.columns=[ 'column_'+str(clmn_num) for clmn_num,clmn_name
                            in enumerate(dataframe.columns) ]

    # construct full dataset but now with hashed IDs
    dataframe['Hashed_ID']=holdout_df[id_column].map(hash_to_string)
    dataframe['Clear_Outcome']=holdout_df[outcome_column]
    dataframe = dataframe.reindex(['Hashed_ID','Clear_Outcome']
              + [a for a in dataframe.columns if a
                    not in ['Hashed_ID','Clear_Outcome']], axis=1)

    ###########################################################
    # Anonymisation done
    ###########################################################

    # Now we add the hashed ids to the original ones and build a map.
    # With this map you can track back the results on your side with a VLOOKUP.
    # Do_not share (upload) this file outside of your domain.
    ID_map_df=holdout_df[[id_column]]
    ID_map_df['Hashed_ID']=dataframe['Hashed_ID']

    return dataframe # send ID_map_df per email


@route('/')
def login():
    return '''
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name = "viewport" content = "width = device-width"> <!-- to take all width of the screen -->

<link rel="apple-touch-icon" sizes="57x57" href="/static/favicons/apple-icon-57x57.png">
<link rel="apple-touch-icon" sizes="60x60" href="/static/favicons/apple-icon-60x60.png">
<link rel="apple-touch-icon" sizes="72x72" href="/static/favicons/apple-icon-72x72.png">
<link rel="apple-touch-icon" sizes="76x76" href="/static/favicons/apple-icon-76x76.png">
<link rel="apple-touch-icon" sizes="114x114" href="/static/favicons/apple-icon-114x114.png">
<link rel="apple-touch-icon" sizes="120x120" href="/static/favicons/apple-icon-120x120.png">
<link rel="apple-touch-icon" sizes="144x144" href="/static/favicons/apple-icon-144x144.png">
<link rel="apple-touch-icon" sizes="152x152" href="/static/favicons/apple-icon-152x152.png">
<link rel="apple-touch-icon" sizes="180x180" href="/static/favicons/apple-icon-180x180.png">
<link rel="icon" type="image/png" sizes="192x192"  href="/static/favicons/android-icon-192x192.png">
<link rel="icon" type="image/png" sizes="32x32" href="/static/favicons/favicon-32x32.png">
<link rel="icon" type="image/png" sizes="96x96" href="/static/favicons/favicon-96x96.png">
<link rel="icon" type="image/png" sizes="16x16" href="/static/favicons/favicon-16x16.png">
<link rel="manifest" href="/static/favicons/manifest.json">
<meta name="msapplication-TileColor" content="#ffffff">
<meta name="msapplication-TileImage" content="/static/favicons/ms-icon-144x144.png">
<meta name="theme-color" content="#ffffff">

<style media="screen" type="text/css">

h1 {
 font-size:200%;
 line-height:1;
}


</style>
<title>Anonyxel</title></head>
<body>

<span style="color:red"> <em> This is a functional demo for testing purposes! Don't upload sensitive data! </em> </span>
<h1>anonyxel
<img src=/static/favicons/android-icon-192x192.png
alt="anonyxel icon" width="42" height="42" align="top">
</h1>

<h2>Excel Sheet Data Anonymizer for Machine Learning.</h2>

<form action="/upload" method="post" enctype="multipart/form-data">
  <p>Select a file: <input type="file" name="upload"
      style="font-weight:bold;"/></p>
  <table>
  <tr>
  <td>Name of the Worksheet where the Data is: </td>
  <td><input type="text" name="worksheet" value="DATA" /></td>
  </tr>
  <tr>
  <td>Name of ID column in that worksheet: </td>
  <td><input type="text" name="id_column" value="ID"/></td>
  </tr>
  <tr>
  <td>Name of column with outcomes (0, 1 or empty for prediction): </td>
  <td><input type="text" name="outcome_column" value="Outcome" /></td>
  </tr>
  <tr>
  <td></td>
  <td>
  <br><input type="submit" value="Upload file and run Anonyxel"
      style="font-weight:bold;" /></td>
  </tr>
  </table>
</form>


<h2>Purpose</h2>
<i>
<p>Anonyxel takes DATA_FILE.xlsx with ID, Outcome and some data columns
in the DATA worksheet.

<p>It hashes IDs, keeps Outcome(0/1) cleartext.
Numbers(integers) excluding float are first hashed to a string.
Afterwards all categorical and string columns are encoded into levels.
Floats are scaled.

<p>The results are returned to you as DATA_FILE.anon.xlsx.
A download should start automatically.

<p>An ID map for joining back to the original data is written into
DATA_FILE.id_map.xlsx and sent to you via e-mail. (TODO)
</p>

<p> An <a href=https://github.com/Motorrat/anonyxel/raw/master/Adult-Dataset-Example.xlsx>example xlsx file</a> you can use for testing</p>
</i>

<p><a href=https://github.com/Motorrat/anonyxel/blob/master/LICENSE>
License</a>
<p><a href=https://github.com/Motorrat/anonyxel>Source Code, Documentation, Acknowledgements etc.</a>

</body>
</html>
'''


@route('/upload', method='POST')
def do_upload():
    worksheet = request.forms.get('worksheet')
    id_column = request.forms.get('id_column')
    outcome_column = request.forms.get('outcome_column')
    upload     = request.files.get('upload')
    name, ext = os.path.splitext(upload.filename)
    if ext not in ('.xlsx','.xls',):
        return ('Empty' if not ext else ext) + \
        ' file extension not allowed. Only .xlsx and .xls are supported.'
    new_name=name+'.anon'+ext

    dfs = pd.read_excel(upload.file,sheet_name=None)

    excel_df=dfs[worksheet]

    result=anonyxel(excel_df,worksheet,id_column,outcome_column)

    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')

    result.to_excel(writer, sheet_name='Anonymized_DATA', index = False)

    writer.save()
    response.content_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    attch_str='attachment; filename='+new_name
    response.add_header('Content-Disposition', attch_str)
    return output.getvalue()

@get("/static/img/<filepath:re:.*\.(jpg|png|gif|ico|svg|json)>")
def img(filepath):
    return static_file(filepath, root="static/img")


application = default_app()

