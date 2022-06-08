#!/usr/bin/env python
# coding: utf-8

# ## ProcessBiFilesNotebook_V3
# 
# 
# 

# # ProcessBiFiles (Upload to Curated)
# 
# This notebook will read incoming files from BI Layer, apply data quality checks and move data into the curated layer.

# In[103]:


list_of_files = mssparkutils.fs.ls("abfss://spf-bi-upload@spfmvpsynapsedev00.dfs.core.windows.net/")


# In[119]:


map_2fs_family_df = spark.read.load(f"abfss://{metadata_container}@{storage_account}.dfs.core.windows.net/map_2fs_family.csv", format='csv',header=True, delimiter=',', inferSchema=True )
list_of_fs_id = map_2fs_family_df.select('fs_id').rdd.flatMap(lambda x: x).collect()


# In[111]:


list_of_ctl_files = []
for f in list_of_files:
    if f.name.find('.ctl') != -1:
        list_of_ctl_files.append(f.name)


# In[112]:


list_of_ctl_files


# In[115]:


# storage account parameters
storage_account = "spfmvpsynapsedev00"

# conatiner parameters
upload_container = "spf-bi-upload"
metadata_container = "spf-bi-metadata"
curated_container = "spf-bi-curated"

# file name parameters
# data_filename = ctl_filename.replace('.ctl', '.dat')


# ## Import Libraries

# In[113]:


import pyspark.sql.functions as F # functions to perform operations on dataframes
from pyspark.sql.functions import explode, arrays_zip, col, expr
from pyspark.sql.types import * # all data types supported in PySpark SQL
import json # to parse JSON data
import requests
# from pyspark.sql.functions import explode, arrays_zip, col, expr # to process json data in dataframes
from notebookutils import mssparkutils # perform common tasks in synapse
import logging # for custom logging


# ## Configure Logging

# In[116]:


FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
formatter = logging.Formatter(fmt=FORMAT)
for handler in logging.getLogger().handlers:
    handler.setFormatter(formatter)

# Customize the log level for a specific logger
syn_logger = logging.getLogger('syn-notebook-log')
syn_logger.setLevel(logging.INFO)

# syn_logger.debug("customized debug message")
# syn_logger.info("customized info message")
# syn_logger.warning("customized warning message")
# syn_logger.error("customized error message")
# syn_logger.critical("customized critical message")


# ## Fetch Schema for control and data files from EDC
# 
# #TODO
# - control file schema is still not present in EDC. So defining the control file schema in the notebook based on charset
# - Align with Luba/Aliaksei/Arindam to move this under EDC later

# In[163]:


class EDC:
    '''
    Class for the EDC objects.

    Attributes:
       url: url of corresponding feeder system.
       fs_id: name of the incoming feeder system.
       #TODO: fs_id will be used to resolve the EDC url for corresponding feeder system metadata
       edc_metadata_df: dataframe to store metadata from EDC
    '''
    def __init__(self, fs_id):
        self.url = "https://spf-mvp-edc-connect.azurewebsites.net/api/spf-mvp-edc-connect?code=c3g7p4mZEbo0aeC_KKAHeZAFtnNKsw4Z28ngMpMrxKngAzFu68uyFg=="
        self.fs_id = fs_id
        self.edc_metadata_df = None
        
    def load_data_from_edc(self):
        '''
        This function reads metadata from the EDC endpoint and stores it as dataframe
        '''
        # get response from the url and read the json into a spark dataframe
        resp = requests.get(self.url)

        df = spark.read.json(sc.parallelize([resp.text]))

        # Explode the JSON into facts and id
        df = (df.withColumn('buffer', explode('items'))
                .withColumn('facts', expr("buffer.facts"))
                .withColumn('id', expr("buffer.id"))
                .drop(*['items','metadata','buffer'])
        )

        # Flatten the json attributes into dataframe columns
        df = df.withColumn("Position", expr("filter(facts, x -> x.attributeId = 'com.infa.ldm.relational.Position')")[0]["value"])                                .withColumn("ColumnName", expr("filter(facts, x -> x.attributeId = 'core.name')")[0]["value"])                                .withColumn("Charset", expr("filter(facts, x -> x.attributeId = 'com.infa.ldm.relational.Code')")[0]["value"])                                .withColumn("FieldSeparator", expr("filter(facts, x -> x.attributeId = 'com.infa.ldm.relational.FieldFormat')")[0]["value"])                                .withColumn("DataType", expr("filter(facts, x -> x.attributeId = 'com.infa.ldm.relational.Datatype')")[0]["value"])                                .withColumn("Length", expr("filter(facts, x -> x.attributeId = 'com.infa.ldm.relational.Length')")[0]["value"])                                .withColumn("Scale", expr("filter(facts, x -> x.attributeId = 'com.infa.ldm.relational.Scale')")[0]["value"])

        # sort the df by Position
        df = df.orderBy(col('Position').cast("int").asc())

        # drop the facts and id columns now
        df = df.drop("facts", "id")

        # convert datatypes to lower case
        df = df.withColumn("DataType", F.lower(F.col("DataType")))

        # set the edc_metadata_df attribute
        self.edc_metadata_df = df

    def get_charset(self):
        '''
        Returns charset of the corresponding control file 
        '''
        # get only the file level info from edc df
        df = self.edc_metadata_df.filter(F.col('Position').isNull())
        
        # fetch charset from the dataframe
        charset = df.select('Charset').collect()[0][0]

        return charset

    def get_field_separator(self):
        '''
        Returns the Field Separator to read the incoming .dat file
        '''
        # mapping to read Filed Separators to spark dataframe readable format
        field_separator_mapping = {
                                        "TAB": '\t',
                                        "Space": ' ',
                                        "Pipe": '|',
                                        "Semi-Colon": ";"
                                        }

        # get only the file level info from edc df
        df = self.edc_metadata_df.filter(F.col('Position').isNull())

        # fetch the field separator from the dataframe
        field_separator = df.select('FieldSeparator').collect()[0][0]

        # map the fieldseparotr from edc to spark
        field_separator = field_separator_mapping[field_separator]

        return field_separator

    def get_scehma_for_data_file(self, file_level_info = False):
        '''
        This function returns schema for data file
        Arguemnts:
            self
        Return:
            dat_schema: Schema to read data file
        '''
        # remove the undefined positions (eg: first row)
        df = self.edc_metadata_df.filter(F.col('Position').isNotNull())
        
        dat_schema = StructType([])

        for row in df.collect():
            dat_schema.add(row['ColumnName'], StringType())

        return dat_schema

    def get_schema_for_control_file(self):
        '''
        This function returns the schema to read the control file based on the charset
        Arguemnts:
            self
            charset: charset of the control file
        Return:
            schema: StructType() containing schema to read control file
        '''
        schema = None

        char_set = get_charset()

        if char_set == "ASCII" or char_set == "EBCDIC" or char_set == "CSV_NAT":
            schema = StructType([
                StructField("fs_id", StringType(), True),        
                StructField("delivery", StringType(), True),
                StructField("delivery_number", StringType(), True),
                StructField("previous_delivery_number", StringType(), True),        
                StructField("sum_of_amount", StringType(), True),        
                StructField("number_of_records", StringType(), True)
            ])
        elif char_set == "CSV_INT":
            schema = StructType([
                StructField("fs_id", StringType(), True),
                StructField("pfs_id", StringType(), True),      
                StructField("delivery", StringType(), True),
                StructField("delivery_number", StringType(), True),
                StructField("previous_delivery_number", StringType(), True),        
                StructField("sum_of_amount", StringType(), True),        
                StructField("number_of_records", StringType(), True)
            ])
        syn_logger.info("Returning the schema for the control file")

        return schema

        


# In[ ]:


class Validator:
    '''
    Class for the Validator objects.

    Attributes:
  
    '''
    def __init__(self, fs_id):
        self.url = ""
        self.fs_id = fs_id
        self.edc_metadata_df = None
        
    def load_data_from_edc(self):
        '''
        This function reads metadata from the EDC endpoint and stores it as dataframe
        '''

    def columns_check(self):
        '''
        Returns charset of the corresponding control file 
        '''
        return charset

    def length_check(self):
        '''
        Returns the Field Separator to read the incoming .dat file
        '''

        return field_separator

    def get_scehma_for_data_file(self, file_level_info = False):
        '''
        '''
        # remove the undefined positions (eg: first row)
        df = self.edc_metadata_df.filter(df.Position != "")
        
        return df

    def get_schema_for_control_file(self):
        '''
        '''
        return schema

        


# In[164]:


edc_instances_per_fs_id = {}


# In[165]:


# create one edc instance for one fs_id
for ctl_file in list_of_ctl_files:
    fs_id = ctl_file[:ctl_file.index('_')]
    if fs_id not in edc_instances_per_fs_id:
        edc_instances_per_fs_id[fs_id] = EDC(fs_id)


# In[166]:


for ctl_file in list_of_ctl_files:
    fs_id = ctl_file[:ctl_file.index('_')]
    edc_instance = edc_instances_per_fs_id[fs_id]


# In[167]:


edc_obj = edc_instances_per_fs_id["ABS"]


# In[169]:


edc_obj.load_data_from_edc()


# In[170]:


edc_obj.get_charset()


# In[171]:


edc_obj.get_field_separator()


# In[173]:


edc_obj.get_scehma_for_data_file()


# In[ ]:


# read control file


# In[ ]:


# read data file


# In[68]:


df = edc_obj.get_data_from_url(schema = True)


# In[ ]:


display(df)


# In[46]:


Charset = df.filter( (F.col('Position').isNull())).select('Charset').collect()[0][0]


# In[53]:


FieldSeparator = df.filter( (F.col('Position').isNull())).select('FieldSeparator').collect()[0][0]


# In[ ]:


#fetch the schema to read control file
ctl_schema = get_schema_for_control_file()


# In[ ]:


# Read edc schema JSON file into dataframe
edc_file = f"abfss://{metadata_container}@{storage_account}.dfs.core.windows.net/edc_schema.json"
edc_schema_df = spark.read.json(edc_file)

# Explode the JSON into facts and id
edc_schema_df = (edc_schema_df.withColumn('buffer', explode('items'))
         .withColumn('facts', expr("buffer.facts"))
         .withColumn('id', expr("buffer.id"))
         .drop(*['items','metadata','buffer'])
)

# Further faltten the json attributes into dataframe columns
edc_schema_df = edc_schema_df.withColumn("Position", expr("filter(facts, x -> x.attributeId = 'com.infa.ldm.relational.Position')")[0]["value"])                         .withColumn("Header", expr("filter(facts, x -> x.attributeId = 'core.name')")[0]["value"])                         .withColumn("DataType", expr("filter(facts, x -> x.attributeId = 'com.infa.ldm.relational.Datatype')")[0]["value"])                         .withColumn("Length", expr("filter(facts, x -> x.attributeId = 'com.infa.ldm.relational.Length')")[0]["value"])                         .withColumn("Scale", expr("filter(facts, x -> x.attributeId = 'com.infa.ldm.relational.Scale')")[0]["value"])

# sort the df by Position
edc_schema_df = edc_schema_df.orderBy(col('Position').cast("int").asc())

# drop the facts and id columns now
edc_schema_df = edc_schema_df.drop("facts", "id")

# convert datatypes to lower case
edc_schema_df = edc_schema_df.withColumn("DataType", lower(col("DataType")))

# remove the undefined positions (eg: first row)
edc_schema_df = edc_schema_df.filter(edc_schema_df.Position != "")


# In[ ]:


# display(edc_schema_df.filter(edc_schema_df.DataType == "number"))


# In[ ]:


# datafile_df.select(col("BUDAT"), to_date(col("BUDAT"), "yyyymmdd")).show()


# In[ ]:


# display(datafile_df.select("BUDAT", "BELDAT"))


# In[ ]:


# display(datafile_df.select("HWBTRG", "TWBTRG", "BASISBETRAG", "STEUERPROZENT"))


# In[ ]:


# data_type_mapping = {
#             "binary": BinaryType(),
#             "boolean": BooleanType(),
#             "tinyint": ByteType(),
#             "char": StringType(),
#             "date": StringType(),
#             "number": DecimalType(),
#             "decimal": DecimalType(),
#             "double": DoubleType(),
#             "float": FloatType(),
#             "int": IntegerType(),
#             "long": LongType(),
#             "short": ShortType(),
#             "string": StringType(),
#             "varchar": StringType(),
#             "varchar2": StringType(),
#             "datetime": TimestampType()
#         }

dat_schema = StructType([])

for row in edc_schema_df.collect():
    # print(row['Header'] + "," + str(data_type_mapping[row['DataType']]) )
    dat_schema.add(row['Header'], StringType())


# In[ ]:


syn_logger.info("Reading data file as string columns: %s", data_filename)
datafile_df = spark.read.load(f"abfss://{upload_container}@{storage_account}.dfs.core.windows.net/{data_filename}", 
                                format = 'csv', header = False, schema = dat_schema, delimiter = '\t' )
syn_logger.info("Loaded data file as string columns: %s", data_filename)


# In[ ]:


# datafile_df.printSchema()
display(edc_schema_df.filter(edc_schema_df.DataType == "date"))


# In[ ]:


df = datafile_df


# In[ ]:


# datafile_df

rejected_cols = []
for column_name, _ in df.dtypes:
    actual_length = edc_schema_df.filter(edc_schema_df.Header == column_name).select("Length").collect()[0][0]
    # print(column_name, actual_length)
    if actual_length:
        df_filtered = df.withColumn(column_name, regexp_replace(column_name, ',', '')).filter( (F.length(F.col(column_name))  <= actual_length) | (F.col(column_name).isNull()) )
        if(df.count() != df_filtered.count()):
            rejected_cols.append(column_name)
if len(rejected_cols) == 0:
    print("success")
else:
    print("fail")


# In[ ]:


print(rejected_cols)


# In[ ]:


display(edc_schema_df.filter(edc_schema_df.Header == "STEUERPROZENT"))


# In[ ]:


# df.filter( (F.length(F.col('STEUERPROZENT')) >= 7 ) | (F.col('STEUERPROZENT').isNull()) ).count()
display(df.withColumn('STEUERPROZENT', regexp_replace('STEUERPROZENT', ',', '')).filter( (F.length(F.col('STEUERPROZENT')) <= 7 ) | (F.col('STEUERPROZENT').isNull())).select('STEUERPROZENT') )
# display(df.select('FS_IC_BUKRS'))


# In[ ]:


# df.select("FS_MANDT").show()
df.filter(F.length(F.col('FS_MANDT')) <= 8).select("FS_MANDT").count()


# In[ ]:


def length_check(df, edc_df):
    df

