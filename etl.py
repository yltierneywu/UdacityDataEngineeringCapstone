import configparser
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import json
import csv
import os
import glob


config = configparser.ConfigParser()
config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']


def create_spark_session():
    '''
    Creates a Spark session
    
    Returns:
    spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def clean_immigration_data(imm_input, cleaned_data):
    '''
    - Loads the json-files from the folder that contains the immigration data
    - Cleans the data
    - Writes the cleaned data as a csv-file in the different folder
    
    Parameters:
    imm_input:   root directory where the immigration data is stored
    cleaned_data:root directory in which the cleaned data will be stored
    '''
    
    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(imm_input):
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))
        print(file_path_list)
        break
    
    dict_df_immigration= {}
    for f in file_path_list:
        year= f[-9: -5]
        print('year= ', year)
        dict_df_immigration[year] = pd.DataFrame(columns=['ID', 'Geslacht', 'LeeftijdOp31December', 'BurgerlijkeStaat', 'Geboorteland', 'Perioden', 'Immigratie_1'])
    
        # Import the json-files with immigration data
        immigration_json= []
        for line in open(f, 'r'):
            immigration_json.append(json.loads(line))
        immigration_dict= immigration_json[0].get('value')
        imm_keys= list(immigration_dict.keys())
    
        for key in imm_keys[0: len(imm_keys)]:
             if immigration_dict.get(key).get('Immigratie_1') != None and immigration_dict.get(key).get('Immigratie_1') > 0 \
                and int(immigration_dict.get(key).get('LeeftijdOp31December')) < 19906 \
                and immigration_dict.get(key).get('Geslacht') != 'T001038' and immigration_dict.get(key).get('LeeftijdOp31December') != '10000' \
                and immigration_dict.get(key).get('BurgerlijkeStaat') != 'T001019' \
                and immigration_dict.get(key).get('Geboorteland') != 'T001175':
                #print(key)
                dict_df_immigration[year] = dict_df_immigration[year].append(immigration_dict.get(key), ignore_index=True)
    
    # Create an empty data frame to collate the immigration data of the multiple years
    df_immigration_clean= pd.DataFrame(columns=['ID', 'Geslacht', 'LeeftijdOp31December', 'BurgerlijkeStaat', 'Geboorteland', 'Perioden', 'Immigratie_1'])
    
    # Append the immigration data of each year to the collection dataframe
    for yr in ['2018', '2019', '2020']:
        df_immigration_clean= df_immigration_clean.append(dict_df_immigration[yr])
    
    # Strip whitespaces in one of the columns
    df_immigration_clean['BurgerlijkeStaat']= df_immigration_clean['BurgerlijkeStaat'].apply(lambda x: x.strip())

    print(df_immigration_clean.shape[0])
    
    # Write csv-file with cleaned data
    df_immigration_clean.to_csv(os.path.join(cleaned_data, 'immigration_cleaned.csv'), index= False)
    print('immigration_cleaned_written')

def load_clean_imm_meta_data(meta_data, cleaned_data):
    '''
    - Loads the csv-file that contains the immigration meta data into a dataframe per table
    - Cleans the meta data tables/dataframes
    - Writes the cleaned meta data as csv-files in a different folder
    
    Parameters:
    meta_data:   path to the meta-data file
    cleaned_data:root directory in which the cleaned data will be stored
    '''
    
    # Load the meta data
    df_meta_gender= pd.read_csv(meta_data, sep=';', skiprows= 17, nrows= 3).iloc[:, 0:2]
    df_meta_age= pd.read_csv(meta_data, sep=';', skiprows= 22, nrows= 128).iloc[:,0:2]
    df_meta_marital= pd.read_csv(meta_data, sep=';', skiprows= 152, nrows= 5).iloc[:,0:2]
    df_meta_birthcountry= pd.read_csv(meta_data, sep=';', skiprows= 159, nrows= 263).iloc[:,0:2]
    df_meta_period= pd.read_csv(meta_data, sep=';', skiprows= 424, nrows= 26).iloc[:,0:2]
    
    # Clean the meta data
    df_meta_gender_clean= df_meta_gender.copy()
    df_meta_gender_clean.rename(columns= {'Title' : 'Geslacht', 'Key' : 'GeslachtCode'}, inplace= True)
    df_meta_gender_clean['Gender']= ['Total men and women', 'Men', 'Women']
     
    df_meta_age_clean= df_meta_age.copy()
    df_meta_age_clean.rename(columns= {'Title' : 'Age', 'Key' : 'LeeftijdCode'}, inplace= True)
    df_meta_age_clean.Age.replace({'Totaal': 'Total'}, regex= True, inplace= True)
    df_meta_age_clean.Age.replace({'jaar': 'years'}, regex= True, inplace= True)
    df_meta_age_clean.Age.replace({'of ouder': 'or older'}, regex= True, inplace= True)
    df_meta_age_clean.Age.replace({'tot': 'to'}, regex= True, inplace= True)
 
    df_meta_marital_clean= df_meta_marital.copy()
    df_meta_marital_clean.rename(columns= {'Title' : 'BurgerlijkeStaatNL', 'Key': 'BurgerlijkeStaatCode'}, inplace= True)
    df_meta_marital_clean['MaritalStatus']= ['Total', 'Unmarried', 'Married', 'Remarried', 'Divorced']
    df_meta_marital_clean['BurgerlijkeStaatCode']= df_meta_marital_clean['BurgerlijkeStaatCode'].apply(lambda x: x.strip())
   
    df_meta_birthcountry_clean= df_meta_birthcountry.copy()
    df_meta_birthcountry_clean.rename(columns= {'Key': 'LandCode', 'Title': 'Land'}, inplace= True)
    df_meta_birthcountry_clean.to_csv(os.path.join(cleaned_data, 'meta_birthcountry.csv'), index= False)
    
    df_meta_period_clean= df_meta_period.copy()
    df_meta_period_clean.rename(columns= {'Key': 'PeriodeCode', 'Title': 'Year'}, inplace= True)
    
    # Writes the cleaned meta data into separate csv-files
    df_meta_gender_clean.to_csv(os.path.join(cleaned_data, 'meta_gender.csv'), index= False)
    df_meta_age_clean.to_csv(os.path.join(cleaned_data, 'meta_age.csv'), index= False)
    df_meta_marital_clean.to_csv(os.path.join(cleaned_data, 'meta_marital.csv'), index= False)
    df_meta_birthcountry_clean.to_csv(os.path.join(cleaned_data, 'meta_birthcountry.csv'), index= False)
    df_meta_period_clean.to_csv(os.path.join(cleaned_data, 'meta_period.csv'), index= False) 
       
def process_immigration_data(spark, cleaned_data, country_translation, output_data):
    '''
    - Loads the cleaned immigration and immigration-meta data
    - Loads the csv-file with the translation of the countries
    - Transforms the immigration data, immigration-meta data and country translation into the fact immigration table
    - Writes the fact immigration table as parquet file to output_data partitioned by year
    
    Parameters:
    spark:       the spark session
    cleaned_data:root directory where the cleaned data is stored
    country_translation: path to the file with the country translations
    output_data: location (S3 bucket) where output data will be stored
    '''
    # get filepath to immigration data
    immigration_data = os.path.join(cleaned_data, 'immigration_cleaned.csv')
    #immigration_data = "cleaned/immigration_cleaned.csv"
    
    # read immigration data files
    df = spark.read.option("header", "true").csv(immigration_data)
    print("immigration_data loaded")
    #df.printSchema()
        
    # extract columns to create immigration table
    immigration_table = df.select('ID', 'Geslacht', 'LeeftijdOp31December', 'BurgerlijkeStaat', 'Geboorteland', 'Perioden', 'Immigratie_1') \
                    .dropDuplicates(['ID'])
    
    # load the cleaned meta data
    meta_gender= spark.read.option("header", "true").csv('cleaned/meta_gender.csv')
    print('meta_gender loaded')
    meta_age= spark.read.option("header", "true").csv('cleaned/meta_age.csv')
    print('meta_age loaded')
    meta_marital= spark.read.option("header", "true").csv('cleaned/meta_marital.csv')
    print('meta_marital loaded')
    meta_country= spark.read.option("header", "true").csv('cleaned/meta_birthcountry.csv')
    print('meta_country loaded')
    meta_period= spark.read.option("header", "true").csv('cleaned/meta_period.csv')
    print('meta_period loaded')
    
    #print(immigration_table.columns)
    #print(meta_gender.columns)
    #print(meta_age.columns)
    #print(meta_marital.columns)
    #print(meta_country.columns)
    #print(meta_period.columns)
        
    # Load the country translation data
    countries= spark.read.option("header", "true").csv(country_translation)
    print('country translation loaded')
    
    fact_imm_table= immigration_table.join(meta_gender, (immigration_table.Geslacht== meta_gender.GeslachtCode))\
        .join(meta_age, (immigration_table.LeeftijdOp31December== meta_age.LeeftijdCode)) \
        .join(meta_marital, (immigration_table.BurgerlijkeStaat== meta_marital.BurgerlijkeStaatCode)) \
        .join(meta_country, (immigration_table.Geboorteland== meta_country.LandCode)) \
        .join(meta_period, (immigration_table.Perioden== meta_period.PeriodeCode)) \
        .join(countries, (meta_country.Land== countries.Land_Dutch)) \
        .select(['ID', 'Gender', 'Age', 'MaritalStatus', 'Country', 'Year', 'Immigratie_1'])
    print('fact imm table: rows= ', fact_imm_table.count(), 'columns= ', len(fact_imm_table.columns))
    
    # Clean the fact table further
    fact_imm_table= fact_imm_table.withColumn('Immigratie_1', fact_imm_table['Immigratie_1'].cast('integer').alias('Immigratie_1'))
    fact_imm_table= fact_imm_table.withColumn('Year', fact_imm_table['Year'].cast('integer').alias('Year'))
    fact_imm_table= fact_imm_table.withColumnRenamed('Immigratie_1', 'ImmigrationVolume')
    
    fact_imm_table.printSchema()
    fact_imm_table.show(3)
    
    # Data quality checks on fact immigration table
    if fact_imm_table.rdd.isEmpty():
        print('Data quality check failed. Fact immigration table contains empty cells.')
    else: 
        print('Data quality check passed. Fact immigration table does not contain empty cells.')
    
    if fact_imm_table.where(fact_imm_table["ImmigrationVolume"] == 0).count() == 0:
        print('Data quality check passed. Fact immigration table does not contain entries with zero ImmigrationVolume.')
    else:
        print('Data quality check failed. Fact immigration table contains entries with zero ImmigrationVolume.')
           
    # write as parquet file to S3
    fact_imm_table.write.partitionBy("Year").mode("overwrite").parquet(os.path.join(output_data, 'fact_immigration.parquet'))

def process_happiness_data(happiness_path, cleaned_data):
    '''
    - Loads the (raw) happiness data that are stored in a csv-file for each year
    - Transforms the separate csv-files into a single cleaned happiness table
    - Writes the cleaned happiness table to the cleaned_data folder
    
    Parameters:
    happiness_path:   root directory where the happiness data is stored
    cleaned_data:     root directory where the cleaned data will be stored
    '''
    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(happiness_path):
    
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))
        print(file_path_list)
        break
    
    # Create empty dataframe for all the data
    df_happiness= pd.DataFrame()
    
    # List of columns that for the output dataframe
    cols= ['Country', 'Happiness_Score', 'GDP_Score', 'Health_Score', 'Freedom_Score', \
           'Generosity_Score', 'Corruption_Score', 'Year']

    rows= 0
    for f in file_path_list:
        year= f[-8:-4]
        print('year= ', year)
        df_happiness_year= pd.read_csv(f)
        df_happiness_year['Year']= year
        row_year= df_happiness_year.shape[0]
        if year== '2020':
            df_happiness_year.rename(columns= {'Country name': 'Country', 'Ladder score': 'Happiness_Score', \
                                           'Explained by: Log GDP per capita': 'GDP_Score', \
                                           'Explained by: Healthy life expectancy': 'Health_Score', \
                                           'Explained by: Freedom to make life choices': 'Freedom_Score', \
                                           'Explained by: Generosity': 'Generosity_Score', \
                                           'Explained by: Perceptions of corruption': 'Corruption_Score'}, inplace= True)
            df_happiness_year.columns
        elif year== '2019' or year== '2018':
            df_happiness_year.rename(columns= {'Country or region': 'Country', 'Score': 'Happiness_Score', \
                                           'GDP per capita': 'GDP_Score', \
                                           'Healthy life expectancy': 'Health_Score', \
                                           'Freedom to make life choices': 'Freedom_Score', \
                                           'Generosity': 'Generosity_Score', \
                                           'Perceptions of corruption': 'Corruption_Score'}, inplace= True)
            df_hapiness_year= df_happiness_year[cols]
        rows= rows + row_year
        df_happiness= df_happiness.append(df_happiness_year.loc[:, cols])
    print('df_happiness: rows= ', df_happiness.shape[0], 'columns= ', df_happiness.shape[1])
    
    # Data quality check
    if df_happiness.shape[0] == rows:
        print('Data quality check passed. All World Happiness Report entries are included in the cleaned table.')
    else:
        print('Data quality check failed. Cleaned happiness_table contains incorrect number of entries.')
   
    df_happiness.to_csv(os.path.join(cleaned_data, 'happiness.csv'), index= False) 

def write_happiness_data(spark, cleaned_data, output_data):
    '''
    - Loads the cleaned happiness data
    - Writes the dimension happiness table as parquet file to output_data partitioned by year
    
    Parameters:
    spark:       the spark session
    cleaned_data:root directory where the cleaned data is stored
    output_data: location (S3 bucket) where output data will be stored
    '''
    # Load cleaned happiness data
    df_happiness= spark.read.option("header", "true").csv(os.path.join(cleaned_data, 'happiness.csv'))
    
    # Correct the data types
    df_happiness= df_happiness.withColumn('Happiness_Score', df_happiness['Happiness_Score'].cast('float').alias('Happiness_Score'))
    df_happiness= df_happiness.withColumn('year', df_happiness['year'].cast('integer').alias('year'))
    df_happiness.printSchema()
    
    # Write as parquet file to S3
    df_happiness.write.partitionBy('year').mode("overwrite").parquet(os.path.join(output_data, 'happiness.parquet'))
    
def main():
    '''
    - Calls the function create_spark_session to create a Spark session
    - Then calls the functions clean_immigration_data, load_clean_imm_meta_data, process_immigration_data,
    process_happiness_data and write_happiness_data
    '''
    print('step 1')
    spark = create_spark_session()
    
    imm_input= 'Immigration_data/'
    cleaned_data= 'cleaned/'
    meta_data= 'input_other/03742_metadata.csv'
    country_translation= 'input_other/countries_translation.csv'
    happiness_path= 'WorldHappinessReport/'
    output_data = "s3a://ylwbucket2/"
     
    print('step 2')
    clean_immigration_data(imm_input, cleaned_data)
    print('step 3')
    load_clean_imm_meta_data(meta_data, cleaned_data)
    print('step 4')
    process_immigration_data(spark, cleaned_data, country_translation, output_data)  
    print('step 5')
    process_happiness_data(happiness_path, cleaned_data)
    print('step 6')
    write_happiness_data(spark, cleaned_data, output_data)
    print('step 7')
    
if __name__ == "__main__":
    main()
