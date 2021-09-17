# Udacity Data Engineering Nanodegree Project 5: Capstone project

## Project summary:
This is my capstone project of the Udacity Data Engineering Nanodegree. The choice was given to complete the project with data provided by Udacity, or to define your own scope and data. In both cases the same outline was to be followed, being: 
1) Gather data
2) Explore and Assess the data
3) Define the Data model
4) Run ETL to Model the Data
5) Write-up the project
I decided to define my own scope and data. I chose to combine immigration data from the Netherlands with data from the yearly World Happiness Report and make a Data model such that questions could be answered like:
- Is there a link between the happiness score and the number of immigrants with that country of birth?
- What are the demographics of people immigrating into the Netherlands? 
- Is here a trend in the immigration into the Netherlands? If so, what kind of? Volume of people? Country of origin? Gender? Age
The final Data Model was a star schema with the immigration data as the fact table and the happiness data as the dimension table. Because the immigration data was provided with coded entries, tables with meta-data were required to transform the immigration into information in English. The country of birth in the immigration data was given with the Dutch naming of the countries. To enable comparison with the Happiness Report data, a table with the translation of the countries was required. 

The tools I used were Spark and the final tables will be written to a bucket in S3. Spark was selected because there are more than 1 million entries and Spark is suitable for processing big data. However, I didn't manage to fully leverage Spark because I didn't manage to read the json-files due to its nested nature. Advice is welcome. The final tables will be written to a bucket in S3 to make it publically accessible for querying.

## Step 1: Data gathering
Two data sets plus corresponding metadata were gathered:
1. The immigration data was gathered through the dataportal "Dataportaal" of Centraal Bureau voor de Statistiek (CBS) which is the Central Bureau of Statistics of the Netherlands. The webpage was https://opendata.cbs.nl/portal.html?_la=nl&_catalog=CBS&tableId=03742&_theme=66. The data was downloaded through "Onbewerkte dataset"= raw dataset, and by selecting the years of interest. The file format selected was json. The data gathered ranged from 2018 up and until 2020. Each year was in a separate json-file. The data included: ID number, gender, age on 31st of December, marital status, country of birth and period/year.
2. Metadata of the immigration dataset was gathered from the same website. It was manually transformed in tables containing keys to convert the codes used in the immigration data into English. As an example, the gender was either T001038, 3000 or 4000 which translate into "total of both males and females", male or female.
3. World Happiness Report data from the years 2018 up and until 2020 were gathered from Kaggle https://www.kaggle.com/datasets https://www.kaggle.com/unsdsn/world-happiness. Each year was in a separate csv-file. The data contained at least: country name, happiness score and parameters that can affect happiness, e.g. life expectancy. 

## Step 2a: Data exploration and assessment
### Immigration data
The immigration data json-file was a dictionary nested in a dictionary that was in a list. There were rows which contained 'None' and entries with '0'= zero immigrants. It also contained both entries with the totals as well as split up, e.g. an entry with the total number of people (men and women) in a year from a certain country, an entry with only the number of men and an entry with only the number of women. There were also aggregates over 5 years of age on top of information per 1-year of age. Zero and 'None' entries were not loaded. Also entries with totals were not loaded since it seemed better to make those aggregations as required rather than having certain people multiple times in the database. Apart from that, there were no duplicates in this data source.

### Immigration metadata
The immigration metadata is all contained in a single csv-file. Certain rows contain information about certain information: age, gender, marital status, country of birth and period over which the immigration had taken place. Each piece of information will be stored in a different table.

For all the meta-data, the column names were not clear/unique. The code was called 'Key', the meaning was called 'Title'. The column names were renamed in the cleaning part.

The age part of the metadata contained codes for the total= all ages, per year of age, and per 5-years of age.

The countries of birth/origin of the immigrants were in Dutch. There were 263 different countries/regions in the 2020 immigration dataset. A separate table with both the Dutch and English country names had to be made to enable joins between the immigration data and the World Happiness Report data.

### Happiness data
There were no duplicate entries and no NA's. 

Combining the data from the various years, certain countries multiple names:
- There is 'Congo (Brazzaville)' and 'Congo (Kinshasa)'
- There is 'Hong Kong' and 'Hong Kong S.A.R. of China'
- There is 'Taiwan' and 'Taiwan Province of China'
- There is 'Trinidad & Tobago' and 'Trinidad and Tobago'
- There is 'North Cyprus' and 'Northern Cyprus' and 'Cyprus'
- There is 'Macedonia' and 'North Macedonia'

## Step 2b: Cleaning steps
### Cleaning - Immigration data
- Don't load entries with none or 0 people in Immigratie_1 by adding two conditions when to append an entry immigration_dict.get(key).get('Immigratie_1') != None and immigration_dict.get(key).get('Immigratie_1') > 0
- For age only load entries that reflect the total or the number per age-year, i.e. not the ones aggregated over a 5-year age range
- Don't load totals for gender (Geslacht), age (Leeftijd),  marital status (BurgerlijkeStaat) or country of birth (Geboorteland)

### Cleaning - df_meta_gender:
- Rename column 'Title' into 'Geslacht'
- Add column 'Gender' with translation of 'Geslacht'

### Cleaning - df_meta_age:
- Rename column 'Title' into 'Age'
- Replace 'totaal' with 'total'
- Replace 'jaar' with'years'
- Replace 'of ouder' with 'or older'
- Replace 'tot' with 'to'

### Cleaning - df_meta_marital:
- Rename column 'Title' into 'BurgerlijkeStaat'
- Add column 'MaritalStatus' with translation of 'BurgerlijkeStaat'
- Remove trailing whitespaces in the 'Key' 

### Cleaning - Happiness data
- Rename 'Congo (Brazzaville)' to 'Congo' 
- Rename 'Congo (Kinshasa)' to 'Congo'
- Rename 'Taiwan Province of China' to 'Taiwan'
- Rename 'Trinidad & Tobago' to 'Trinidad and Tobago'
- Rename 'Northern Cyprus' and 'North Cyprus' to 'Cyprus'
- Rename 'North Macedonia' to 'Macedonia'

## Step 3: Define the Data Model
### 3.1 Conceptual Data Model
The final schema will be a star schema with a fact table and actually only one dimension table as in the ERD below. They are connected by the country. This model was chosen, because it is simple and suitable for the analytical purpose.

![alt text](ERD_final.JPG "Title")

### 3.2 Mapping Out Data Pipelines
The initial ERD looks like the ERD below:

![alt text](ERD_before.JPG "Title")

## Step 4: ETL pipeline
To arrive at the final ERD the following steps are required:
- Load/stage all data
- Clean loaded/staged data
- Create the fact table Immigration from the tables Immigration, MetaGender, MetaAge, MetaMarital, MetaCountry and LandCountry
- The dimension table Happiness is equal to the cleaned table Happiness.

## Step 5: Write-up the project
### How to run the scripts
- First, an IAM user needs to be created that enables Redshift to access S3 bucket. The access key and secret key are entered in the config.cfg file.
- Then, an S3 bucket with public access needs to be created. The bucket name should be entered behind "output_data" in etl.py
- Then, etl.py can be run in a launcher with python etl.py or in a notebook with Python magic !python etl.py
- The final tables should then be available in the S3 bucket for querying.

### Considerations
- It is proposed to update the model yearly because the immigration data is a yearly aggregate and the World Happiness Report is yearly published.
- Spark was (attempted to) use(d) because the data size is in the order of a million entries. Spark is suitable for this size of data. Storing the resulting data model in a bucket in S3 was chosen for its wide accessibility. Because the input data only gets published yearly and is not updated, there was no direct need to use Airflow, which would have been very useful if it was required to update the model regularly.  
- What if the amount of data is 100x as much? Then, you would probably want to run this on a cluster like Amazon EMR. Or at least have Spark reading the json-files.
- What if you would need to run this on a daily basis at 7 am? Then, you would probaly want to rewrite the code such that you can run it on Airflow in which you can schedule it.
- What if the database needs to be accessed by 100+ people at the same time? As long as the S3 bucket is publically accessible, then the current solution would still be suitable.

## Data dictionary
<b> Input tables: </b>
MetaGender table
| col | expected format | description | accepted range of values |
| --- | --- | --- | --- |
| `Geslacht Code` | string | Code for the Gender | T001038, 3000 or 4000 |
| `Geslacht` | string | Gender in Dutch| Totaal mannen en vrouwen, Mannen or Vrouwen |
| `Gender` | string | Gender in English | Total men and women, Men or Women |

MetaAge table
| col | expected format | description | accepted range of values |
| --- | --- | --- | --- |
| `LeeftijdCode` | string | Code relating to the age | Any |
| `Age` | string | Age in years | Any |

MetaMarital table
| col | expected format | description | accepted range of values |
| --- | --- | --- | --- |
| `BurgerlijkeStaatCode` | string | Code relating to the marital status | T001019, 1010, 1020, 1050 or 1080 |
| `BurgerlijkeStaatNL` | string | Marital Status in Dutch|  Totaal burgerlijke staat, Ongehuwd, Gehuwd, Verweduwd or Gescheiden |
| `MaritalStatus` | string | Marital Status |  Total, Unmarried, Married, Remarried or Divorced |

MetaCountry table
| col | expected format | description | accepted range of values |
| --- | --- | --- | --- |
| `LandCode` | string | Code related to the country of birth of the immigrant| Any |
| `Land` | string | Country in Dutch | Any |

MetaPeriod table
| col | expected format | description | accepted range of values |
| --- | --- | --- | --- |
| `PeriodeCode` | string | Code relating for the year of immigration| Any, but in format YYYY'JJ00' |
| `Year` | integer | Year the immigration occurred | 1995- 2020 |

Landcountry table
| col | expected format | description | accepted range of values |
| --- | --- | --- | --- |
| `Land_Dutch` | string | Country name or region in Dutch| Any |
| `Country` | string | Country name or region in English | Any |

<b> Final tables: </b>
Fact immigration table
| col | expected format | description | accepted range of values | source |
| --- | --- | --- | --- | --- |
| `ID` | string | Identification number | any | immigration json-file |
| `Gender` | string | Gender| male or female | join between immigration data and metaGender |
| `Age` | string | Age of the person on 31st December | 0- 105 in years | join between immigration data and metaAge |
| `MaritalStatus` | string | Marital status  | unmarried, married, remarried or divorced | join between immigration data and metaMarital |
| `Country` | string | Country of Birth of the immigrant | any | join between immigration data , MetaCountry and Landcountry |
| `Period` | integer | Year in which the immigration took place | any, current input data from 2018- 2020  | join between immigration data and metaPeriod |
| `ImmigrationVolume` | integer | Number of people immigrated | > 0 | immigration json-file |

Dimension immigration table
| col | expected format | description | accepted range of values |
| --- | --- | --- | --- |
| `Country` | string | Country | any |
| `Overall rank` | integer | Overall rank for that year in the World Happiness Report| > 0 |
| `Happiness_Score` | float | Happiness Score | 0- 10 |
| `GDP_Score` | float | Contribution of GDP per capita to the Happiness Score  | 0- 10 |
| `Health_Score` | float | Contribution of Healthy Life Expectancy to the Happiness Score | 0- 10 |
| `Freedom_Score` | float | Contribution of the Freedom of Life Choices to the Happiness Score | 0- 10  |
| `Generosity_Score` | float | Contribution of Generosity to the Happiness Score | 0- 10 |
| `Corruption_Score` | float | Contribution of Perception of Corruption to the Happiness Score | 0- 10 |
| `Year` | integer | Year of the Happiness Report | > 0 |

## Files in this repository:
- In the folder "WorldHappinessReport": csv-files with the World Happiness Report data per year
- In the folder "input_other": csv-file with the immigration meta data, csv-file with the translation information for country from Dutch to English
- etl.py: file with the ETL pipeline
- config.cfg: file in which to enter your AWS access key ID and secret access key
- Capstone_Project_YL.ipynb: Jupyter notebook that was mainly used to assess the data and to test the cleaning that was all later compiled into the etl.py. The notebook is not needed to build the data model.
- README.md: this readme file
- The immigration json-files were too large to add to this repository. They can be downloaded from the website mentioned above.
