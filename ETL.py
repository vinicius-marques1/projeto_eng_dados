from pyspark.sql.session import SparkSession # Importação das Bibliotecas
import pyspark.sql.functions as F

from pyspark.sql.types import (BooleanType, IntegerType, StringType, StructType,
TimestampType, StructField, ArrayType, FloatType, DoubleType, DataType, MapType, DateType)

from pyspark.sql.functions import explode

# Inicio da Sessão Spark
spark = SparkSession.builder \
    .appName('projetofinal') \
    .config('spark.master', 'local') \
    .config('spark.executor.memory', '1gb') \
    .config('spark.shuffle.sql.partitions', 1) \
    .getOrCreate()

# Extração do Arquivo json

# Schema Estruturado com os Tipos de Dados
schemaj1 = StructType([StructField('continent', StringType()),
StructField('location', StringType()),
StructField('data', ArrayType(elementType= StringType())),
StructField('population', StringType()),
StructField('median_age', DoubleType()),
StructField('aged_65_older', DoubleType()),
StructField('aged_70_older', DoubleType()),
StructField('gdp_per_capita', DoubleType()),
StructField('life_expectancy', DoubleType()),
StructField('human_development_index', DoubleType()),
StructField('population_density', DoubleType()),
StructField('cardiovasc_death_rate', DoubleType()),
StructField('diabetes_prevalence', DoubleType()),
StructField('handwashing_facilities', StringType()),
StructField('hospital_beds_per_thousand', StringType()),
StructField('_id', StringType()),
]
)


# Leitura do Arquivo json. Criação do primeiro Dataframe
df = spark.read.schema(schemaj1).option('multiline','True').json("C:\scripts\covid.json")


# Seleção das colunas e função Explode para aranjar um array (lista) em linhas
df2 = df.select(df.continent, df.location, explode(df.data), df.population, df.median_age, df.aged_65_older, df.aged_70_older, 
df.gdp_per_capita, df.life_expectancy, df.human_development_index)


# Segundo Schema para definir o tipo dos dados relacionados a cada dado interno da coluna Data.
schemaj2 = StructType(
    [
        StructField('date', DateType(), True),
        StructField('total_cases', IntegerType(), True),
        StructField('new_cases', IntegerType(), True),
        StructField('total_deaths', IntegerType(), True),
        StructField('new_deaths', IntegerType(), True),
        StructField('stringency_index', DoubleType(), True),
        StructField('people_vaccinated', IntegerType(), True),
        StructField('people_fully_vaccinated', IntegerType(), True)
    ]
)

# Função para expandir dados que estão em formato json em varias colunas. 
df3 = df2.withColumn("col", F.from_json("col", schemaj2))\
    .select(F.col('continent'), F.col('location'), F.col('col.*'), F.col('population'), F.col('median_age'), F.col('aged_65_older'), 
    F.col('aged_70_older'), F.col('gdp_per_capita'), F.col('life_expectancy'), F.col('human_development_index'))

# Filtro retornando somente dados de 2021.
df3 = df3.filter( df3.date > '2021-01-01')

df3.show(30)
df3.printSchema()



# Extração do arquivo CSV

schemaCsv = StructType([StructField("Country Code", StringType()),
                   StructField("Country Name", StringType()),
                   StructField("Dt Extraction", StringType()),
                   StructField("If Closed due to COVID19 When", StringType()),
                   StructField("Income Level", StringType()),
                   StructField("Region Name", StringType()),
                   StructField("School Status", StringType()),
                   StructField("Year Pre", IntegerType()),
                   StructField("Year Prm", IntegerType()),
                   StructField("Year Sec", IntegerType()),
                   StructField("Year Ter", IntegerType()),
                   StructField("Latitude", DoubleType()),
                   StructField("Longitude", DoubleType()),
                   StructField("Enrollment", StringType()),
                   StructField("Se Pre Enrl", StringType()),
                   StructField("Se Prm Enrl", StringType()),
                   StructField("Se Sec Enrl", StringType()),
                   StructField("Se Ter Enrl", StringType()),
                  ]) 

path = "C:/scripts/education_COVID19.csv"

df_education = spark.read.format("csv")\
                  .schema(schemaCsv)\
                  .load(path, header="True")



df_education = df_education.drop('Longitude','Latitude','Country Code', 'Region Name', 
'Year Pre','Year Prm','Year Sec', 'Year Ter', 'Latitude',' Longitude', 'Se Pre Enrl',
'Se Prm Enrl','Se Sec Enrl', 'Se Ter Enrl', 'Enrollment', 'Dt Extraction')


df_education = df_education.withColumnRenamed('Country Name', 'location')


df_education.show(30)
df_education.printSchema()

dados_covid = df3.join(df_education, ['location'], how='left')
dados_covid.show(30)


# LOAD
# dados_covid.write.format('json').save('meuDF1.json')
# print("JSON gerado com sucesso!")

