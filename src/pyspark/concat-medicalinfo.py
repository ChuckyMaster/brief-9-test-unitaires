from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, concat_ws

# Créer une session Spark
spark = SparkSession.builder.appName("medicalinfo").getOrCreate()

# Données
data = [
    ('John Doe', 'Diabetes'),
    ('Jane Smith', 'Heart Disease'),
    ('Alice Brown', 'Hypertension')
]

# Colonnes
columns = ['patient_name', 'diagnosis']

# Création du DataFrame PySpark
df_spark = spark.createDataFrame(data, schema=columns)

# Conversion en minuscules et ajout d'un champ 'diagnosis_lower'
df_spark = df_spark.withColumn('diagnosis_lower', lower(df_spark['diagnosis']))

# Ajout de la colonne 'full_info' en concaténant 'patient_name' et 'diagnosis_lower'
df_spark = df_spark.withColumn('full_info', concat_ws(' - ', df_spark['patient_name'], df_spark['diagnosis_lower']))

# Afficher le DataFrame final
df_spark.show(truncate=False)


