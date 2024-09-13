from pyspark.sql import SparkSession
from pyspark.sql.functions import mean

# Créer une session Spark
spark = SparkSession.builder.appName("PandasToPySpark").getOrCreate()

# Données
data = [
    (1, 34, 'Cardiology'),
    (2, None, 'Neurology'),
    (3, 50, 'Orthopedics'),
    (4, None, None),
    (5, 15, 'Neurology')
]

# Colonnes
columns = ['patient_id', 'age', 'department']

# Création du DataFrame PySpark
df_spark = spark.createDataFrame(data, schema=columns)

# Calcul de la moyenne de l'âge pour remplacer les valeurs manquantes
