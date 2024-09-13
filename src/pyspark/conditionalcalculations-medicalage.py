from pyspark.sql import SparkSession
from pyspark.sql.functions import when

# Créer une session Spark
spark = SparkSession.builder.appName("PandasToPySpark").getOrCreate()

# Données
data = [
    (1, 34, 'Cardiology'),
    (2, 70, 'Neurology'),
    (3, 50, 'Orthopedics'),
    (4, 20, 'Cardiology'),
    (5, 15, 'Neurology')
]

# Colonnes
columns = ['patient_id', 'age', 'department']

# Création du DataFrame PySpark
df_spark = spark.createDataFrame(data, schema=columns)

# Ajout d'une colonne conditionnelle (catégorie d'âge) avec la fonction when
df_spark = df_spark.withColumn(
    'age_category',
    when(df_spark['age'] > 60, 'senior')
    .when(df_spark['age'] > 18, 'adult')
    .otherwise('minor')
)

# Afficher le DataFrame final
df_spark.show()

# Si tu veux sauvegarder le résultat, tu peux ajouter une ligne comme ceci :
# df_spark.write.csv("/chemin/vers/output.csv", header=True)
