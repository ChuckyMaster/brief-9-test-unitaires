from pyspark.sql import SparkSession

# Créer une session Spark
spark = SparkSession.builder.appName("PandasToPySpark").getOrCreate()

# Données
data = [
    (1, 34, 'Cardiology'),
    (2, 45, 'Neurology'),
    (3, 50, 'Orthopedics'),
    (4, 20, 'Cardiology'),
    (5, 15, 'Neurology')
]

# Colonnes
columns = ['patient_id', 'age', 'department']

# Création du DataFrame PySpark
df_spark = spark.createDataFrame(data, schema=columns)

# Filtrer les patients âgés de plus de 30 ans
filtered_df_spark = df_spark.filter(df_spark['age'] > 30)

# Afficher le DataFrame filtré
filtered_df_spark.show()

# Si tu veux sauvegarder le résultat, tu peux ajouter une ligne comme ceci :
# filtered_df_spark.write.csv("/chemin/vers/output.csv", header=True)
