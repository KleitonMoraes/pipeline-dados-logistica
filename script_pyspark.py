# ==========================================================
# PROJETO: Pipeline de Dados de Logística (Arquitetura Medallion)
# Ferramentas: PySpark & Spark SQL (Databricks)
# ==========================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg

# 1. Inicializando a Sessão do Spark
spark = SparkSession.builder \
    .appName("PipelineLogisticaJúnior") \
    .getOrCreate()

# 2. CAMADA BRONZE: Simulando a carga de dados brutos de entregas
# (Imagine que estes são dados extraídos do sistema operacional)
dados_brutos = [
    (1, "Curitiba", 2.5, "Atrasado"),
    (2, "São José dos Pinhais", 1.0, "No Prazo"),
    (3, "Joinville", 4.0, "Atrasado"),
    (4, "Araucária", 0.5, "No Prazo"),
    (5, "Curitiba", 3.2, "Atrasado")
]

colunas = ["id_entrega", "cidade", "tempo_entrega_horas", "status_entrega"]

df_bronze = spark.createDataFrame(dados_brutos, colunas)
print("--- DADOS NA CAMADA BRONZE (Brutos) ---")
df_bronze.show()

# 3. CAMADA SILVER: Limpeza e tratamento dos dados usando PySpark
# Tratando possíveis nulos e padronizando as colunas
df_silver = df_bronze.dropna().withColumn(
    "entrega_problema",
    when(col("status_entrega") == "Atrasado", 1).otherwise(0)
)

print("--- DADOS NA CAMADA SILVER (Limpos e Tratados) ---")
df_silver.show()

# Registrando o DataFrame como uma tabela temporária para usar SQL
df_silver.createOrReplaceTempView("tabela_silver_entregas")

# 4. CAMADA GOLD: Agregação de Negócio usando Spark SQL
# Respondendo a perguntas de negócio: Qual o tempo médio de entrega por cidade?
df_gold = spark.sql("""
    SELECT 
        cidade,
        COUNT(id_entrega) as total_entregas,
        ROUND(AVG(tempo_entrega_horas), 2) as tempo_medio_horas,
        SUM(entrega_problema) as total_atrasos
    FROM tabela_silver_entregas
    GROUP BY cidade
    ORDER BY tempo_medio_horas DESC
""")

print("--- DADOS NA CAMADA GOLD (Visão de Negócio / Indicadores) ---")
df_gold.show()
