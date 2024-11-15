from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql.functions import udf, col
from datetime import datetime, timedelta

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("CreateTimeTable").getOrCreate()

# Define a função para gerar os registros de tempo
def generate_time_records():
    records = []
    for i in range(86400):  # 86400 segundos em um dia
        time_delta = timedelta(seconds=i)
        total_seconds = time_delta.total_seconds()
        hours = time_delta.seconds // 3600
        minutes = (time_delta.seconds // 60) % 60
        seconds = time_delta.seconds % 60
        time_str = (datetime.min + time_delta).time().strftime("%H:%M:%S")
        period = "Madrugada" if hours < 6 else "Manhã" if hours < 12 else "Tarde" if hours < 18 else "Noite"
        post_meridiem = "AM" if hours < 12 else "PM"
        records.append((i, total_seconds / 3600, time_str, time_str[:5] + ":00", time_str[:3] + "00:00", hours, minutes, seconds, period, post_meridiem))
    return records

# Gera os registros
time_records = generate_time_records()

# Definir o esquema da tabela
schema = StructType([
    StructField("Indice", IntegerType(), False),
    StructField("HorarioDecimal", DoubleType(), False),
    StructField("Horario", StringType(), False),
    StructField("InicioHora", StringType(), False),
    StructField("InicioMinuto", StringType(), False),
    StructField("Hora", IntegerType(), False),
    StructField("Minuto", IntegerType(), False),
    StructField("Segundo", IntegerType(), False),
    StructField("Periodo", StringType(), False),
    StructField("PostMeridiem", StringType(), False)
])

# Cria o DataFrame com o esquema definido e os registros gerados
df = spark.createDataFrame(time_records, schema)

# Define os turnos de trabalho
turnos = [
    ("Turno 1", datetime.strptime("05:00:00", "%H:%M:%S").time(), datetime.strptime("13:29:59", "%H:%M:%S").time()),
    ("Turno 2", datetime.strptime("13:30:00", "%H:%M:%S").time(), datetime.strptime("21:59:59", "%H:%M:%S").time()),
    ("Turno 3", datetime.strptime("22:00:00", "%H:%M:%S").time(), datetime.strptime("04:59:59", "%H:%M:%S").time())
]

# Define a função para determinar o turno
def get_turno(horario):
    horario = datetime.strptime(horario, "%H:%M:%S").time()
    for turno, inicio, fim in turnos:
        if inicio <= fim:
            if inicio <= horario <= fim:
                return turno
        else:  # Caso especial onde o horário final é menor que o inicial
            if horario >= inicio or horario <= fim:
                return turno
    return None

# Define a função UDF para determinar o turno
get_turno_udf = udf(get_turno, StringType())

# Adiciona a coluna "Turno"
df = df.withColumn("Turno", get_turno_udf(col("Horario")))

# Salva o DataFrame como uma tabela no Lakehouse
df.write.format("delta").mode("overwrite").save("Tables/dim_hora")

# Mostra a tabela carregada
df.show()

# Finalizar a sessão Spark
spark.stop()

# Fim do arquivo dim_horas.py
# Elaborado por Alison Pezzott
# Data: 15/11/2024

