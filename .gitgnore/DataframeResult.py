from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, to_date, lit, when
from pyspark.sql import functions as f
'''Create  the session'''
spark = SparkSession.builder.appName("Dataframe Reporting").getOrCreate()
datasets_path = 'C/Users/asmas/IdeaProjects/HadoopSparkMProjetPMN/data"'

df_full = spark.read.csv(datasets_path + 'output_csv_full.csv', header=True, inferSchema=True)
df_country = spark.read.csv(datasets_path + 'country_classification.csv', header=True, inferSchema=True)


# Question 1: Convertir le format de la date '202206' vers '01/06/2022'
def question_1(df):
    rdf = df.withColumn("date", to_date(concat_ws("/", lit(
        "01"), col("time_ref").substr(5, 2), col(
        "time_ref").substr(1, 4)), "d/M/yyyy"))

    return rdf
# Question 2: Extraire l'année
def question_2(df):
    rdf = df.withColumn("year", f.year(df["date"]))
    return rdf

# Question 3: Ajouter le nom du pays
def question_3(df):
    rdf = df.join(df_country, on="country_code", how="left")
    return rdf

# Question 4: Ajouter une colonne is_goods (1 si Goods, 0 sinon)
def question_4(df):
    rdf = df.withColumn("details_good", when(col("product_type") == "Goods", 1).otherwise(0))

    return rdf


# Question 5: Ajouter une colonne is_services (1 si Services, 0 sinon)
def question_5(df):
    rdf = df.withColumn("details_service", when(col("product_type") == "Services", 1).otherwise(0))
    return rdf

from pyspark.sql.functions import sum as _sum
# Question 6 : Classer les pays Exporteurs par Services et Goods
def question_6(df):
    df_exporters = df.filter(col("account") == 'Exports')

    grouped_df = df_exporters.groupby('country_label') \
        .agg(_sum('details_good').alias('total_goods'),
             _sum('details_service').alias('total_service'))
    sorted_df = grouped_df.orderBy(col("total_goods").desc(), col("total_service").desc())
    return sorted_df
# Question 7 : Classer les pays Importeurs par Services et Goods
def question_7(df):
    df_exporters = df.filter(col("account") == 'Imports')

    grouped_df = df_exporters.groupby('country_label') \
        .agg(_sum('details_good').alias('total_goods'),
             _sum('details_service').alias('total_service'))
    sorted_df = grouped_df.orderBy(col("total_goods").desc(), col("total_service").desc())
    return sorted_df
# Question 8 : regroupement par good
def question_8(df):
    #df_exporters = df.filter(col("account") == 'Imports')

    grouped_df = df.groupby('country_label') \
        .agg(_sum('details_good').alias('total_goods'))
    sorted_df = grouped_df.orderBy(col("total_goods").desc())
    return sorted_df


# Question 9 : regroupement des pays  par service
def question_9(df):
    #df_exporters = df.filter(col("account") == 'Imports')

    grouped_df = df.groupby('country_label') \
        .agg(_sum('details_service').alias('total_service'))
    sorted_df = grouped_df.orderBy(col("total_service").desc())
    return sorted_df
# Question 10 : Classer les pays Exporteurs par Services et Goods
def question_10(df):
    df_exporters = df.filter(df["country_code"] == "FR" )
    df_exporters = df_exporters.filter(df_exporters["account"] == "Exports").select("details_service","product_type")
    return df_exporters

r1 = question_1(df_full)
#r1.show()


r2 = question_2(r1)
#r2.show()
r3 = question_3(r2)
#r3.show()

r4 = question_4(r3)
#r4.show()

r5 = question_5(r4)
r5.show()
r6 = question_6(r5)
#r6.show() # change it to an export
r7 = question_7(r5)
#r7.show() # change it to an Import
r8 = question_8(r5)
r8.show()
r9 = question_9(r5)
r9.show()
r10 = question_10(r5)
r10.show()
# La liste des services exportés de la France.
def services_exportes_france(df):
    df_export_france_services = df.filter((col("account") == "Exports") & (col("product_type") == "Services") & (col("country_code") == "FR"))
    return df_export_france_services

# La liste des goods importés en France.
def goods_importes_france(df):
    df_import_france_goods = df.filter((col("account") == "Imports") & (col("product_type") == "Goods") & (col("country_code") == "FR"))
    return df_import_france_goods

# Classement des services les moins demandés.
def classement_services_moins_demandes(df):
    df_services = df.filter(col("product_type") == "Services")
    grouped_df = df_services.groupby("country_label") \
        .agg(_sum("details_service").alias("total_service"))
    sorted_df = grouped_df.orderBy(col("total_service").asc())
    return sorted_df

# Classement des goods les plus demandés.
def classement_goods_plus_demandes(df):
    df_goods = df.filter(col("product_type") == "Goods")
    grouped_df = df_goods.groupby("country_label") \
        .agg(_sum("details_good").alias("total_goods"))
    sorted_df = grouped_df.orderBy(col("total_goods").desc())
    return sorted_df

# Appels des fonctions pour les questions supplémentaires
r11 = services_exportes_france(r5)
r11.show()

r12 = goods_importes_france(r5)
r12.show()

r13 = classement_services_moins_demandes(r5)
r13.show()

r14 = classement_goods_plus_demandes(r5)
r14.show()

# Ajouter la colonne "status_import_export" : si import > export : négative, sinon positive (par pays)
def add_status_import_export_column(df):
    rdf = df.withColumn("status_import_export", when(col("details_good") > col("details_service"), "négative").otherwise("positive"))
    return rdf

# Ajouter la colonne "difference_import_export" qui va calculer les exports - imports (par pays)
def add_difference_import_export_column(df):
    rdf = df.withColumn("difference_import_export", col("details_good") - col("details_service"))
    return rdf

# Ajouter la colonne "Somme_good" qui va calculer la somme des goods par pays
def add_somme_good_column(df):
    rdf = df.groupby("country_label").agg(_sum("details_good").alias("Somme_good"))
    return rdf

# Ajouter la colonne "somme_service" qui va calculer la somme des services par pays
def add_somme_service_column(df):
    rdf = df.groupby("country_label").agg(_sum("details_service").alias("somme_service"))
    return rdf

# Ajouter la colonne "pourcentages_good" qui va calculer le pourcentage de la colonne "details_good" par rapport à tous les goods d'un seul pays (regroupement par import et export)
def add_pourcentages_good_column(df):
    total_goods_df = df.groupby("country_label").agg(_sum("details_good").alias("total_goods"))
    rdf = df.join(total_goods_df, on="country_label", how="left") \
        .withColumn("pourcentages_good", col("details_good") / col("total_goods") * 100)
    return rdf

# Ajouter la colonne "pourcentages_service" qui va calculer le pourcentage de la colonne "details_service" par rapport à tous les services d'un seul pays (regroupement par import et export)
def add_pourcentages_service_column(df):
    total_service_df = df.groupby("country_label").agg(_sum("details_service").alias("total_service"))
    rdf = df.join(total_service_df, on="country_label", how="left") \
        .withColumn("pourcentages_service", col("details_service") / col("total_service") * 100)
    return rdf

# Regrouper les goods selon leur type (Code HS2)
def regroupement_par_good(df):
    rdf = df.groupby("HS2").agg(_sum("details_good").alias("total_goods"))
    return rdf

# Classement des pays exportateurs de pétrole
def classement_pays_exportateurs_petrole(df):
    df_export_petrolier = df.filter((col("account") == "Exports") & (col("product_type") == "Goods") & (col("HS2") == "27"))
    grouped_df = df_export_petrolier.groupby("country_label") \
        .agg(_sum("details_good").alias("total_goods"))
    sorted_df = grouped_df.orderBy(col("total_goods").desc())
    return sorted_df

# Classement des pays importateurs de viandes
def classement_pays_importateurs_viandes(df):
    df_import_viandes = df.filter((col("account") == "Imports") & (col("product_type") == "Goods") & (col("HS2") == "02"))
    grouped_df = df_import_viandes.groupby("country_label") \
        .agg(_sum("details_good").alias("total_goods"))
    sorted_df = grouped_df.orderBy(col("total_goods").desc())
    return sorted_df

# Classement des pays qui ont le plus de demandes sur les services informatiques (computer services, computer software et other computer services)
def classement_pays_demandes_services_informatiques(df):
    df_services_informatiques = df.filter((col("account") == "Exports") & (col("product_type") == "Services") & (col("HS2").isin(["85", "84", "99"])))
    grouped_df = df_services_informatiques.groupby("country_label") \
        .agg(_sum("details_service").alias("total_service"))
    sorted_df = grouped_df.orderBy(col("total_service").desc())
    return sorted_df

# (Bonus) Ajouter une colonne "description" : qui prend comme valeur : "le pays XXXX fait un [IMPORT ou EXPORT] sur [goods ou Services]"
def add_description_column(df):
    rdf = df.withColumn("description", concat_ws(" ", lit("le pays"), col("country_label"), lit("fait un"), when(col("account") == "Exports", "EXPORT").otherwise("IMPORT"), lit("sur"), when(col("product_type") == "Goods", "goods").otherwise("Services")))
    return rdf

# Appels des fonctions
r15 = add_status_import_export_column(r5)
r15.show()

r16 = add_difference_import_export_column(r5)
r16.show()

r17 = add_somme_good_column(r5)
r17.show()

r18 = add_somme_service_column(r5)
r18.show()

r19 = add_pourcentages_good_column(r5)
r19.show()

r20 = add_pourcentages_service_column(r5)
r20.show()

r21 = regroupement_par_good(r5)
r21.show()

r22 = classement_pays_exportateurs_petrole(r5)
r22.show()

r23 = classement_pays_importateurs_viandes(r5)
r23.show()

r24 = classement_pays_demandes_services_informatiques(r5)
r24.show()

r25 = add_description_column(r5)
r25.show()
