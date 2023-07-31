# HadoopSparkMProjetPMN

# HadoopSparkProjetPMN

Description du projet :
Ce projet contient un ensemble de scripts et de programmes Spark pour effectuer des analyses de données à l'aide de Hadoop et Spark. Les scripts bash sont utilisés pour préparer l'environnement de travail, copier les données, lancer les applications Spark et nettoyer les ressources temporaires.

## Étapes pour lancer le projet

### Étape 1 : Git

1. projet sur GitHub avec le nom HadoopSparkProjetPMN et ajouter mon  nom comme description.

### Étape 3 : Scripts Bash

Créer les scripts bash suivants :

- `verify_input.sh` : un script pour vérifier si le dossier input est vide ou non.
- `copie_data_input.sh` : un script pour copier les fichiers de données dans le dossier input.
- `copie_data_tmp.sh` : un script pour créer un dossier data_tmp et y copier les fichiers présents dans input.
- `delete_tmp.sh` : un script pour supprimer le dossier data_tmp et son contenu.
- `delete_content_input.sh` : un script pour supprimer le contenu du dossier input.
- `run_spark.sh` : un script pour lancer l'application Spark.

### Étape 4 : Développement

- Avant de commencer les requêtes, inspectez bien les données dans le dossier ressources.
- Assurez-vous de créer le dossier input dans HDFS.
- Pour chaque requête, créez une fonction qui effectue les opérations et sauvegardez les résultats en format CSV et/ou Parquet.

### Étape 5 : Script Final

Dans un script bash,  les actions sont :

1. Vérification du dossier input.
2. Copie des données dans le dossier input.
3. Copie des données vers le dossier temporaire data_tmp.
4. Lancement de l'application Spark.
5. Suppression du dossier temporaire data_tmp.
6. Suppression du contenu du dossier input.






