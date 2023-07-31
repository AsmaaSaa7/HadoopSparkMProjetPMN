#!/bin/bash

input_dir="C:\Users\asmas\IdeaProjects\HadoopSparkMProjetPMN\data"

if [ -z "$(ls -A $input_dir)" ]; then
    echo "Le dépôt input est vide."
else
    echo "Le dépôt input n'est pas vide."
fi
