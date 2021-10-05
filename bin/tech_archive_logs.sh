#!/bin/bash

#Путь к логам
LOG_DIR=/data/logs

#Получение папки, необходимой для архивирования в текущем месяце
MONTH_DIR=$(date +%Y-%m -d '2 months ago')

zip -r -9 $LOG_DIR/$MONTH_DIR/$MONTH_DIR.zip $LOG_DIR/$MONTH_DIR/*

zip_rc=$?

#Удаление всех исходных папок директории
if [[ -d $LOG_DIR/$MONTH_DIR/ && $zip_rc -eq 0 ]] ; then
    cd $LOG_DIR/$MONTH_DIR
    find . -maxdepth 1 -type d -exec rm -r {} \;
fi
