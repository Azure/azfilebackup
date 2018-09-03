#!/bin/bash

rg=backuptest
vm_name=saphec7

az vm update --resource-group "${rg}" --name "${vm_name}" --set \
    tags.db_backup_interval_min=1d \
    tags.db_backup_interval_max=3d \
    tags.log_backup_interval_min=10m \
    tags.db_backup_window_1="111111 111000 000000 011111" \
    tags.db_backup_window_2="111111 111000 000000 011111" \
    tags.db_backup_window_3="111111 111000 000000 011111" \
    tags.db_backup_window_4="111111 111000 000000 011111" \
    tags.db_backup_window_5="111111 111000 000000 011111" \
    tags.db_backup_window_6="111111 111111 111111 111111" \
    tags.db_backup_window_7="111111 111111 111111 111111"
