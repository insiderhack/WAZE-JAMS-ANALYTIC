echo ""
echo ""
echo -e "Muhammad Rizki █▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀█"
echo -e "█                     » WAZER DATAS Sqooping «                     █"
echo -e "█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄ v-0.0.5 ▄▄▄█ Muhammad Rizki"
echo ""
echo "     Sqooping IS The Another Level Store Data    "
echo ""
echo ""

pass=$(. ./credential2.sh)

##############################################################################

sqoop import -m 1 --connect jdbc:postgresql://databaseip:5432/waze_dump --username wazedump --password $pass --warehouse-dir /user/husni/muhammadrizki/record --table jams -- --schema public

##############################################################################

hive -e $1
###############################################################################

# ############# ############# #############
# ##       TIME TO RUN THE SCRIPT        ##
# ##                                     ##
# ## You shouldn't need to edit anything ##
# ## beneath this line                   ##
# ##                                     ##
# ############# ############# #############

echo ""
echo ""
echo -e "Muhammad Rizki █▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀█"
echo -e "█                     » WAZER DATAS Sqooping «                     █"
echo -e "█▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄ v-0.0.5 ▄▄▄█ Muhammad Rizki"
echo ""
echo "     Sqooping IS The Another Level Store Data    "
echo ""
