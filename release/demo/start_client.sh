set -e

export ONEDB_ROOT=$PWD/..
java -Dlog4j.configuration=file:"../config/log4j.properties" -jar ../bin/onedb_user_client.jar -c ../config/client_model.json