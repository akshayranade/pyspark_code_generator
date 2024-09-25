source ../conf/env.conf

SOURCE_FILE=$1
PIPELINE_FILE=$2

SOURCE_CONTENT=$(cat "$SOURCE_FILE")
PIPELINE_CONTENT=$(cat "$PIPELINE_FILE")

cd $PROJECT_HOME

spark-submit \
--deploy-mode $DEPLOY_MODE \
--files $SOURCE_FILE,$PIPELINE_FILE,$HIVE_CONF \
--jars "${PROJECT_HOME}/jars/*.jar" \
--conf "spark.sql.catalogImplementation=hive" \
--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
--conf "spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener" \
--conf "spark.openlineage.url=http://localhost:5000/api/v1/namespaces/spark_integration/" \
--conf "spark.openlineage.transport.type=http" \
--conf "spark.openlineage.transport.url=http://localhost:5000/api/v1/lineage" \
--conf "spark.openlineage.namespace=ofti_demo_namespace" \
--py-files $APPLICATION_PACKAGE \
bin/wrapper.py "$SOURCE_CONTENT" "$PIPELINE_CONTENT"