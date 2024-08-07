source ../conf/env.conf

cd $PROJECT_HOME

spark-submit \
--deploy-mode $DEPLOY_MODE \
--files $SOURCE_FILE,$PIPELINE_FILE \
--py-files $APPLICATION_PACKAGE \
bin/wrapper.py