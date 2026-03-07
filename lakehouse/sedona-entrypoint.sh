#!/bin/sh
# Template spark-defaults.conf with Garage credentials at runtime.
# Uses sed instead of envsubst for compatibility with minimal images.
sed \
  -e "s|\${GARAGE_KEY_ID}|${GARAGE_KEY_ID}|g" \
  -e "s|\${GARAGE_SECRET_KEY}|${GARAGE_SECRET_KEY}|g" \
  /opt/spark/conf/spark-defaults.conf.template \
  > /opt/spark/conf/spark-defaults.conf

# Execute whatever CMD/args were passed
exec "$@"
