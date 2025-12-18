#!/bin/bash

hbase shell <<EOF2
create 'crypto_index_aggregates',
  {NAME => 'ohlc', VERSIONS => 1},
  {NAME => 'indicators', VERSIONS => 1},
  {NAME => 'meta', VERSIONS => 1}
exit
EOF2
