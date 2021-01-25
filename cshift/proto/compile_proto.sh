SRC_DIR=.
DST_DIR=../core
protoc -I=$SRC_DIR --python_out=$DST_DIR $SRC_DIR/cshift.proto
