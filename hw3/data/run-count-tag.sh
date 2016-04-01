#!/bin/sh

INPUT_DIR=/opt/tmp/hw3/tag/
OUTPUT_DIR=/tmp/output/tags
hadoop jar target/hw3-1.0.jar com.epam.bdc.tag.CountTagApp -files ./target/user.tags.txt -libjars ./target/lib/UserAgentUtils-1.19.jar $INPUT_DIR $OUTPUT_DIR