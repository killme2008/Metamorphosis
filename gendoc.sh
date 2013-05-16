#!/bin/bash
mvn site:site
mvn javadoc:javadoc -Daggregate=true -Dencoding=GBK -Ddocencoding=GBK -Dcharset=GBK