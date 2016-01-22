CLASS_NAME="TopCarrierPerRoute"

export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
hadoop com.sun.tools.javac.Main ${CLASS_NAME}.java -d build 
jar -cvf ${CLASS_NAME}.jar -C build/ ./
