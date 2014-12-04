export CLASSPATH=./bin:./libs/chronicle-3.2.5.jar:./libs/kilim.jar:./libs/affinity-2.1.2.jar:./libs/annotations-9.0.jar:./libs/asm-all-4.1.jar:./libs/compiler-2.2.0.jar:./libs/jna-4.1.0.jar:./libs/jna-platform-4.1.0.jar:./libs/lang-6.4.10.jar:./libs/slf4j-api-1.7.6.jar:$CLASSPATH 

rm -rf ./bin

mkdir ./bin

cp config.properties ./bin

javac -Xlint:unchecked -g -d ./bin `find . -name *.java`


java -ea kilim.tools.Weaver -d ./bin -x "ExInvalid | test" ./bin

