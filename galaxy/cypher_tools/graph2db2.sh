cp $HOME/galaxy/galaxy-tools/batch-import/batch.properties ./
java -server -jar $HOME/galaxy/galaxy-tools/batch-import/target/batch-import-jar-with-dependencies.jar ./db $1 $2 2>&1
$HOME/neo4j/neo4j-community-1.9.4/bin/neo4j stop
rm -rf $HOME/neo4j/neo4j-community-1.9.4/data/graph.db
mv ./db $HOME/neo4j/neo4j-community-1.9.4/data/graph.db
$HOME/neo4j/neo4j-community-1.9.4/bin/neo4j start
