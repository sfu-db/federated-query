set dotenv-load := true

java sql="${TEST_SQL}":
    cd rewriter && mvn package && java -jar target/federated-rewriter-1.0-SNAPSHOT-jar-with-dependencies.jar "{{sql}}"

build:
    cd rewriter && mvn package

rust file="test.sql":
    cp -f rewriter/target/federated-rewriter-1.0-SNAPSHOT-jar-with-dependencies.jar connector-x/connectorx/federated-rewriter.jar
    cd connector-x/connectorx && cargo run --features src_postgres --features dst_arrow --example jvm_test "../../queries/{{file}}"
