set dotenv-load := true

java sql="${TEST_SQL}":
    cd rewriter && mvn package && java -jar target/federated-rewriter-1.0-SNAPSHOT-jar-with-dependencies.jar "{{sql}}"

build:
    cd rewriter && mvn package -Dmaven.test.skip=true

rust-single file="test.sql":
    cp -f rewriter/target/federated-rewriter-1.0-SNAPSHOT-jar-with-dependencies.jar connector-x/connectorx/federated-rewriter.jar
    cd connector-x/connectorx && cargo run --features src_postgres --features dst_arrow --example jvm_test "../../queries/{{file}}"

rust file="test.sql":
    cp -f rewriter/target/federated-rewriter-1.0-SNAPSHOT-jar-with-dependencies.jar connector-x/connectorx/federated-rewriter.jar
    cd connector-x/connectorx && cargo run --features src_postgres --features src_mysql --features dst_arrow --features federation --example federated_test "../../fed-queries/{{file}}"

test file="test.sql":
    cp -f rewriter/target/federated-rewriter-1.0-SNAPSHOT-jar-with-dependencies.jar connector-x/connectorx/federated-rewriter.jar
    cd connector-x/connectorx && cargo run --features src_postgres --features src_mysql --features dst_arrow --features federation --example test "../../fed-queries/{{file}}"

