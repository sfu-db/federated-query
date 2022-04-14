set dotenv-load := true

build:
    cd rewriter && mvn package -Dmaven.test.skip=true

rust file="test.sql":
    cp -f rewriter/target/federated-rewriter-1.0-SNAPSHOT-jar-with-dependencies.jar connector-x/connectorx/federated-rewriter.jar
    cd connector-x/connectorx && cargo run --features src_postgres --features src_mysql --features dst_arrow --features federation --example federated_test "../../fed-queries/{{file}}"

rust-test:
    cd connector-x/connectorx && cargo run --features src_postgres --features src_mysql --features dst_arrow --features federation --example test

cat-single qid:
    cat fed-queries/q{{qid}}.sql | sed 's/db1.//g;s/db2.//g'

cat qid:
    cat fed-queries/q{{qid}}.sql

vim qid:
    vim fed-queries/q{{qid}}.sql
