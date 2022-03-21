set dotenv-load := true

java:
    cd rewriter && mvn package
    cp -f rewriter/target/federated-rewriter-1.0-SNAPSHOT-jar-with-dependencies.jar connector-x/connectorx/federated-rewriter.jar

rust:
    cd connector-x/connectorx && cargo run --features src_postgres --features dst_arrow --example jvm_test
