set dotenv-load := true

build:
    cd rewriter && mvn package -Dmaven.test.skip=true

rust file="test.sql":
    cp -f rewriter/target/federated-rewriter-1.0-SNAPSHOT-jar-with-dependencies.jar connector-x/connectorx/federated-rewriter.jar
    cd connector-x/connectorx && cargo run --features src_postgres --features src_mysql --features dst_arrow --features federation --example federated_test "../../test-queries/{{file}}"

rust-test:
    cd connector-x/connectorx && cargo run --features src_postgres --features src_mysql --features dst_arrow --features federation --example test

@cat1 qid:
    cat test-queries/q{{qid}}.sql | sed 's/db1.//g;s/db2.//g'

@cat qid:
    cat test-queries/q{{qid}}.sql

vim qid:
    vim test-queries/q{{qid}}.sql

process-queries path:
    cd {{path}} && for ((i=5;i<=22;i++)); do mv q${i}.sql tmp.sql && cat tmp.sql | sed '/^--/d;/^\s*$/d;/^limit/d' | tr -d ';' > q${i}.sql && rm tmp.sql; done

gen-bench-queries inpath outpath:
    cat {{inpath}}/q2.sql | sed 's/partsupp/db1.partsupp/g;s/\tpart/\tdb1.part/g;s/supplier/db1.supplier/g;s/\tnation/\tdb2.nation/g;s/\tregion/\tdb2.region/g' > {{outpath}}/q2.sql
    cat {{inpath}}/q3.sql | sed 's/lineitem/db1.lineitem/g;s/orders/db1.orders/g;s/customer/db2.customer/g' > {{outpath}}/q3.sql
    cat {{inpath}}/q4.sql | sed 's/lineitem/db1.lineitem/g;s/orders/db2.orders/g' > {{outpath}}/q4.sql
    cat {{inpath}}/q5.sql | sed 's/lineitem/db1.lineitem/g;s/orders/db1.orders/g;s/customer/db1.customer/g;s/supplier/db2.supplier/g;s/\tnation/\tdb2.nation/g;s/\tregion/\tdb2.region/g' > {{outpath}}/q5.sql
    cat {{inpath}}/q7.sql | sed 's/lineitem/db1.lineitem/g;s/orders/db1.orders/g;s/supplier/db1.supplier/g;s/customer/db2.customer/g;s/\tnation/\tdb2.nation/g' > {{outpath}}/q7.sql
    cat {{inpath}}/q8.sql | sed 's/\tpart/\tdb1.part/g;s/lineitem/db1.lineitem/g;s/orders/db1.orders/g;s/supplier/db1.supplier/g;s/customer/db2.customer/g;s/\tnation/\tdb2.nation/g;s/\tregion/\tdb2.region/g' > {{outpath}}/q8.sql
    cat {{inpath}}/q9.sql | sed 's/\tpart/\tdb1.part/g;s/lineitem/db1.lineitem/g;s/orders/db2.orders/g;s/supplier/db1.supplier/g;s/\t\tnation/\t\tdb2.nation/g' > {{outpath}}/q9.sql
    cat {{inpath}}/q10.sql | sed 's/lineitem/db1.lineitem/g;s/orders/db1.orders/g;s/customer/db2.customer/g;s/\tnation/\tdb2.nation/g' > {{outpath}}/q10.sql
    cat {{inpath}}/q11.sql | sed 's/value/myvalue/g;s/partsupp/db1.partsupp/g;s/supplier/db1.supplier/g;s/\tnation/\tdb2.nation/g' > {{outpath}}/q11.sql
    cat {{inpath}}/q12.sql | sed 's/lineitem/db1.lineitem/g;s/orders/db2.orders/g' > {{outpath}}/q12.sql
    cat {{inpath}}/q13.sql | sed 's/customer/db2.customer/g;s/ orders/ db1.orders/g' > {{outpath}}/q13.sql
    cat {{inpath}}/q14.sql | sed 's/lineitem/db1.lineitem/g;s/\tpart/\tdb2.part/g' > {{outpath}}/q14.sql
    cat {{inpath}}/q17.sql | sed 's/lineitem/db1.lineitem/g;s/\tpart/\tdb2.part/g' > {{outpath}}/q17.sql
    cat {{inpath}}/q18.sql | sed 's/lineitem/db1.lineitem/g;s/orders/db1.orders/g;s/customer/db2.customer/g' > {{outpath}}/q18.sql
    cat {{inpath}}/q19.sql | sed 's/lineitem/db1.lineitem/g;s/\tpart/\tdb2.part/g' > {{outpath}}/q19.sql
    cat {{inpath}}/q20.sql | sed 's/partsupp/db1.partsupp/g;s/\tpart/\tdb1.part/g;s/supplier/db1.supplier/g;s/lineitem/db1.lineitem/g;s/\tnation/\tdb2.nation/g' > {{outpath}}/q20.sql
    cat {{inpath}}/q22.sql | sed 's/customer/db2.customer/g;s/\torders/\tdb1.orders/g' > {{outpath}}/q22.sql
