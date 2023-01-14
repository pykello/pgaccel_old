## Requirements

* sudo apt install build-essential cmake googletest libreadline-dev libtbb-dev libpapi-dev linux-tools-generic linux-cloud-tools-generic linux-tools-command
* Install libarrow-dev && libparquet-dev: https://arrow.apache.org/install/
* enable perf counters: `sudo sh -c 'echo -1 > /proc/sys/kernel/perf_event_paranoid'`

## Loading data into clickhouse

```
CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
                             L_PARTKEY     INTEGER NOT NULL,
                             L_SUPPKEY     INTEGER NOT NULL,
                             L_LINENUMBER  INTEGER NOT NULL,
                             L_QUANTITY    DECIMAL(15,2) NOT NULL,
                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                             L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                             L_TAX         DECIMAL(15,2) NOT NULL,
                             L_RETURNFLAG  VARCHAR(1) NOT NULL,
                             L_LINESTATUS  VARCHAR(1) NOT NULL,
                             L_SHIPDATE    DATE NOT NULL,
                             L_COMMITDATE  DATE NOT NULL,
                             L_RECEIPTDATE DATE NOT NULL,
                             L_SHIPINSTRUCT VARCHAR(25) NOT NULL,
                             L_SHIPMODE     VARCHAR(10) NOT NULL,
                             L_COMMENT      VARCHAR(44) NOT NULL) engine Memory;

cat /home/hadi/disk1/data/tpch/16/parquet/lineitem.parquet | clickhouse-client --query="INSERT INTO LINEITEM FORMAT Parquet"
```

## Performance Notes

### Operator fusing
Q6 count, 1000 runs on 16gb:
* Fusing:
    * Without fusing: 7.0
    * With fusing: 7.2
* Loop unrolling: no effect
* Fused op anding:
    * mask/mask/&: 7.000
    * mask/mask_compare: 6.98

