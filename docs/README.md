# DuckDB Tee Extension
Tee is a [DuckDB](https://github.com/duckdb/duckdb) extension. The name refers to the Unix/Linux command of the same name, ‘tee’, which reads input and writes it to an output. The tee operator implements a table function, so it expects a table or a subquery. In standard mode, this is printed in the terminal.

## Example

```sql
WITH RECURSIVE fib(n, a, b) AS (
    SELECT  0 AS n,
            0 AS a,
            1 AS b
    UNION ALL
    SELECT  n + 1 AS n,
            b     AS a,
            a + b AS b
    FROM
        tee((FROM fib))
    WHERE n < 5
)
SELECT n, a FROM fib;
```

```
Tee Operator: 
┌───────┬───────┬───────┐
│   n   │   a   │   b   │
│ int32 │ int32 │ int32 │
├───────┼───────┼───────┤
│     0 │     0 │     1 │
└───────┴───────┴───────┘
Tee Operator: 
┌───────┬───────┬───────┐
│   n   │   a   │   b   │
│ int32 │ int32 │ int32 │
├───────┼───────┼───────┤
│     1 │     1 │     1 │
└───────┴───────┴───────┘
Tee Operator: 
┌───────┬───────┬───────┐
│   n   │   a   │   b   │
│ int32 │ int32 │ int32 │
├───────┼───────┼───────┤
│     2 │     1 │     2 │
└───────┴───────┴───────┘
Tee Operator: 
┌───────┬───────┬───────┐
│   n   │   a   │   b   │
│ int32 │ int32 │ int32 │
├───────┼───────┼───────┤
│     3 │     2 │     3 │
└───────┴───────┴───────┘
Tee Operator: 
┌───────┬───────┬───────┐
│   n   │   a   │   b   │
│ int32 │ int32 │ int32 │
├───────┼───────┼───────┤
│     4 │     3 │     5 │
└───────┴───────┴───────┘
Tee Operator: 
┌───────┬───────┬───────┐
│   n   │   a   │   b   │
│ int32 │ int32 │ int32 │
├───────┼───────┼───────┤
│     5 │     5 │     8 │
└───────┴───────┴───────┘

┌───────┬───────┐
│   n   │   a   │
│ int32 │ int32 │
├───────┼───────┤
│     0 │     0 │
│     1 │     1 │
│     2 │     1 │
│     3 │     2 │
│     4 │     3 │
│     5 │     5 │
└───────┴───────┘

```
## Installation
The extension is not live yet.

## Named Parameters
The tee operator can be called with various named parameters using the following syntax:
``` SQL
... FROM tee((TABLE t), 
    named_parameter := ..., 
    named_parameter := ...,
    ...)
```

| Name         | Datatype | Default | Description                                                                                                                                                                                                    |
|--------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `symbol`     | String   | –       | The output of a tee call is given the name ‘symbol’ so that it can be referenced.                                                                                                                              |
| `terminal`   | Boolean  | `true`  | The terminal flag determines whether the output should actually be printed to the console.                                                                                                                     |
| `path`       | String   | –       | The output of the tee call is written to a file in CSV format on the specified path. When used within a recursive CTE, an additional column showing the iteration step of the recursion is written to the csv. |
| `table_name` | String   | –       | The tee call is written as a table in the current attachted database. When used within a recursive CTE, an additional column showing the iteration step of the recursion is written to the table.              |
| `pager`      | Boolean  | `false` | If this flag is set, the system-specific pager is always activated for the data output by the tee call.                                                                                                        |
| `maxrows`    | Integer  | `40`    | Specifies how many rows the terminal or pager shows.                                                                                                                                                           |


## Parameters in detail
The examples below run against the TPC-H schema:

```sql
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf = 1);
```

### symbol: labelling tee calls
Because every tee call prints the same header, a query with multiple tee calls can become confusing. The 'symbol' parameter can be used to name these tee calls.

```sql
SELECT o_orderstatus, count(*) AS lines
FROM tee((SELECT o_orderkey, o_orderstatus
          FROM orders
          WHERE o_orderkey < 5), symbol = 'orders')
JOIN tee((SELECT l_orderkey
          FROM lineitem
          WHERE l_orderkey < 5), symbol = 'lineitem')
  ON o_orderkey = l_orderkey
GROUP BY o_orderstatus;
```

```
Tee Operator; Symbol: orders
┌────────────┬───────────────┐
│ o_orderkey │ o_orderstatus │
│   int64    │    varchar    │
├────────────┼───────────────┤
│          1 │ O             │
│          2 │ O             │
│          3 │ F             │
│          4 │ O             │
└────────────┴───────────────┘
Tee Operator; Symbol: lineitem
┌────────────┐
│ l_orderkey │
│   int64    │
├────────────┤
│          1 │
│          1 │
│          1 │
│          1 │
│          1 │
│          1 │
│          2 │
│          3 │
│          3 │
│          3 │
│          3 │
│          3 │
│          3 │
│          4 │
└────────────┘


┌───────────────┬───────┐
│ o_orderstatus │ lines │
│    varchar    │ int64 │
├───────────────┼───────┤
│ F             │     6 │
│ O             │     8 │
└───────────────┴───────┘
```

### terminal: disable tee rendering
With `terminal = false` the rows still pass through tee, but nothing is rendered on the terminal. This can be useful if the tee output is only needed in a CSV file or table. 

```sql
SELECT *
FROM tee((SELECT l_orderkey, l_quantity
          FROM lineitem
          WHERE l_quantity = 50),
         table_name = 'quantity_50',
         terminal = false)
LIMIT 3;
```

```
Table quantity_50 created and added to the current attached database.
┌────────────┬───────────────┐
│ l_orderkey │  l_quantity   │
│   int64    │ decimal(15,2) │
├────────────┼───────────────┤
│          5 │         50.00 │
│        131 │         50.00 │
│        199 │         50.00 │
└────────────┴───────────────┘
```

Only the query result itself is printed, still the table `quantity_50` holds all rows that passed through tee.

### path: writing the tee output to a CSV file
```sql
WITH RECURSIVE double_up(start_value, value) AS (
    SELECT *
    FROM (VALUES (1, 1), (3, 3))
    UNION ALL
    SELECT start_value,
           2 * value AS value
FROM tee((FROM double_up), path = 'double_up.csv')
WHERE value < 42
    )
SELECT start_value, value
FROM double_up
ORDER BY start_value, value;
```

<details>
<summary>Query output</summary>

```
Write to: double_up.csv
Tee Operator: 
┌─────────────┬───────┐
│ start_value │ value │
│    int32    │ int32 │
├─────────────┼───────┤
│           1 │     1 │
│           3 │     3 │
└─────────────┴───────┘
Tee Operator: 
┌─────────────┬───────┐
│ start_value │ value │
│    int32    │ int32 │
├─────────────┼───────┤
│           1 │     2 │
│           3 │     6 │
└─────────────┴───────┘
Tee Operator: 
┌─────────────┬───────┐
│ start_value │ value │
│    int32    │ int32 │
├─────────────┼───────┤
│           1 │     4 │
│           3 │    12 │
└─────────────┴───────┘
Tee Operator: 
┌─────────────┬───────┐
│ start_value │ value │
│    int32    │ int32 │
├─────────────┼───────┤
│           1 │     8 │
│           3 │    24 │
└─────────────┴───────┘
Tee Operator: 
┌─────────────┬───────┐
│ start_value │ value │
│    int32    │ int32 │
├─────────────┼───────┤
│           1 │    16 │
│           3 │    48 │
└─────────────┴───────┘
Tee Operator: 
┌─────────────┬───────┐
│ start_value │ value │
│    int32    │ int32 │
├─────────────┼───────┤
│           1 │    32 │
└─────────────┴───────┘
Tee Operator: 
┌─────────────┬───────┐
│ start_value │ value │
│    int32    │ int32 │
├─────────────┼───────┤
│           1 │    64 │
└─────────────┴───────┘
┌─────────────┬───────┐
│ start_value │ value │
│    int32    │ int32 │
├─────────────┼───────┤
│           1 │     1 │
│           1 │     2 │
│           1 │     4 │
│           1 │     8 │
│           1 │    16 │
│           1 │    32 │
│           1 │    64 │
│           3 │     3 │
│           3 │     6 │
│           3 │    12 │
│           3 │    24 │
│           3 │    48 │
└─────────────┴───────┘
  12 rows   2 columns
```

</details>

<details>
<summary>Contents of double_up.csv</summary>

```sql
FROM 'double_up.csv';
```

```
┌───────────┬─────────────┬───────┐
│ iteration │ start_value │ value │
│   int64   │    int64    │ int64 │
├───────────┼─────────────┼───────┤
│         0 │           1 │     1 │
│         0 │           3 │     3 │
│         1 │           1 │     2 │
│         1 │           3 │     6 │
│         2 │           1 │     4 │
│         2 │           3 │    12 │
│         3 │           1 │     8 │
│         3 │           3 │    24 │
│         4 │           1 │    16 │
│         4 │           3 │    48 │
│         5 │           1 │    32 │
│         6 │           1 │    64 │
└───────────┴─────────────┴───────┘
  12 rows               3 columns
```

</details>

### table_name: writing the tee output into a table

```sql
WITH RECURSIVE double_up(start_value, value) AS (
    SELECT *
    FROM (VALUES (1, 1), (3, 3))
    UNION ALL
    SELECT start_value,
           2 * value AS value
    FROM tee((FROM double_up), table_name = 'doubled')
    WHERE value < 42
)
SELECT start_value, value
FROM double_up
ORDER BY start_value, value;
```

<details>
<summary>Query output</summary>

```
Table doubled created and added to the current attached database. 
Tee Operator: 
┌─────────────┬───────┐
│ start_value │ value │
│    int32    │ int32 │
├─────────────┼───────┤
│           1 │     1 │
│           3 │     3 │
└─────────────┴───────┘
Tee Operator: 
┌─────────────┬───────┐
│ start_value │ value │
│    int32    │ int32 │
├─────────────┼───────┤
│           1 │     2 │
│           3 │     6 │
└─────────────┴───────┘
Tee Operator: 
┌─────────────┬───────┐
│ start_value │ value │
│    int32    │ int32 │
├─────────────┼───────┤
│           1 │     4 │
│           3 │    12 │
└─────────────┴───────┘
Tee Operator: 
┌─────────────┬───────┐
│ start_value │ value │
│    int32    │ int32 │
├─────────────┼───────┤
│           1 │     8 │
│           3 │    24 │
└─────────────┴───────┘
Tee Operator: 
┌─────────────┬───────┐
│ start_value │ value │
│    int32    │ int32 │
├─────────────┼───────┤
│           1 │    16 │
│           3 │    48 │
└─────────────┴───────┘
Tee Operator: 
┌─────────────┬───────┐
│ start_value │ value │
│    int32    │ int32 │
├─────────────┼───────┤
│           1 │    32 │
└─────────────┴───────┘
Tee Operator: 
┌─────────────┬───────┐
│ start_value │ value │
│    int32    │ int32 │
├─────────────┼───────┤
│           1 │    64 │
└─────────────┴───────┘
┌─────────────┬───────┐
│ start_value │ value │
│    int32    │ int32 │
├─────────────┼───────┤
│           1 │     1 │
│           1 │     2 │
│           1 │     4 │
│           1 │     8 │
│           1 │    16 │
│           1 │    32 │
│           1 │    64 │
│           3 │     3 │
│           3 │     6 │
│           3 │    12 │
│           3 │    24 │
│           3 │    48 │
└─────────────┴───────┘
  12 rows   2 columns
```

</details>

<details>
<summary>Contents of the table doubled</summary>

```sql
FROM doubled;
```

```
┌───────────┬─────────────┬───────┐
│ iteration │ start_value │ value │
│   int64   │    int32    │ int32 │
├───────────┼─────────────┼───────┤
│         0 │           1 │     1 │
│         0 │           3 │     3 │
│         1 │           1 │     2 │
│         1 │           3 │     6 │
│         2 │           1 │     4 │
│         2 │           3 │    12 │
│         3 │           1 │     8 │
│         3 │           3 │    24 │
│         4 │           1 │    16 │
│         4 │           3 │    48 │
│         5 │           1 │    32 │
│         6 │           1 │    64 │
└───────────┴─────────────┴───────┘
  12 rows               3 columns
```

</details>

### maxrows: limiting the rendered rows

```sql
SELECT * FROM tee((FROM range(4242)), maxrows = 7);
```

```
Tee Operator: 
┌───────────┐
│   range   │
│   int64   │
├───────────┤
│         0 │
│         1 │
│         2 │
│         3 │
│         · │
│         · │
│         · │
│      4239 │
│      4240 │
│      4241 │
└───────────┘
  4242 rows
  (7 shown) 
 
┌────────────┐
│   range    │
│   int64    │
├────────────┤
│          0 │
│          1 │
│          2 │
│          3 │
│          4 │
│          5 │
│          6 │
│          7 │
│          8 │
│          9 │
│         10 │
│         11 │
│         12 │
│         13 │
│         14 │
│         15 │
│         16 │
│         17 │
│         18 │
│         19 │
│          · │
│          · │
│          · │
│       4222 │
│       4223 │
│       4224 │
│       4225 │
│       4226 │
│       4227 │
│       4228 │
│       4229 │
│       4230 │
│       4231 │
│       4232 │
│       4233 │
│       4234 │
│       4235 │
│       4236 │
│       4237 │
│       4238 │
│       4239 │
│       4240 │
│       4241 │
└────────────┘
  4242 rows 
  (40 shown) 

```
`maxrows = 0` renders everything.
