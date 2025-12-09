# DuckDB Tee Extension
Tee is a [DuckDB](https://github.com/duckdb/duckdb) extension. The name refers to the Unix/Linux command of the same name, ‘tee’, which reads input and writes it to an output. The tee operator implements a table function, so it expects a table or a subquery. In standard mode, this is printed in the terminal. 

### Example:

```sql
> SELECT a FROM tee(SELECT * FROM t);

Tee Operator:
┌───────┬───────┐
│   a   │   b   │
│ int32 │ int32 │
├───────┼───────┤
│     0 │    42 │
│    27 │    84 │
└───────┴───────┘
┌───────┐
│   a   │                                                                                                                                                                                                                                                            
│ int32 │                                                                                                                                                                                                                                                            
├───────┤                                                                                                                                                                                                                                                            
│     0 │                                                                                                                                                                                                                                                            
│    27 │                                                                                                                                                                                                                                                            
└───────┘   
```
## Installation
The extension is not live yet.

## Parameters
The tee operator can be called with various named parameters using the following syntax:
``` SQL
... FROM tee((TABLE t), 
    named_parameter := ..., 
    named_parameter := ...,
    ...)
```

### The following parameters are implemented:

| Name       | Datatype | Description                                                                                                               |
|------------|----------|---------------------------------------------------------------------------------------------------------------------------|
| symbol     | String   | The output of a tee call is given the name ‘symbol’ so that it can be referenced.                                         |
| terminal   | Boolean  | The terminal flag determines whether the output should actually be printed to the console. By default, it is set to true. |
| path       | String   | The output of the tee call is written to a file in csv format on the specified path.                                      |
| table_name | String   | The tee call is written as a table in the current attachted database. The table is then named 'table_name'.      |
| pager      | Boolean  | If this flag is set, the system-specific pager is always activated for the data output by the tee call. The pager is set to false by default.    |

## Examples
### Symbol:
```sql
> SELECT a FROM tee((SELECT * FROM t), symbol = 'Called');

Tee Operator, Query: Called
    
┌───────┬───────┐
│   a   │   b   │
│ int32 │ int32 │
├───────┼───────┤
│     0 │    42 │
│    27 │    84 │
└───────┴───────┘
┌───────┐
│   a   │                                                                                                                                                                                                                                                            
│ int32 │                                                                                                                                                                                                                                                            
├───────┤                                                                                                                                                                                                                                                            
│     0 │                                                                                                                                                                                                                                                            
│    27 │                                                                                                                                                                                                                                                            
└───────┘ 
```

### Terminal:
```sql
> SELECT a FROM tee((SELECT * FROM t), terminal = false);
┌───────┐
│   a   │                                                                                                                                                                                                                                                            
│ int32 │                                                                                                                                                                                                                                                            
├───────┤                                                                                                                                                                                                                                                            
│     0 │                                                                                                                                                                                                                                                            
│    27 │                                                                                                                                                                                                                                                            
└───────┘
```

### Path:
```sql
> SELECT a FROM tee((SELECT * FROM t), path = 'out.csv');

Write to: test_dir/csv_files/out.csv

Tee Operator:
┌───────┬───────┐
│   a   │   b   │
│ int32 │ int32 │
├───────┼───────┤
│     0 │    42 │
│    27 │    84 │
└───────┴───────┘
┌───────┐
│   a   │                                                                                                                                                                                                                                                            
│ int32 │                                                                                                                                                                                                                                                            
├───────┤                                                                                                                                                                                                                                                            
│     0 │                                                                                                                                                                                                                                                            
│    27 │                                                                                                                                                                                                                                                            
└───────┘

> .shell cat test_dir/csv_files/out.csv
        
a,b
0,42
27,8
```

### table_name:
```sql
> SELECT * FROM tee((SELECT * FROM range(5)), table_name = 'huge_range', terminal = false);

Table huge_range created and added to the current attached database.

┌───────┐
│ range │                                                                                                                                                                                                                                                            
│ int64 │                                                                                                                                                                                                                                                            
├───────┤                                                                                                                                                                                                                                                            
│     0 │                                                                                                                                                                                                                                                            
│     1 │                                                                                                                                                                                                                                                            
│     2 │                                                                                                                                                                                                                                                            
│     3 │                                                                                                                                                                                                                                                            
│     4 │                                                                                                                                                                                                                                                            
└───────┘

> FROM huge_range;

┌───────┐
│ range │                                                                                                                                                                                                                                                            
│ int64 │                                                                                                                                                                                                                                                            
├───────┤                                                                                                                                                                                                                                                            
│     0 │                                                                                                                                                                                                                                                            
│     1 │                                                                                                                                                                                                                                                            
│     2 │                                                                                                                                                                                                                                                            
│     3 │                                                                                                                                                                                                                                                            
│     4 │                                                                                                                                                                                                                                                            
└───────┘ 
```



