# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(tee
        SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
        LOAD_TESTS
)

duckdb_extension_load(tpch)
duckdb_extension_statically_link(tpch)
duckdb_extension_statically_link(tee)