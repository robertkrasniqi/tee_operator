import pytest
import subprocess
import os
import shutil

DUCKDB = os.path.expanduser("~/tee_operator/build/debug/duckdb")

@pytest.fixture
def workdir(tmp_path):
    base_dir = tmp_path / "csv_files_testing"
    base_dir.mkdir()

    old_cwd = os.getcwd()
    os.chdir(base_dir)

    # If I want to check the actual output:
    # print(f"\n Directory is: {base_dir} \n")

    yield base_dir

    # Delete the generated files
    os.chdir(old_cwd)
    shutil.rmtree(base_dir, ignore_errors=True)


def test_basic_query(workdir):
    sql = """
          SELECT * FROM tee((SELECT 42 AS 'a'), path = 'out.csv');
          """

    result = subprocess.run(
        [DUCKDB, "-c", sql],
        text=True,
        capture_output=True,
        check=True
    )

    print("STDOUT:", result.stdout)
    print("STDERR: \n", result.stderr)
    print(result.returncode)

    assert result.returncode == 0

    output_file = workdir / "out.csv"
    assert output_file.exists()

    lines = output_file.read_text().splitlines()
    assert len(lines) == 2

    # Check header
    assert lines[0] == "a"

    # Check second line
    first_row = lines[1].split(",")
    assert first_row[0] == "42"

def test_large_query(workdir):
    row_count = 4097

    sql = f"""
    SELECT * FROM tee((
          SELECT
          a,
          10 AS b,
          a % 9 AS c,
          FLOOR(a / 5) :: int AS d
    FROM range({row_count}) AS _(a)), path = 'out.csv');
    """

    result = subprocess.run(
        [DUCKDB, "-c", sql],
        text=True,
        capture_output=True,
        check=True
    )

    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)
    print(result.returncode)

    assert result.returncode == 0

    output_file = workdir / "out.csv"
    assert output_file.exists()

    lines = output_file.read_text().splitlines()

    assert len(lines) == row_count + 1

    # Check header
    assert lines[0] == "a,b,c,d"

    # Check second line
    first_row = lines[1].split(",")
    assert first_row[0] == "0"
    assert first_row[1] == "10"
    assert first_row[2] == "0"
    assert first_row[3] == "0"

    # Check last line
    last_row = lines[-1].split(",")
    assert last_row[0] == str(row_count - 1)
    assert last_row[1] == "10"
    assert last_row[2] == str((row_count - 1) % 9)
    assert last_row[3] == str((row_count - 1) // 5)