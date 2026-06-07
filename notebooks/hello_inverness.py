import marimo

__generated_with = "0.20.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    mo.md("# Inverness\n\nLocal-first baseball analytics platform.")
    return (mo,)


@app.cell
def _(mo):
    import duckdb

    result = duckdb.sql("SELECT 'Inverness is alive' AS status, current_timestamp AS ts")
    mo.ui.table(result.fetchdf())
    return (duckdb,)


if __name__ == "__main__":
    app.run()
