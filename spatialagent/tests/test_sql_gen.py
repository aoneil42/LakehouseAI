import pytest

from spatial_agent.planner.sql_gen import extract_sql, validate_sql, ExtractionError, ValidationError


class TestExtractSQL:
    def test_fenced_sql(self):
        response = "Here's the query:\n```sql\nSELECT * FROM buildings;\n```\nDone."
        assert extract_sql(response) == "SELECT * FROM buildings;"

    def test_unfenced_response(self):
        response = "Here's the query:\nSELECT b.* FROM lakehouse.default.buildings b"
        result = extract_sql(response)
        assert result.startswith("SELECT")
        assert "buildings" in result

    def test_mixed_explanation(self):
        response = (
            "I'll generate a query to find buildings.\n\n"
            "SELECT b.id, b.name, b.geom\n"
            "FROM lakehouse.default.buildings b\n"
            "WHERE b.height > 10"
        )
        result = extract_sql(response)
        assert "SELECT" in result
        assert "buildings" in result

    def test_no_sql_raises(self):
        response = "I don't understand the question. Can you clarify?"
        with pytest.raises(ExtractionError):
            extract_sql(response)

    def test_with_cte(self):
        response = "```sql\nWITH tall AS (SELECT * FROM buildings WHERE height > 50)\nSELECT * FROM tall;\n```"
        result = extract_sql(response)
        assert result.startswith("WITH")


class TestValidateSQL:
    def test_valid_select(self):
        validate_sql("SELECT * FROM buildings")  # should not raise

    def test_valid_with_select(self):
        validate_sql("WITH t AS (SELECT 1) SELECT * FROM t")

    def test_rejects_insert(self):
        with pytest.raises(ValidationError):
            validate_sql("INSERT INTO buildings VALUES (1, 'test')")

    def test_rejects_drop(self):
        with pytest.raises(ValidationError):
            validate_sql("DROP TABLE buildings")

    def test_rejects_delete(self):
        with pytest.raises(ValidationError):
            validate_sql("DELETE FROM buildings WHERE id = 1")

    def test_unmatched_parens(self):
        with pytest.raises(ValidationError, match="parenthesis"):
            validate_sql("SELECT * FROM buildings WHERE (id > 1")

    def test_extra_close_paren(self):
        with pytest.raises(ValidationError, match="parenthesis"):
            validate_sql("SELECT * FROM buildings WHERE id > 1)")

    def test_rejects_non_select(self):
        with pytest.raises(ValidationError):
            validate_sql("UPDATE buildings SET name = 'x'")
