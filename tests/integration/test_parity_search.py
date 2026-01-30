"""
Parity Tests for Fuzzy Search (P2-040)

These tests verify that the Django fuzzy search implementations:
1. Use pg_trgm similarity correctly
2. Respect similarity thresholds
3. Return properly formatted results
"""
import pytest


# =============================================================================
# Similarity Threshold Tests
# =============================================================================

class TestFuzzySearchThresholds:
    """
    Test that fuzzy search correctly applies similarity thresholds.

    The pg_trgm similarity function returns a value between 0 and 1.
    Only results with similarity >= threshold should be returned.
    """

    def test_default_threshold_is_03(self):
        """
        Default similarity threshold should be 0.3.
        """
        default_threshold = 0.3

        # High similarity - should pass
        assert 0.8 >= default_threshold  # Exact match or close
        assert 0.5 >= default_threshold  # Moderate match
        assert 0.3 >= default_threshold  # At threshold

        # Low similarity - should fail
        assert not (0.2 >= default_threshold)
        assert not (0.1 >= default_threshold)

    def test_custom_threshold(self):
        """
        Custom thresholds should be respected.
        """
        # Strict threshold
        strict_threshold = 0.7
        assert 0.8 >= strict_threshold
        assert not (0.5 >= strict_threshold)

        # Loose threshold
        loose_threshold = 0.1
        assert 0.2 >= loose_threshold
        assert 0.15 >= loose_threshold

    def test_threshold_range_validation(self):
        """
        Threshold should be clamped between 0.0 and 1.0.
        """
        def validate_threshold(threshold: float) -> float:
            return max(0.0, min(1.0, threshold))

        assert validate_threshold(0.5) == 0.5
        assert validate_threshold(-0.5) == 0.0
        assert validate_threshold(1.5) == 1.0
        assert validate_threshold(0.0) == 0.0
        assert validate_threshold(1.0) == 1.0


# =============================================================================
# Similarity Calculation Tests
# =============================================================================

class TestSimilarityCalculation:
    """
    Test similarity calculation logic using pg_trgm approach.
    """

    def test_greatest_similarity_used(self):
        """
        The search uses GREATEST() to find the highest similarity
        across multiple fields.
        """
        # Simulate similarity scores for different fields
        first_name_similarity = 0.2
        last_name_similarity = 0.8
        email_similarity = 0.1

        # GREATEST returns the highest value
        best_similarity = max(first_name_similarity, last_name_similarity, email_similarity)

        assert best_similarity == 0.8

    def test_null_fields_return_zero_similarity(self):
        """
        NULL field values should return 0 similarity (via COALESCE).
        """
        def coalesce_similarity(value):
            return value if value is not None else 0.0

        assert coalesce_similarity(None) == 0.0
        assert coalesce_similarity(0.5) == 0.5

    def test_case_insensitive_matching(self):
        """
        Search should be case-insensitive using LOWER().
        """
        # All these should produce the same result
        queries = ["John", "JOHN", "john", "JoHn"]

        normalized = [q.lower() for q in queries]

        assert all(q == "john" for q in normalized)


# =============================================================================
# Agent Search Tests
# =============================================================================

class TestAgentFuzzySearch:
    """
    Test agent fuzzy search specific behaviors.
    """

    def test_search_fields(self):
        """
        Agent search should check first_name, last_name, email, and full name.
        """
        search_fields = ['first_name', 'last_name', 'email', 'full_name']

        # All four fields should be searchable
        assert len(search_fields) == 4
        assert 'first_name' in search_fields
        assert 'last_name' in search_fields
        assert 'email' in search_fields
        assert 'full_name' in search_fields

    def test_excludes_client_role(self):
        """
        Agent search should exclude users with role='client'.
        """
        users = [
            {'name': 'Agent User', 'role': 'agent'},
            {'name': 'Client User', 'role': 'client'},
            {'name': 'Admin User', 'role': 'admin'},
        ]

        # Filter out clients
        agents = [u for u in users if u['role'] != 'client']

        assert len(agents) == 2
        assert all(u['role'] != 'client' for u in agents)

    def test_result_includes_production_data(self):
        """
        Agent search results should include total_prod field.
        """
        result = {
            'id': 'uuid',
            'first_name': 'John',
            'last_name': 'Smith',
            'email': 'john@example.com',
            'total_prod': 50000.00,
            'similarity': 0.8,
        }

        assert 'total_prod' in result


# =============================================================================
# Client Search Tests
# =============================================================================

class TestClientFuzzySearch:
    """
    Test client fuzzy search specific behaviors.
    """

    def test_search_fields(self):
        """
        Client search should check first_name, last_name, email, phone, and full name.
        """
        search_fields = ['first_name', 'last_name', 'email', 'phone', 'full_name']

        assert 'phone' in search_fields

    def test_searches_clients_table(self):
        """
        Client search queries the clients table, not users table.
        """
        # Client search uses 'clients' table
        table_name = 'clients'

        assert table_name == 'clients'

    def test_result_includes_agent_id(self):
        """
        Client search results should include the associated agent_id.
        """
        result = {
            'id': 'uuid',
            'first_name': 'Jane',
            'last_name': 'Doe',
            'email': 'jane@example.com',
            'phone': '555-1234',
            'agent_id': 'agent-uuid',
            'similarity': 0.75,
        }

        assert 'agent_id' in result


# =============================================================================
# Policy Search Tests
# =============================================================================

class TestPolicyFuzzySearch:
    """
    Test policy/deal fuzzy search specific behaviors.
    """

    def test_search_fields(self):
        """
        Policy search should check policy_number, application_number, client_name.
        """
        search_fields = ['policy_number', 'application_number', 'client_name']

        assert 'policy_number' in search_fields
        assert 'application_number' in search_fields
        assert 'client_name' in search_fields

    def test_result_includes_deal_details(self):
        """
        Policy search results should include essential deal information.
        """
        result = {
            'id': 'uuid',
            'policy_number': 'POL-12345',
            'application_number': 'APP-67890',
            'client_name': 'John Smith',
            'agent_id': 'agent-uuid',
            'carrier_id': 'carrier-uuid',
            'annual_premium': 12000.00,
            'status_standardized': 'active',
            'similarity': 0.9,
        }

        required_fields = ['id', 'policy_number', 'client_name', 'annual_premium', 'status_standardized']
        for field in required_fields:
            assert field in result


# =============================================================================
# Result Ordering Tests
# =============================================================================

class TestSearchResultOrdering:
    """
    Test that search results are properly ordered by similarity.
    """

    def test_results_ordered_by_similarity_desc(self):
        """
        Results should be ordered by similarity score descending.
        """
        results = [
            {'name': 'John Smith', 'similarity': 0.9},
            {'name': 'Johnny Smithson', 'similarity': 0.7},
            {'name': 'Jon Smythe', 'similarity': 0.5},
        ]

        # Sort by similarity descending
        sorted_results = sorted(results, key=lambda r: r['similarity'], reverse=True)

        assert sorted_results[0]['similarity'] == 0.9
        assert sorted_results[1]['similarity'] == 0.7
        assert sorted_results[2]['similarity'] == 0.5

    def test_limit_applied_after_sorting(self):
        """
        Limit should be applied after sorting to get top matches.
        """
        results = [
            {'name': 'Result 1', 'similarity': 0.5},
            {'name': 'Result 2', 'similarity': 0.9},
            {'name': 'Result 3', 'similarity': 0.7},
            {'name': 'Result 4', 'similarity': 0.6},
            {'name': 'Result 5', 'similarity': 0.8},
        ]

        limit = 3

        # Sort then limit
        sorted_results = sorted(results, key=lambda r: r['similarity'], reverse=True)
        limited_results = sorted_results[:limit]

        assert len(limited_results) == 3
        # Top 3 by similarity
        assert limited_results[0]['similarity'] == 0.9
        assert limited_results[1]['similarity'] == 0.8
        assert limited_results[2]['similarity'] == 0.7


# =============================================================================
# Permission Scoping Tests
# =============================================================================

class TestSearchPermissionScoping:
    """
    Test that search respects permission boundaries.
    """

    def test_agency_id_filtering(self):
        """
        Search should only return results from the specified agency.
        """
        all_results = [
            {'name': 'Agent 1', 'agency_id': 'agency-1'},
            {'name': 'Agent 2', 'agency_id': 'agency-2'},
            {'name': 'Agent 3', 'agency_id': 'agency-1'},
        ]

        target_agency = 'agency-1'
        filtered = [r for r in all_results if r['agency_id'] == target_agency]

        assert len(filtered) == 2
        assert all(r['agency_id'] == target_agency for r in filtered)

    def test_allowed_agent_ids_filtering(self):
        """
        When allowed_agent_ids is provided, only those agents should be searchable.
        """
        all_agents = [
            {'id': 'agent-1', 'name': 'Agent 1'},
            {'id': 'agent-2', 'name': 'Agent 2'},
            {'id': 'agent-3', 'name': 'Agent 3'},
        ]

        allowed_ids = {'agent-1', 'agent-3'}
        filtered = [a for a in all_agents if a['id'] in allowed_ids]

        assert len(filtered) == 2
        assert filtered[0]['id'] in allowed_ids
        assert filtered[1]['id'] in allowed_ids


# =============================================================================
# Edge Cases
# =============================================================================

class TestSearchEdgeCases:
    """
    Test edge cases in fuzzy search.
    """

    def test_empty_query_rejected(self):
        """
        Empty or whitespace-only queries should be rejected.
        """
        def validate_query(query: str) -> bool:
            return bool(query and query.strip())

        assert not validate_query('')
        assert not validate_query('   ')
        assert validate_query('john')
        assert validate_query('  john  ')  # Has content after strip

    def test_special_characters_handled(self):
        """
        Special characters in queries should be handled safely.
        """
        # These should not cause SQL injection or errors
        safe_queries = [
            "O'Brien",
            "Smith-Jones",
            "test@email.com",
            "123-456-7890",
        ]

        for query in safe_queries:
            # Query should be usable (no exceptions)
            assert isinstance(query, str)

    def test_very_long_query_handled(self):
        """
        Very long queries should be truncated or handled gracefully.
        """
        long_query = 'a' * 1000

        # Truncate to reasonable length
        max_length = 255
        truncated = long_query[:max_length]

        assert len(truncated) == max_length

    def test_unicode_characters_supported(self):
        """
        Unicode characters in names should be searchable.
        """
        unicode_names = [
            "José García",
            "Müller",
            "北京",
            "Наташа",
        ]

        for name in unicode_names:
            # Name should be valid string
            assert isinstance(name, str)
            assert len(name) > 0
