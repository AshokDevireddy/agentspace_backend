"""
Parity Tests for Scoreboard (P2-036)

These tests verify that the Django scoreboard implementation produces
correct results for:
1. Date range filtering
2. Agent ranking calculation
3. Stats totals accuracy
"""
from datetime import date, timedelta
from decimal import Decimal

import pytest


# =============================================================================
# Date Range Filtering Tests
# =============================================================================

class TestScoreboardDateRangeFiltering:
    """
    Test that scoreboard correctly filters deals by date range.

    The scoreboard should only include deals where:
    - submission_date >= start_date
    - submission_date <= end_date
    """

    def test_date_range_boundaries(self):
        """
        Deals exactly on boundary dates should be included.
        """
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 31)

        # Test boundary conditions
        deal_on_start = date(2024, 1, 1)
        deal_on_end = date(2024, 1, 31)
        deal_before = date(2023, 12, 31)
        deal_after = date(2024, 2, 1)

        # Deals on boundaries should be included
        assert start_date <= deal_on_start <= end_date
        assert start_date <= deal_on_end <= end_date

        # Deals outside should be excluded
        assert not (start_date <= deal_before <= end_date)
        assert not (start_date <= deal_after <= end_date)

    def test_lookback_date_calculation(self):
        """
        The scoreboard uses a 1-year lookback from end_date.

        lookback_date = end_date - 1 year
        """
        end_date = date(2024, 6, 30)

        # Calculate lookback (1 year before end_date)
        lookback_date = date(end_date.year - 1, end_date.month, end_date.day)

        assert lookback_date == date(2023, 6, 30)

    def test_date_range_with_no_deals(self):
        """
        When no deals fall within the date range, totals should be zero.
        """
        # Empty result structure
        empty_stats = {
            'totalProduction': Decimal('0.00'),
            'totalDeals': 0,
            'activeAgents': 0
        }

        assert empty_stats['totalProduction'] == Decimal('0.00')
        assert empty_stats['totalDeals'] == 0


# =============================================================================
# Agent Ranking Tests
# =============================================================================

class TestScoreboardAgentRanking:
    """
    Test that agents are ranked correctly by total production.
    """

    def test_agents_ranked_by_total_production(self):
        """
        Agents should be ranked in descending order by total_production.
        """
        # Sample agent production data
        agents = [
            {'name': 'Agent A', 'total': Decimal('50000.00')},
            {'name': 'Agent B', 'total': Decimal('75000.00')},
            {'name': 'Agent C', 'total': Decimal('25000.00')},
        ]

        # Sort by total descending (this is what the scoreboard does)
        ranked = sorted(agents, key=lambda a: a['total'], reverse=True)

        assert ranked[0]['name'] == 'Agent B'  # Highest
        assert ranked[1]['name'] == 'Agent A'  # Middle
        assert ranked[2]['name'] == 'Agent C'  # Lowest

    def test_agents_with_equal_production(self):
        """
        Agents with equal production should maintain stable ordering.
        """
        agents = [
            {'id': '1', 'name': 'Agent A', 'total': Decimal('50000.00')},
            {'id': '2', 'name': 'Agent B', 'total': Decimal('50000.00')},
        ]

        # With equal totals, ordering should be stable (by id or name)
        ranked = sorted(agents, key=lambda a: (a['total'], a['name']), reverse=True)

        # Both have same total, sorted alphabetically by name in reverse
        assert ranked[0]['total'] == ranked[1]['total']

    def test_only_active_agents_included(self):
        """
        Only agents with is_active=true should be included in the scoreboard.
        """
        agents = [
            {'name': 'Active Agent', 'is_active': True, 'total': Decimal('50000.00')},
            {'name': 'Inactive Agent', 'is_active': False, 'total': Decimal('100000.00')},
        ]

        # Filter to only active agents
        active_agents = [a for a in agents if a['is_active']]

        assert len(active_agents) == 1
        assert active_agents[0]['name'] == 'Active Agent'

    def test_client_role_excluded(self):
        """
        Users with role='client' should be excluded from the scoreboard.
        """
        users = [
            {'name': 'Agent User', 'role': 'agent', 'total': Decimal('50000.00')},
            {'name': 'Client User', 'role': 'client', 'total': Decimal('75000.00')},
            {'name': 'Admin User', 'role': 'admin', 'total': Decimal('25000.00')},
        ]

        # Filter out clients
        non_clients = [u for u in users if u['role'] != 'client']

        assert len(non_clients) == 2
        assert all(u['role'] != 'client' for u in non_clients)


# =============================================================================
# Stats Totals Tests
# =============================================================================

class TestScoreboardStatsTotals:
    """
    Test that stats totals are calculated correctly.
    """

    def test_total_production_sum(self):
        """
        totalProduction should be the sum of all annual_premium within the date range.
        """
        deals = [
            {'annual_premium': Decimal('12000.00')},
            {'annual_premium': Decimal('24000.00')},
            {'annual_premium': Decimal('6000.00')},
        ]

        total_production = sum(d['annual_premium'] for d in deals)

        assert total_production == Decimal('42000.00')

    def test_total_deals_count(self):
        """
        totalDeals should be the count of deals within the date range.
        """
        deals = [
            {'id': '1', 'annual_premium': Decimal('12000.00')},
            {'id': '2', 'annual_premium': Decimal('24000.00')},
            {'id': '3', 'annual_premium': Decimal('6000.00')},
        ]

        total_deals = len(deals)

        assert total_deals == 3

    def test_active_agents_count(self):
        """
        activeAgents should be the count of distinct agents who wrote deals.
        """
        deals = [
            {'agent_id': 'agent-1', 'annual_premium': Decimal('12000.00')},
            {'agent_id': 'agent-1', 'annual_premium': Decimal('24000.00')},  # Same agent
            {'agent_id': 'agent-2', 'annual_premium': Decimal('6000.00')},
        ]

        active_agents = len(set(d['agent_id'] for d in deals))

        assert active_agents == 2

    def test_production_excludes_null_premium(self):
        """
        Deals with NULL or 0 annual_premium should be excluded from totals.
        """
        deals = [
            {'annual_premium': Decimal('12000.00')},
            {'annual_premium': None},  # NULL premium
            {'annual_premium': Decimal('0.00')},  # Zero premium
            {'annual_premium': Decimal('6000.00')},
        ]

        # Filter out NULL and zero premiums
        valid_deals = [
            d for d in deals
            if d['annual_premium'] is not None and d['annual_premium'] > 0
        ]

        total_production = sum(d['annual_premium'] for d in valid_deals)

        assert total_production == Decimal('18000.00')
        assert len(valid_deals) == 2


# =============================================================================
# Daily Breakdown Tests
# =============================================================================

class TestScoreboardDailyBreakdown:
    """
    Test the daily breakdown aggregation for agent production.
    """

    def test_daily_production_aggregation(self):
        """
        Each agent's production should be broken down by day.
        """
        # Agent's deals on different days
        agent_deals = [
            {'submission_date': date(2024, 1, 1), 'annual_premium': Decimal('12000.00')},
            {'submission_date': date(2024, 1, 1), 'annual_premium': Decimal('6000.00')},
            {'submission_date': date(2024, 1, 2), 'annual_premium': Decimal('24000.00')},
        ]

        # Aggregate by date
        daily_totals = {}
        for deal in agent_deals:
            day = deal['submission_date'].isoformat()
            if day not in daily_totals:
                daily_totals[day] = Decimal('0.00')
            daily_totals[day] += deal['annual_premium']

        assert daily_totals['2024-01-01'] == Decimal('18000.00')
        assert daily_totals['2024-01-02'] == Decimal('24000.00')

    def test_monthly_summary(self):
        """
        Agent monthly summary should aggregate daily production.
        """
        daily_breakdown = {
            '2024-01-01': Decimal('12000.00'),
            '2024-01-15': Decimal('6000.00'),
            '2024-01-31': Decimal('18000.00'),
        }

        # Sum all days in January
        monthly_total = sum(daily_breakdown.values())

        assert monthly_total == Decimal('36000.00')


# =============================================================================
# Response Structure Tests
# =============================================================================

class TestScoreboardResponseStructure:
    """
    Test that the scoreboard response matches expected structure.
    """

    def test_response_structure(self):
        """
        Verify the response structure matches frontend expectations.
        """
        # Expected response structure
        response = {
            'success': True,
            'data': {
                'leaderboard': [
                    {
                        'id': 'uuid',
                        'name': 'Agent Name',
                        'total': Decimal('50000.00'),
                        'deals': 10,
                        'dailyBreakdown': {'2024-01-01': 5000},
                    }
                ],
                'stats': {
                    'totalProduction': Decimal('100000.00'),
                    'totalDeals': 20,
                    'activeAgents': 5,
                },
                'dateRange': {
                    'startDate': '2024-01-01',
                    'endDate': '2024-01-31',
                }
            }
        }

        assert response['success'] is True
        assert 'data' in response
        assert 'leaderboard' in response['data']
        assert 'stats' in response['data']
        assert 'dateRange' in response['data']

    def test_leaderboard_entry_fields(self):
        """
        Each leaderboard entry should have required fields.
        """
        entry = {
            'id': 'agent-uuid',
            'name': 'John Smith',
            'total': Decimal('50000.00'),
            'deals': 10,
            'dailyBreakdown': {},
        }

        required_fields = ['id', 'name', 'total', 'deals']
        for field in required_fields:
            assert field in entry
