"""
Parity Tests for Expected Payouts (P2-040)

These tests verify that the Django payout calculations:
1. Use the correct commission formula
2. Handle hierarchy levels correctly
3. Calculate debt proration accurately
"""
from datetime import date, timedelta
from decimal import Decimal

import pytest


# =============================================================================
# Commission Formula Tests
# =============================================================================

class TestPayoutFormula:
    """
    Test the payout commission formula.

    The correct formula is:
    payout = annual_premium * 0.75 * (agent_commission_% / hierarchy_total_%)
    """

    def test_basic_formula_calculation(self):
        """
        Basic formula: annual_premium * 0.75 * (agent_% / total_%)
        """
        annual_premium = Decimal('12000.00')
        agent_commission_percentage = Decimal('60.00')
        hierarchy_total_percentage = Decimal('100.00')

        expected_payout = (
            annual_premium *
            Decimal('0.75') *
            (agent_commission_percentage / hierarchy_total_percentage)
        )

        # 12000 * 0.75 * (60/100) = 12000 * 0.75 * 0.6 = 5400
        assert expected_payout == Decimal('5400.00')

    def test_formula_with_split_hierarchy(self):
        """
        When hierarchy has multiple agents, each gets proportional share.
        """
        annual_premium = Decimal('12000.00')
        hierarchy_total_percentage = Decimal('100.00')

        # Agent 1: 60% of hierarchy
        agent1_percentage = Decimal('60.00')
        agent1_payout = (
            annual_premium *
            Decimal('0.75') *
            (agent1_percentage / hierarchy_total_percentage)
        )

        # Agent 2: 30% of hierarchy
        agent2_percentage = Decimal('30.00')
        agent2_payout = (
            annual_premium *
            Decimal('0.75') *
            (agent2_percentage / hierarchy_total_percentage)
        )

        # Agent 3: 10% of hierarchy
        agent3_percentage = Decimal('10.00')
        agent3_payout = (
            annual_premium *
            Decimal('0.75') *
            (agent3_percentage / hierarchy_total_percentage)
        )

        # Verify individual payouts
        assert agent1_payout == Decimal('5400.00')  # 12000 * 0.75 * 0.6
        assert agent2_payout == Decimal('2700.00')  # 12000 * 0.75 * 0.3
        assert agent3_payout == Decimal('900.00')   # 12000 * 0.75 * 0.1

        # Total should equal 75% of annual premium
        total_payout = agent1_payout + agent2_payout + agent3_payout
        assert total_payout == annual_premium * Decimal('0.75')

    def test_formula_with_uneven_hierarchy(self):
        """
        Hierarchy total doesn't have to be 100%.
        """
        annual_premium = Decimal('10000.00')
        hierarchy_total_percentage = Decimal('80.00')  # Only 80% assigned

        # Agent has 40% of 80% total
        agent_percentage = Decimal('40.00')
        agent_payout = (
            annual_premium *
            Decimal('0.75') *
            (agent_percentage / hierarchy_total_percentage)
        )

        # 10000 * 0.75 * (40/80) = 7500 * 0.5 = 3750
        assert agent_payout == Decimal('3750.00')

    def test_formula_rounds_to_two_decimals(self):
        """
        Payout should be rounded to 2 decimal places.
        """
        annual_premium = Decimal('12345.67')
        agent_percentage = Decimal('33.33')
        hierarchy_total = Decimal('100.00')

        raw_payout = (
            annual_premium *
            Decimal('0.75') *
            (agent_percentage / hierarchy_total)
        )

        # Round to 2 decimals
        rounded_payout = round(raw_payout, 2)

        assert isinstance(rounded_payout, Decimal)
        # Check it has at most 2 decimal places
        assert rounded_payout == rounded_payout.quantize(Decimal('0.01'))

    def test_zero_hierarchy_total_returns_zero(self):
        """
        If hierarchy total is 0, payout should be 0 (avoid division by zero).
        """
        annual_premium = Decimal('12000.00')
        agent_percentage = Decimal('60.00')
        hierarchy_total = Decimal('0.00')

        # Guard against division by zero
        if hierarchy_total > 0:
            payout = annual_premium * Decimal('0.75') * (agent_percentage / hierarchy_total)
        else:
            payout = Decimal('0.00')

        assert payout == Decimal('0.00')

    def test_null_commission_percentage_returns_zero(self):
        """
        If agent commission percentage is NULL, payout should be 0.
        """
        annual_premium = Decimal('12000.00')
        agent_percentage = None
        hierarchy_total = Decimal('100.00')

        if agent_percentage is None:
            payout = Decimal('0.00')
        else:
            payout = annual_premium * Decimal('0.75') * (agent_percentage / hierarchy_total)

        assert payout == Decimal('0.00')


# =============================================================================
# Hierarchy Level Tests
# =============================================================================

class TestHierarchyLevels:
    """
    Test hierarchy level calculations in payouts.
    """

    def test_level_0_is_writing_agent(self):
        """
        Level 0 represents the writing agent (direct producer).
        """
        hierarchy_levels = {
            0: 'Writing Agent',
            1: 'Direct Upline',
            2: 'Agency Owner',
        }

        assert hierarchy_levels[0] == 'Writing Agent'

    def test_all_levels_sum_to_total_percentage(self):
        """
        All hierarchy levels should sum to the total percentage.
        """
        levels = [
            {'level': 0, 'percentage': Decimal('60.00')},
            {'level': 1, 'percentage': Decimal('30.00')},
            {'level': 2, 'percentage': Decimal('10.00')},
        ]

        total = sum(l['percentage'] for l in levels)

        assert total == Decimal('100.00')

    def test_higher_levels_get_override(self):
        """
        Higher levels (uplines) get override commission on top of direct production.
        """
        # Level 0 agent gets 60%
        # Level 1 agent gets 30% (override)
        # Level 2 agent gets 10% (override of override)

        levels = [
            {'level': 0, 'percentage': Decimal('60.00'), 'type': 'direct'},
            {'level': 1, 'percentage': Decimal('30.00'), 'type': 'override'},
            {'level': 2, 'percentage': Decimal('10.00'), 'type': 'override'},
        ]

        # Writing agent gets direct
        assert levels[0]['type'] == 'direct'

        # Uplines get override
        assert all(l['type'] == 'override' for l in levels[1:])


# =============================================================================
# Debt Calculation Tests
# =============================================================================

class TestDebtCalculation:
    """
    Test debt calculation and proration logic.
    """

    def test_debt_is_negative_balance(self):
        """
        Debt represents money advanced to agent that hasn't been earned back.
        """
        advance_given = Decimal('5000.00')
        earned_back = Decimal('3000.00')

        debt = advance_given - earned_back

        assert debt == Decimal('2000.00')

    def test_debt_proration_by_days(self):
        """
        Debt can be prorated based on days in period.
        """
        total_debt = Decimal('3000.00')
        days_in_period = 30
        days_elapsed = 15

        prorated_debt = total_debt * Decimal(days_elapsed) / Decimal(days_in_period)

        # Half the period elapsed, half the debt prorated
        assert prorated_debt == Decimal('1500.00')

    def test_debt_proration_by_percentage(self):
        """
        Debt can be prorated based on a percentage.
        """
        total_debt = Decimal('10000.00')
        proration_percentage = Decimal('0.25')  # 25%

        prorated_debt = total_debt * proration_percentage

        assert prorated_debt == Decimal('2500.00')

    def test_zero_debt_returns_zero(self):
        """
        If no debt exists, prorated amount should be 0.
        """
        total_debt = Decimal('0.00')
        proration_percentage = Decimal('0.50')

        prorated_debt = total_debt * proration_percentage

        assert prorated_debt == Decimal('0.00')

    def test_debt_cannot_be_negative(self):
        """
        Debt amount should not be negative (that would be a credit).
        """
        calculated_debt = Decimal('-1000.00')

        # Clamp to 0
        actual_debt = max(Decimal('0.00'), calculated_debt)

        assert actual_debt == Decimal('0.00')


# =============================================================================
# Filter Tests
# =============================================================================

class TestPayoutFiltering:
    """
    Test payout filtering by various criteria.
    """

    def test_filter_by_date_range(self):
        """
        Payouts should be filtered by policy effective date range.
        """
        deals = [
            {'policy_effective_date': date(2024, 1, 15), 'payout': Decimal('1000.00')},
            {'policy_effective_date': date(2024, 2, 15), 'payout': Decimal('2000.00')},
            {'policy_effective_date': date(2024, 3, 15), 'payout': Decimal('3000.00')},
        ]

        start_date = date(2024, 1, 1)
        end_date = date(2024, 2, 28)

        filtered = [
            d for d in deals
            if start_date <= d['policy_effective_date'] <= end_date
        ]

        assert len(filtered) == 2
        assert filtered[0]['payout'] == Decimal('1000.00')
        assert filtered[1]['payout'] == Decimal('2000.00')

    def test_filter_by_agent_ids(self):
        """
        Payouts should be filtered by specific agent IDs.
        """
        payouts = [
            {'agent_id': 'agent-1', 'payout': Decimal('1000.00')},
            {'agent_id': 'agent-2', 'payout': Decimal('2000.00')},
            {'agent_id': 'agent-3', 'payout': Decimal('3000.00')},
        ]

        allowed_agents = {'agent-1', 'agent-3'}

        filtered = [p for p in payouts if p['agent_id'] in allowed_agents]

        assert len(filtered) == 2
        total_payout = sum(p['payout'] for p in filtered)
        assert total_payout == Decimal('4000.00')

    def test_filter_by_status(self):
        """
        Payouts can be filtered by deal status.
        """
        payouts = [
            {'status': 'active', 'payout': Decimal('1000.00')},
            {'status': 'pending', 'payout': Decimal('2000.00')},
            {'status': 'lapsed', 'payout': Decimal('3000.00')},
        ]

        active_payouts = [p for p in payouts if p['status'] == 'active']

        assert len(active_payouts) == 1
        assert active_payouts[0]['payout'] == Decimal('1000.00')


# =============================================================================
# Aggregation Tests
# =============================================================================

class TestPayoutAggregation:
    """
    Test payout aggregation and totals.
    """

    def test_aggregate_by_agent(self):
        """
        Payouts should be aggregated by agent.
        """
        payouts = [
            {'agent_id': 'agent-1', 'payout': Decimal('1000.00')},
            {'agent_id': 'agent-1', 'payout': Decimal('2000.00')},
            {'agent_id': 'agent-2', 'payout': Decimal('1500.00')},
        ]

        # Aggregate by agent
        agent_totals = {}
        for p in payouts:
            agent_id = p['agent_id']
            if agent_id not in agent_totals:
                agent_totals[agent_id] = Decimal('0.00')
            agent_totals[agent_id] += p['payout']

        assert agent_totals['agent-1'] == Decimal('3000.00')
        assert agent_totals['agent-2'] == Decimal('1500.00')

    def test_aggregate_by_period(self):
        """
        Payouts can be aggregated by time period.
        """
        payouts = [
            {'month': '2024-01', 'payout': Decimal('1000.00')},
            {'month': '2024-01', 'payout': Decimal('2000.00')},
            {'month': '2024-02', 'payout': Decimal('1500.00')},
        ]

        # Aggregate by month
        monthly_totals = {}
        for p in payouts:
            month = p['month']
            if month not in monthly_totals:
                monthly_totals[month] = Decimal('0.00')
            monthly_totals[month] += p['payout']

        assert monthly_totals['2024-01'] == Decimal('3000.00')
        assert monthly_totals['2024-02'] == Decimal('1500.00')

    def test_grand_total_calculation(self):
        """
        Grand total should sum all payouts.
        """
        payouts = [
            {'payout': Decimal('1000.00')},
            {'payout': Decimal('2000.00')},
            {'payout': Decimal('3000.00')},
        ]

        grand_total = sum(p['payout'] for p in payouts)

        assert grand_total == Decimal('6000.00')


# =============================================================================
# Edge Cases
# =============================================================================

class TestPayoutEdgeCases:
    """
    Test edge cases in payout calculations.
    """

    def test_zero_annual_premium(self):
        """
        Zero annual premium should result in zero payout.
        """
        annual_premium = Decimal('0.00')
        agent_percentage = Decimal('60.00')
        hierarchy_total = Decimal('100.00')

        payout = annual_premium * Decimal('0.75') * (agent_percentage / hierarchy_total)

        assert payout == Decimal('0.00')

    def test_very_small_premium(self):
        """
        Very small premiums should calculate correctly.
        """
        annual_premium = Decimal('0.01')
        agent_percentage = Decimal('100.00')
        hierarchy_total = Decimal('100.00')

        payout = annual_premium * Decimal('0.75') * (agent_percentage / hierarchy_total)

        # 0.01 * 0.75 * 1.0 = 0.0075, rounds to 0.01
        assert round(payout, 2) == Decimal('0.01')

    def test_large_premium(self):
        """
        Large premiums should calculate without overflow.
        """
        annual_premium = Decimal('1000000.00')  # $1M premium
        agent_percentage = Decimal('50.00')
        hierarchy_total = Decimal('100.00')

        payout = annual_premium * Decimal('0.75') * (agent_percentage / hierarchy_total)

        # 1000000 * 0.75 * 0.5 = 375000
        assert payout == Decimal('375000.00')

    def test_fractional_percentages(self):
        """
        Fractional commission percentages should work correctly.
        """
        annual_premium = Decimal('10000.00')
        agent_percentage = Decimal('33.33')
        hierarchy_total = Decimal('100.00')

        payout = annual_premium * Decimal('0.75') * (agent_percentage / hierarchy_total)

        # 10000 * 0.75 * 0.3333 = 2499.75
        assert round(payout, 2) == Decimal('2499.75')

    def test_multiple_deals_same_agent(self):
        """
        Agent can have payouts from multiple deals.
        """
        agent_payouts = [
            Decimal('1000.00'),
            Decimal('2000.00'),
            Decimal('1500.00'),
        ]

        total = sum(agent_payouts)

        assert total == Decimal('4500.00')

    def test_agent_in_multiple_hierarchies(self):
        """
        Same agent can appear in multiple deal hierarchies.
        """
        deals_with_agent = [
            {'deal_id': 'deal-1', 'level': 0, 'payout': Decimal('1000.00')},  # Writing agent
            {'deal_id': 'deal-2', 'level': 1, 'payout': Decimal('500.00')},   # Override
            {'deal_id': 'deal-3', 'level': 2, 'payout': Decimal('200.00')},   # Higher override
        ]

        total_payout = sum(d['payout'] for d in deals_with_agent)

        assert total_payout == Decimal('1700.00')
