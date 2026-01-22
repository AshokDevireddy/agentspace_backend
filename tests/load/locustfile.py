"""
Load Tests for AgentSpace Backend API (P2-041)

Run with:
    cd backend
    locust -f tests/load/locustfile.py --host=http://localhost:8000

Or headless:
    locust -f tests/load/locustfile.py --host=http://localhost:8000 \
        --headless -u 50 -r 10 -t 60s

Configuration:
    -u: Number of concurrent users
    -r: Spawn rate (users per second)
    -t: Run time (e.g., 60s, 5m, 1h)
"""
import os
import random
import uuid
from datetime import date, timedelta

from locust import HttpUser, task, between, tag


# Test configuration
TEST_AUTH_TOKEN = os.environ.get('TEST_AUTH_TOKEN', 'test-token')
TEST_AGENCY_ID = os.environ.get('TEST_AGENCY_ID', str(uuid.uuid4()))


class AuthenticatedUser(HttpUser):
    """Base class for authenticated API users."""

    wait_time = between(1, 3)  # Wait 1-3 seconds between tasks
    abstract = True

    def on_start(self):
        """Set up authentication headers."""
        self.headers = {
            'Authorization': f'Bearer {TEST_AUTH_TOKEN}',
            'Content-Type': 'application/json',
        }


# =============================================================================
# Read-Heavy User (Most common usage pattern)
# =============================================================================

class DashboardUser(AuthenticatedUser):
    """
    Simulates a user checking their dashboard and browsing data.
    Most common usage pattern - read-heavy operations.
    """

    weight = 10  # 10x more likely than other user types

    @task(10)
    @tag('dashboard', 'p0')
    def get_dashboard_summary(self):
        """GET /api/dashboard/summary - Most frequent endpoint."""
        with self.client.get(
            '/api/dashboard/summary',
            headers=self.headers,
            name='/api/dashboard/summary',
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            elif response.status_code == 401:
                response.failure('Authentication required')
            else:
                response.failure(f'Unexpected status: {response.status_code}')

    @task(5)
    @tag('agents', 'p1')
    def get_agents_list(self):
        """GET /api/agents - Agent list with pagination."""
        page = random.randint(1, 5)
        with self.client.get(
            f'/api/agents/?view=table&page={page}&limit=20',
            headers=self.headers,
            name='/api/agents/',
        ) as response:
            pass

    @task(3)
    @tag('agents', 'p1')
    def get_agents_tree(self):
        """GET /api/agents/?view=tree - Hierarchy tree view."""
        with self.client.get(
            '/api/agents/?view=tree',
            headers=self.headers,
            name='/api/agents/?view=tree',
        ) as response:
            pass

    @task(5)
    @tag('deals', 'p1')
    def get_book_of_business(self):
        """GET /api/deals/book-of-business - Deal list with filters."""
        with self.client.get(
            '/api/deals/book-of-business',
            headers=self.headers,
            name='/api/deals/book-of-business',
        ) as response:
            pass

    @task(2)
    @tag('deals', 'p1')
    def get_filter_options(self):
        """GET /api/deals/filter-options - Static filter options."""
        with self.client.get(
            '/api/deals/filter-options',
            headers=self.headers,
            name='/api/deals/filter-options',
        ) as response:
            pass

    @task(3)
    @tag('scoreboard', 'p1')
    def get_scoreboard(self):
        """GET /api/scoreboard - Leaderboard data."""
        with self.client.get(
            '/api/scoreboard',
            headers=self.headers,
            name='/api/scoreboard',
        ) as response:
            pass


# =============================================================================
# Agent Manager User (Hierarchy operations)
# =============================================================================

class AgentManagerUser(AuthenticatedUser):
    """
    Simulates a manager viewing downlines and agent details.
    Hierarchy-heavy operations.
    """

    weight = 3

    @task(5)
    @tag('agents', 'hierarchy')
    def get_agent_downlines(self):
        """GET /api/agents/downlines - Agent's downline list."""
        agent_id = str(uuid.uuid4())  # In real test, use actual agent IDs
        with self.client.get(
            f'/api/agents/downlines?agentId={agent_id}',
            headers=self.headers,
            name='/api/agents/downlines',
        ) as response:
            pass

    @task(3)
    @tag('agents', 'positions')
    def get_agents_without_positions(self):
        """GET /api/agents/without-positions - Agents needing positions."""
        with self.client.get(
            '/api/agents/without-positions',
            headers=self.headers,
            name='/api/agents/without-positions',
        ) as response:
            pass

    @task(2)
    @tag('positions')
    def get_positions(self):
        """GET /api/positions - Position list."""
        with self.client.get(
            '/api/positions/',
            headers=self.headers,
            name='/api/positions/',
        ) as response:
            pass


# =============================================================================
# Payouts User (Commission tracking)
# =============================================================================

class PayoutsUser(AuthenticatedUser):
    """
    Simulates a user checking expected payouts and commissions.
    """

    weight = 2

    @task(5)
    @tag('payouts', 'p1')
    def get_expected_payouts(self):
        """GET /api/expected-payouts - Commission forecasts."""
        with self.client.get(
            '/api/expected-payouts/',
            headers=self.headers,
            name='/api/expected-payouts/',
        ) as response:
            pass

    @task(2)
    @tag('payouts', 'p1')
    def get_expected_payouts_filtered(self):
        """GET /api/expected-payouts with date filter."""
        start_date = (date.today() - timedelta(days=30)).isoformat()
        end_date = date.today().isoformat()
        with self.client.get(
            f'/api/expected-payouts/?startDate={start_date}&endDate={end_date}',
            headers=self.headers,
            name='/api/expected-payouts/?filtered',
        ) as response:
            pass


# =============================================================================
# SMS User (Communication features)
# =============================================================================

class SMSUser(AuthenticatedUser):
    """
    Simulates a user using SMS communication features.
    """

    weight = 2

    @task(5)
    @tag('sms', 'p1')
    def get_conversations(self):
        """GET /api/sms/conversations - SMS conversation list."""
        with self.client.get(
            '/api/sms/conversations',
            headers=self.headers,
            name='/api/sms/conversations',
        ) as response:
            pass

    @task(3)
    @tag('sms', 'p1')
    def get_drafts(self):
        """GET /api/sms/drafts - Draft messages."""
        with self.client.get(
            '/api/sms/drafts',
            headers=self.headers,
            name='/api/sms/drafts',
        ) as response:
            pass

    @task(2)
    @tag('sms', 'p1')
    def get_messages(self):
        """GET /api/sms/messages - Messages for a conversation."""
        conversation_id = str(uuid.uuid4())  # In real test, use actual IDs
        with self.client.get(
            f'/api/sms/messages?conversationId={conversation_id}',
            headers=self.headers,
            name='/api/sms/messages',
        ) as response:
            pass


# =============================================================================
# Search User (Search operations)
# =============================================================================

class SearchUser(AuthenticatedUser):
    """
    Simulates search operations across different entities.
    """

    weight = 2

    @task(5)
    @tag('search')
    def search_agents(self):
        """GET /api/search-agents - Agent search."""
        query = random.choice(['john', 'jane', 'smith', 'agent'])
        with self.client.get(
            f'/api/search-agents?q={query}',
            headers=self.headers,
            name='/api/search-agents',
        ) as response:
            pass

    @task(3)
    @tag('clients')
    def get_clients(self):
        """GET /api/clients - Client list."""
        with self.client.get(
            '/api/clients/',
            headers=self.headers,
            name='/api/clients/',
        ) as response:
            pass


# =============================================================================
# Reference Data User (Carriers/Products)
# =============================================================================

class ReferenceDataUser(AuthenticatedUser):
    """
    Simulates fetching reference data like carriers and products.
    """

    weight = 1

    @task(3)
    @tag('carriers')
    def get_carriers(self):
        """GET /api/carriers - Carrier list."""
        with self.client.get(
            '/api/carriers/',
            headers=self.headers,
            name='/api/carriers/',
        ) as response:
            pass

    @task(2)
    @tag('carriers')
    def get_carriers_with_products(self):
        """GET /api/carriers/with-products - Carriers with products."""
        with self.client.get(
            '/api/carriers/with-products',
            headers=self.headers,
            name='/api/carriers/with-products',
        ) as response:
            pass

    @task(3)
    @tag('products')
    def get_products(self):
        """GET /api/products - Product list."""
        with self.client.get(
            '/api/products/',
            headers=self.headers,
            name='/api/products/',
        ) as response:
            pass


# =============================================================================
# Spike Test User (Stress testing)
# =============================================================================

class SpikeTestUser(AuthenticatedUser):
    """
    Used for spike testing - rapid requests to simulate traffic spikes.
    Only use with --tags spike
    """

    wait_time = between(0.1, 0.5)  # Very short wait
    weight = 0  # Don't include in normal runs

    @task
    @tag('spike')
    def rapid_dashboard(self):
        """Rapid dashboard requests for spike testing."""
        self.client.get(
            '/api/dashboard/summary',
            headers=self.headers,
            name='/api/dashboard/summary [spike]',
        )

    @task
    @tag('spike')
    def rapid_agents(self):
        """Rapid agent list requests for spike testing."""
        self.client.get(
            '/api/agents/',
            headers=self.headers,
            name='/api/agents/ [spike]',
        )
