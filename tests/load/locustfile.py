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

from locust import HttpUser, between, tag, task

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
        ):
            pass

    @task(3)
    @tag('agents', 'p1')
    def get_agents_tree(self):
        """GET /api/agents/?view=tree - Hierarchy tree view."""
        with self.client.get(
            '/api/agents/?view=tree',
            headers=self.headers,
            name='/api/agents/?view=tree',
        ):
            pass

    @task(5)
    @tag('deals', 'p1')
    def get_book_of_business(self):
        """GET /api/deals/book-of-business - Deal list with filters."""
        with self.client.get(
            '/api/deals/book-of-business',
            headers=self.headers,
            name='/api/deals/book-of-business',
        ):
            pass

    @task(3)
    @tag('deals', 'filters')
    def get_book_with_complex_filters(self):
        """GET /api/deals/book-of-business with multiple filters."""
        params = {
            'status_standardized': 'active',
            'date_from': (date.today() - timedelta(days=90)).isoformat(),
            'date_to': date.today().isoformat(),
            'view': 'downlines',
            'limit': 50,
            'effective_date_sort': 'newest',
        }
        with self.client.get(
            '/api/deals/book-of-business/',
            params=params,
            headers=self.headers,
            name='/api/deals/book-of-business [complex filters]',
        ):
            pass

    @task(2)
    @tag('deals', 'filters')
    def get_book_with_date_range(self):
        """GET /api/deals/book-of-business with date range filter."""
        days_back = random.choice([30, 60, 90, 180, 365])
        params = {
            'date_from': (date.today() - timedelta(days=days_back)).isoformat(),
            'date_to': date.today().isoformat(),
            'limit': 25,
        }
        with self.client.get(
            '/api/deals/book-of-business/',
            params=params,
            headers=self.headers,
            name='/api/deals/book-of-business [date range]',
        ):
            pass

    @task(2)
    @tag('deals', 'p1')
    def get_filter_options(self):
        """GET /api/deals/filter-options - Static filter options."""
        with self.client.get(
            '/api/deals/filter-options',
            headers=self.headers,
            name='/api/deals/filter-options',
        ):
            pass

    @task(3)
    @tag('scoreboard', 'p1')
    def get_scoreboard(self):
        """GET /api/scoreboard - Leaderboard data."""
        with self.client.get(
            '/api/scoreboard',
            headers=self.headers,
            name='/api/scoreboard',
        ):
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
        ):
            pass

    @task(3)
    @tag('agents', 'positions')
    def get_agents_without_positions(self):
        """GET /api/agents/without-positions - Agents needing positions."""
        with self.client.get(
            '/api/agents/without-positions',
            headers=self.headers,
            name='/api/agents/without-positions',
        ):
            pass

    @task(2)
    @tag('positions')
    def get_positions(self):
        """GET /api/positions - Position list."""
        with self.client.get(
            '/api/positions/',
            headers=self.headers,
            name='/api/positions/',
        ):
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
        ):
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
        ):
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
        ):
            pass

    @task(3)
    @tag('sms', 'p1')
    def get_drafts(self):
        """GET /api/sms/drafts - Draft messages."""
        with self.client.get(
            '/api/sms/drafts',
            headers=self.headers,
            name='/api/sms/drafts',
        ):
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
        ):
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
        ):
            pass

    @task(3)
    @tag('clients')
    def get_clients(self):
        """GET /api/clients - Client list."""
        with self.client.get(
            '/api/clients/',
            headers=self.headers,
            name='/api/clients/',
        ):
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
        ):
            pass

    @task(2)
    @tag('carriers')
    def get_carriers_agency(self):
        """GET /api/carriers/agency - Agency-specific carriers (P2-030)."""
        with self.client.get(
            '/api/carriers/agency',
            headers=self.headers,
            name='/api/carriers/agency',
        ):
            pass

    @task(2)
    @tag('carriers')
    def get_carriers_with_products(self):
        """GET /api/carriers/with-products - Carriers with products."""
        with self.client.get(
            '/api/carriers/with-products',
            headers=self.headers,
            name='/api/carriers/with-products',
        ):
            pass

    @task(3)
    @tag('products')
    def get_products(self):
        """GET /api/products - Product list."""
        with self.client.get(
            '/api/products/',
            headers=self.headers,
            name='/api/products/',
        ):
            pass


# =============================================================================
# Spike Test User (Stress testing)
# =============================================================================

# =============================================================================
# AI Chat User (Pro/Expert tier features)
# =============================================================================

class AIUser(AuthenticatedUser):
    """
    Simulates AI chat usage (Pro/Expert tier).
    Tests AI conversation endpoints.
    """

    weight = 2

    def on_start(self):
        """Set up authentication and initialize conversation ID."""
        super().on_start()
        self.conversation_id = None

    @task(5)
    @tag('ai', 'p2')
    def list_ai_conversations(self):
        """GET /api/ai/conversations - List AI conversations."""
        with self.client.get(
            '/api/ai/conversations/',
            headers=self.headers,
            name='/api/ai/conversations',
        ) as response:
            if response.status_code == 200:
                data = response.json()
                conversations = data.get('conversations', [])
                if conversations:
                    self.conversation_id = conversations[0].get('id')

    @task(3)
    @tag('ai', 'p2')
    def get_ai_conversation_messages(self):
        """GET /api/ai/conversations/{id}/messages - Get conversation messages."""
        if self.conversation_id:
            with self.client.get(
                f'/api/ai/conversations/{self.conversation_id}/messages/',
                headers=self.headers,
                name='/api/ai/conversations/[id]/messages',
            ):
                pass

    @task(2)
    @tag('ai', 'p2')
    def create_ai_conversation(self):
        """POST /api/ai/conversations - Create new AI conversation."""
        with self.client.post(
            '/api/ai/conversations/',
            json={'title': 'Load test conversation'},
            headers=self.headers,
            name='/api/ai/conversations [create]',
        ) as response:
            if response.status_code in [200, 201]:
                data = response.json()
                self.conversation_id = data.get('id')

    @task(1)
    @tag('ai', 'p2')
    def send_ai_message(self):
        """POST /api/ai/messages - Send message to AI."""
        if self.conversation_id:
            with self.client.post(
                f'/api/ai/conversations/{self.conversation_id}/messages/',
                json={'content': 'What is my production this month?'},
                headers=self.headers,
                name='/api/ai/messages [send]',
            ):
                pass


# =============================================================================
# Admin Write User (Write operations)
# =============================================================================

class AdminWriteUser(AuthenticatedUser):
    """
    Simulates admin write operations.
    Lower weight as writes are less common than reads.
    """

    weight = 1

    @task(2)
    @tag('agents', 'write')
    def assign_position(self):
        """POST /api/agents/assign-position - Assign position to agent."""
        with self.client.post(
            '/api/agents/assign-position/',
            json={
                'agent_id': str(uuid.uuid4()),
                'position_id': str(uuid.uuid4()),
            },
            headers=self.headers,
            name='/api/agents/assign-position',
        ):
            pass

    @task(2)
    @tag('sms', 'write')
    def send_sms_message(self):
        """POST /api/sms/messages - Send SMS message."""
        conversation_id = str(uuid.uuid4())
        with self.client.post(
            f'/api/sms/messages/{conversation_id}/',
            json={'content': 'Load test message'},
            headers=self.headers,
            name='/api/sms/messages [send]',
        ):
            pass

    @task(1)
    @tag('sms', 'write')
    def approve_draft(self):
        """POST /api/sms/drafts/{id}/approve - Approve draft message."""
        draft_id = str(uuid.uuid4())
        with self.client.post(
            f'/api/sms/drafts/{draft_id}/approve/',
            headers=self.headers,
            name='/api/sms/drafts/approve',
        ):
            pass

    @task(1)
    @tag('clients', 'write')
    def update_client(self):
        """PATCH /api/clients/{id} - Update client info."""
        client_id = str(uuid.uuid4())
        with self.client.patch(
            f'/api/clients/{client_id}/',
            json={'notes': 'Updated via load test'},
            headers=self.headers,
            name='/api/clients [update]',
        ):
            pass


# =============================================================================
# Analytics User (Analytics endpoints)
# =============================================================================

class AnalyticsUser(AuthenticatedUser):
    """
    Simulates users accessing analytics endpoints.
    """

    weight = 2

    @task(5)
    @tag('analytics', 'p2')
    def get_production_summary(self):
        """GET /api/analytics/production-summary - Production metrics."""
        with self.client.get(
            '/api/analytics/production-summary/',
            headers=self.headers,
            name='/api/analytics/production-summary',
        ):
            pass

    @task(3)
    @tag('analytics', 'p2')
    def get_production_trends(self):
        """GET /api/analytics/production-trends - Historical trends."""
        params = {
            'period': random.choice(['week', 'month', 'quarter', 'year']),
        }
        with self.client.get(
            '/api/analytics/production-trends/',
            params=params,
            headers=self.headers,
            name='/api/analytics/production-trends',
        ):
            pass

    @task(2)
    @tag('analytics', 'p2')
    def get_carrier_breakdown(self):
        """GET /api/analytics/carrier-breakdown - Carrier distribution."""
        with self.client.get(
            '/api/analytics/carrier-breakdown/',
            headers=self.headers,
            name='/api/analytics/carrier-breakdown',
        ):
            pass

    @task(2)
    @tag('analytics', 'p2')
    def get_status_distribution(self):
        """GET /api/analytics/status-distribution - Deal status breakdown."""
        with self.client.get(
            '/api/analytics/status-distribution/',
            headers=self.headers,
            name='/api/analytics/status-distribution',
        ):
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


# =============================================================================
# Stress Test User (High-frequency stress testing)
# =============================================================================

class StressTestUser(AuthenticatedUser):
    """
    High-frequency stress testing.
    Used for identifying breaking points.
    Only use with --tags stress
    """

    wait_time = between(0.1, 0.5)  # Very fast requests
    weight = 0  # Don't include in normal runs

    @task(10)
    @tag('stress')
    def stress_dashboard(self):
        """Stress test dashboard endpoint."""
        self.client.get('/api/dashboard/summary', headers=self.headers)

    @task(5)
    @tag('stress')
    def stress_book_of_business(self):
        """Stress test book-of-business endpoint."""
        self.client.get('/api/deals/book-of-business/', headers=self.headers)

    @task(3)
    @tag('stress')
    def stress_agents(self):
        """Stress test agents list."""
        self.client.get('/api/agents/', headers=self.headers)

    @task(3)
    @tag('stress')
    def stress_scoreboard(self):
        """Stress test scoreboard."""
        self.client.get('/api/scoreboard', headers=self.headers)

    @task(2)
    @tag('stress')
    def stress_payouts(self):
        """Stress test expected payouts."""
        self.client.get('/api/expected-payouts/', headers=self.headers)


# =============================================================================
# Combined Scenario User (Realistic user journeys)
# =============================================================================

class RealisticUserJourney(AuthenticatedUser):
    """
    Simulates realistic user journeys through the application.
    Represents complete workflows, not just individual endpoints.
    """

    weight = 3

    @task(5)
    @tag('journey', 'morning-review')
    def morning_dashboard_review(self):
        """Morning dashboard check workflow."""
        # Start with dashboard
        self.client.get('/api/dashboard/summary', headers=self.headers,
                        name='[journey] dashboard')
        # Check scoreboard
        self.client.get('/api/scoreboard', headers=self.headers,
                        name='[journey] scoreboard')
        # Review book of business
        self.client.get('/api/deals/book-of-business/', headers=self.headers,
                        name='[journey] book-of-business')

    @task(3)
    @tag('journey', 'client-review')
    def client_review_workflow(self):
        """Client review and follow-up workflow."""
        # Get clients
        self.client.get('/api/clients/', headers=self.headers,
                        name='[journey] clients')
        # Check conversations
        self.client.get('/api/sms/conversations', headers=self.headers,
                        name='[journey] conversations')
        # Check drafts for approval
        self.client.get('/api/sms/drafts', headers=self.headers,
                        name='[journey] drafts')

    @task(2)
    @tag('journey', 'team-review')
    def team_management_workflow(self):
        """Manager team review workflow."""
        # Get agent list
        self.client.get('/api/agents/?view=table', headers=self.headers,
                        name='[journey] agents-table')
        # Get hierarchy tree
        self.client.get('/api/agents/?view=tree', headers=self.headers,
                        name='[journey] agents-tree')
        # Check downlines
        self.client.get('/api/agents/downlines/', headers=self.headers,
                        name='[journey] downlines')
        # Check production
        self.client.get('/api/expected-payouts/', headers=self.headers,
                        name='[journey] payouts')
