"""
Dashboard API Views (P2-032, P2-033, P2-034, P2-035)

Endpoints:
- /api/dashboard/summary - Dashboard summary data
- /api/dashboard/scoreboard - Scoreboard/leaderboard
- /api/dashboard/production - Production metrics
- /api/dashboard/widgets - Widget management
- /api/dashboard/reports - Report generation
- /api/dashboard/scheduled-reports - Scheduled reports
- /api/dashboard/export - Data export
"""
import logging
from datetime import date, datetime
from uuid import UUID

from django.http import HttpResponse
from rest_framework import status
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.core.constants import PAGINATION, EXPORT_FORMATS
from apps.core.mixins import AuthenticatedAPIView
from .services import (
    UserContext,
    get_dashboard_summary,
    get_scoreboard_data,
    get_scoreboard_lapsed_deals,
    get_scoreboard_with_billing_cycle,
    get_production_data,
    WidgetInput,
    get_user_widgets,
    create_widget,
    update_widget,
    delete_widget,
    get_widget_by_id,
    reorder_widgets,
    ReportInput,
    create_report,
    get_report_by_id,
    list_reports,
    generate_report,
    ScheduledReportInput,
    create_scheduled_report,
    get_scheduled_report_by_id,
    list_scheduled_reports,
    update_scheduled_report,
    delete_scheduled_report,
    export_to_csv,
    export_to_excel,
    export_to_pdf,
)

logger = logging.getLogger(__name__)


def _parse_date(date_str: str) -> date | None:
    """Parse date string in YYYY-MM-DD format."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return None


def _get_user_ctx(user) -> UserContext:
    """Convert AuthenticatedUser to UserContext for services."""
    return UserContext(
        internal_user_id=user.id,
        auth_user_id=user.auth_user_id,
        agency_id=user.agency_id,
        email=user.email or "",
        is_admin=user.is_admin or user.role == "admin",
    )


class DashboardSummaryView(AuthenticatedAPIView, APIView):
    """GET /api/dashboard/summary - Dashboard summary data."""

    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = self.get_user(request)
        user_ctx = _get_user_ctx(user)

        as_of_date_str = request.query_params.get("as_of_date")
        as_of_date = _parse_date(as_of_date_str) if as_of_date_str else None

        data = get_dashboard_summary(user_ctx, as_of_date)
        return Response(data)


class ScoreboardView(AuthenticatedAPIView, APIView):
    """GET /api/dashboard/scoreboard - Scoreboard/leaderboard data."""

    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = self.get_user(request)
        user_ctx = _get_user_ctx(user)

        start_date_str = request.query_params.get("start_date")
        end_date_str = request.query_params.get("end_date")

        if not start_date_str or not end_date_str:
            return Response(
                {"error": "start_date and end_date are required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        start_date = _parse_date(start_date_str)
        end_date = _parse_date(end_date_str)

        if not start_date or not end_date:
            return Response(
                {"error": "Invalid date format. Use YYYY-MM-DD"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        data = get_scoreboard_data(user_ctx, start_date, end_date)
        return Response(data)


class ScoreboardLapsedView(AuthenticatedAPIView, APIView):
    """GET /api/dashboard/scoreboard-lapsed - Scoreboard with lapsed deals."""

    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = self.get_user(request)
        user_ctx = _get_user_ctx(user)

        start_date_str = request.query_params.get("start_date")
        end_date_str = request.query_params.get("end_date")

        if not start_date_str or not end_date_str:
            return Response(
                {"error": "start_date and end_date are required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        start_date = _parse_date(start_date_str)
        end_date = _parse_date(end_date_str)

        if not start_date or not end_date:
            return Response(
                {"error": "Invalid date format. Use YYYY-MM-DD"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        assumed_months = int(request.query_params.get("assumed_months_till_lapse", 0) or 0)
        scope = request.query_params.get("scope", "agency")
        if scope not in ("agency", "downline"):
            scope = "agency"
        submitted = request.query_params.get("submitted", "false").lower() in ("true", "1")

        data = get_scoreboard_lapsed_deals(
            user_ctx,
            start_date,
            end_date,
            assumed_months_till_lapse=assumed_months,
            scope=scope,
            submitted=submitted,
        )
        return Response(data)


class ScoreboardBillingCycleView(AuthenticatedAPIView, APIView):
    """GET /api/dashboard/scoreboard-billing-cycle - Scoreboard with billing cycle."""

    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = self.get_user(request)
        user_ctx = _get_user_ctx(user)

        start_date_str = request.query_params.get("start_date")
        end_date_str = request.query_params.get("end_date")

        if not start_date_str or not end_date_str:
            return Response(
                {"error": "start_date and end_date are required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        start_date = _parse_date(start_date_str)
        end_date = _parse_date(end_date_str)

        if not start_date or not end_date:
            return Response(
                {"error": "Invalid date format. Use YYYY-MM-DD"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        scope = request.query_params.get("scope", "agency")
        if scope not in ("agency", "downline"):
            scope = "agency"

        data = get_scoreboard_with_billing_cycle(user_ctx, start_date, end_date, scope=scope)
        return Response(data)


class ProductionView(AuthenticatedAPIView, APIView):
    """GET /api/dashboard/production - Production metrics."""

    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = self.get_user(request)
        user_ctx = _get_user_ctx(user)

        agent_ids_str = request.query_params.get("agent_ids", "")
        start_date_str = request.query_params.get("start_date")
        end_date_str = request.query_params.get("end_date")

        if not agent_ids_str:
            return Response(
                {"error": "agent_ids is required"}, status=status.HTTP_400_BAD_REQUEST
            )

        if not start_date_str or not end_date_str:
            return Response(
                {"error": "start_date and end_date are required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        start_date = _parse_date(start_date_str)
        end_date = _parse_date(end_date_str)

        if not start_date or not end_date:
            return Response(
                {"error": "Invalid date format. Use YYYY-MM-DD"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        agent_ids = [aid.strip() for aid in agent_ids_str.split(",") if aid.strip()]
        if not agent_ids:
            return Response(
                {"error": "No valid agent_ids provided"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        data = get_production_data(user_ctx, agent_ids, start_date, end_date)
        return Response(data)


class WidgetsListView(AuthenticatedAPIView, APIView):
    """
    GET /api/dashboard/widgets - List user's widgets
    POST /api/dashboard/widgets - Create a new widget (P2-032)
    """

    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = self.get_user(request)
        widgets = get_user_widgets(user.id)
        return Response({"widgets": widgets})

    def post(self, request):
        user = self.get_user(request)

        data = request.data
        widget_type = data.get("widget_type")
        title = data.get("title", "").strip()

        if not widget_type or not title:
            return Response(
                {"error": "widget_type and title are required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        widget = create_widget(
            user.id,
            WidgetInput(
                widget_type=widget_type,
                title=title,
                position=data.get("position", 0),
                config=data.get("config"),
                is_visible=data.get("is_visible", True),
            ),
        )
        return Response(widget, status=status.HTTP_201_CREATED)


class WidgetDetailView(AuthenticatedAPIView, APIView):
    """
    GET /api/dashboard/widgets/{id} - Get widget details
    PUT /api/dashboard/widgets/{id} - Update widget
    DELETE /api/dashboard/widgets/{id} - Delete widget (P2-032)
    """

    permission_classes = [IsAuthenticated]

    def get(self, request, widget_id):
        user = self.get_user(request)
        widget_uuid = self.parse_uuid(widget_id, "widget_id")

        widget = get_widget_by_id(widget_uuid, user.id)
        if not widget:
            return Response(
                {"error": "Widget not found"}, status=status.HTTP_404_NOT_FOUND
            )
        return Response(widget)

    def put(self, request, widget_id):
        user = self.get_user(request)
        widget_uuid = self.parse_uuid(widget_id, "widget_id")

        data = request.data
        widget_type = data.get("widget_type")
        title = data.get("title", "").strip()

        if not widget_type or not title:
            return Response(
                {"error": "widget_type and title are required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        widget = update_widget(
            widget_uuid,
            user.id,
            WidgetInput(
                widget_type=widget_type,
                title=title,
                position=data.get("position", 0),
                config=data.get("config"),
                is_visible=data.get("is_visible", True),
            ),
        )
        if not widget:
            return Response(
                {"error": "Widget not found"}, status=status.HTTP_404_NOT_FOUND
            )
        return Response(widget)

    def delete(self, request, widget_id):
        user = self.get_user(request)
        widget_uuid = self.parse_uuid(widget_id, "widget_id")

        deleted = delete_widget(widget_uuid, user.id)
        if not deleted:
            return Response(
                {"error": "Widget not found"}, status=status.HTTP_404_NOT_FOUND
            )
        return Response({"success": True})


class WidgetsReorderView(AuthenticatedAPIView, APIView):
    """POST /api/dashboard/widgets/reorder - Reorder widgets (P2-032)."""

    permission_classes = [IsAuthenticated]

    def post(self, request):
        user = self.get_user(request)

        positions = request.data.get("positions", [])
        if not positions:
            return Response(
                {"error": "positions is required"}, status=status.HTTP_400_BAD_REQUEST
            )

        widgets = reorder_widgets(user.id, positions)
        return Response({"widgets": widgets})


class ReportsListView(AuthenticatedAPIView, APIView):
    """
    GET /api/dashboard/reports - List reports
    POST /api/dashboard/reports - Create a new report (P2-033)
    """

    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = self.get_user(request)
        user_ctx = _get_user_ctx(user)

        limit = int(request.query_params.get("limit", 50))
        reports = list_reports(user_ctx, limit)
        return Response({"reports": reports})

    def post(self, request):
        user = self.get_user(request)
        user_ctx = _get_user_ctx(user)

        data = request.data
        report_type = data.get("report_type")
        title = data.get("title", "").strip()

        if not report_type or not title:
            return Response(
                {"error": "report_type and title are required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        report = create_report(
            user_ctx,
            ReportInput(
                report_type=report_type,
                title=title,
                parameters=data.get("parameters", {}),
                format=data.get("format", "csv"),
            ),
        )
        return Response(report, status=status.HTTP_201_CREATED)


class ReportDetailView(AuthenticatedAPIView, APIView):
    """GET /api/dashboard/reports/{id} - Get report details (P2-033)."""

    permission_classes = [IsAuthenticated]

    def get(self, request, report_id):
        user = self.get_user(request)
        user_ctx = _get_user_ctx(user)
        report_uuid = self.parse_uuid(report_id, "report_id")

        report = get_report_by_id(report_uuid, user_ctx)
        if not report:
            return Response(
                {"error": "Report not found"}, status=status.HTTP_404_NOT_FOUND
            )
        return Response(report)


class ReportGenerateView(AuthenticatedAPIView, APIView):
    """POST /api/dashboard/reports/{id}/generate - Generate a report (P2-033)."""

    permission_classes = [IsAuthenticated]

    def post(self, request, report_id):
        user = self.get_user(request)
        user_ctx = _get_user_ctx(user)
        report_uuid = self.parse_uuid(report_id, "report_id")

        result = generate_report(report_uuid, user_ctx)
        if not result:
            return Response(
                {"error": "Report not found or generation failed"},
                status=status.HTTP_404_NOT_FOUND,
            )
        return Response(result)


class ScheduledReportsListView(AuthenticatedAPIView, APIView):
    """
    GET /api/dashboard/scheduled-reports - List scheduled reports
    POST /api/dashboard/scheduled-reports - Create scheduled report (P2-034)
    """

    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = self.get_user(request)
        user_ctx = _get_user_ctx(user)

        reports = list_scheduled_reports(user_ctx)
        return Response({"scheduled_reports": reports})

    def post(self, request):
        user = self.get_user(request)
        user_ctx = _get_user_ctx(user)

        data = request.data
        report_type = data.get("report_type")
        title = data.get("title", "").strip()
        frequency = data.get("frequency")

        if not report_type or not title or not frequency:
            return Response(
                {"error": "report_type, title, and frequency are required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        report = create_scheduled_report(
            user_ctx,
            ScheduledReportInput(
                report_type=report_type,
                title=title,
                parameters=data.get("parameters", {}),
                format=data.get("format", "csv"),
                frequency=frequency,
                email_recipients=data.get("email_recipients", []),
                is_active=data.get("is_active", True),
            ),
        )
        return Response(report, status=status.HTTP_201_CREATED)


class ScheduledReportDetailView(AuthenticatedAPIView, APIView):
    """
    GET /api/dashboard/scheduled-reports/{id} - Get scheduled report details
    PUT /api/dashboard/scheduled-reports/{id} - Update scheduled report
    DELETE /api/dashboard/scheduled-reports/{id} - Delete scheduled report (P2-034)
    """

    permission_classes = [IsAuthenticated]

    def get(self, request, report_id):
        user = self.get_user(request)
        user_ctx = _get_user_ctx(user)
        report_uuid = self.parse_uuid(report_id, "report_id")

        report = get_scheduled_report_by_id(report_uuid, user_ctx)
        if not report:
            return Response(
                {"error": "Scheduled report not found"},
                status=status.HTTP_404_NOT_FOUND,
            )
        return Response(report)

    def put(self, request, report_id):
        user = self.get_user(request)
        user_ctx = _get_user_ctx(user)
        report_uuid = self.parse_uuid(report_id, "report_id")

        data = request.data
        report_type = data.get("report_type")
        title = data.get("title", "").strip()
        frequency = data.get("frequency")

        if not report_type or not title or not frequency:
            return Response(
                {"error": "report_type, title, and frequency are required"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        report = update_scheduled_report(
            report_uuid,
            user_ctx,
            ScheduledReportInput(
                report_type=report_type,
                title=title,
                parameters=data.get("parameters", {}),
                format=data.get("format", "csv"),
                frequency=frequency,
                email_recipients=data.get("email_recipients", []),
                is_active=data.get("is_active", True),
            ),
        )
        if not report:
            return Response(
                {"error": "Scheduled report not found"},
                status=status.HTTP_404_NOT_FOUND,
            )
        return Response(report)

    def delete(self, request, report_id):
        user = self.get_user(request)
        user_ctx = _get_user_ctx(user)
        report_uuid = self.parse_uuid(report_id, "report_id")

        deleted = delete_scheduled_report(report_uuid, user_ctx)
        if not deleted:
            return Response(
                {"error": "Scheduled report not found"},
                status=status.HTTP_404_NOT_FOUND,
            )
        return Response({"success": True})


class ExportView(AuthenticatedAPIView, APIView):
    """POST /api/dashboard/export - Export data (P2-035)."""

    permission_classes = [IsAuthenticated]

    def post(self, request):
        self.get_user(request)

        data = request.data.get("data", [])
        export_format = request.data.get("format", "csv")
        title = request.data.get("title", "Export")

        if not data:
            return Response(
                {"error": "data is required"}, status=status.HTTP_400_BAD_REQUEST
            )

        if export_format not in EXPORT_FORMATS:
            return Response(
                {"error": f"Invalid format. Use one of: {', '.join(EXPORT_FORMATS)}"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        if export_format == "csv":
            content = export_to_csv(data)
            response = HttpResponse(content, content_type="text/csv")
            response["Content-Disposition"] = f'attachment; filename="{title}.csv"'
            return response

        if export_format == "xlsx":
            content = export_to_excel(data, title)
            response = HttpResponse(
                content,
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            response["Content-Disposition"] = f'attachment; filename="{title}.xlsx"'
            return response

        if export_format == "pdf":
            content = export_to_pdf(data, title)
            response = HttpResponse(content, content_type="application/pdf")
            response["Content-Disposition"] = f'attachment; filename="{title}.pdf"'
            return response
