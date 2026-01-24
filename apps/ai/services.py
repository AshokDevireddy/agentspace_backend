"""
AI Services (P2-036, P2-037, P2-038)

Business logic for AI chat, suggestions, and analytics insights.
Uses OpenAI API for LLM capabilities.
"""
import json
import logging
import uuid
from dataclasses import dataclass
from typing import Optional

from decouple import config
from django.db import connection
from openai import OpenAI

from apps.core.authentication import AuthenticatedUser

logger = logging.getLogger(__name__)

# OpenAI client configuration
OPENAI_API_KEY = config('OPENAI_API_KEY', default='')
OPENAI_MODEL = config('OPENAI_MODEL', default='gpt-4o-mini')
OPENAI_MAX_TOKENS = config('OPENAI_MAX_TOKENS', default=4096, cast=int)

# System prompts
CHAT_SYSTEM_PROMPT = """You are an AI assistant for AgentSpace, an insurance agency management platform.
You help insurance agents with:
- Understanding their performance metrics and analytics
- Providing insights on their book of business
- Answering questions about deals, clients, and commissions
- Offering advice on improving sales and client relationships

Be concise, professional, and data-driven. When referencing specific data, cite the metrics clearly.
If you don't have enough context to answer a question, ask for clarification."""

SUGGESTIONS_SYSTEM_PROMPT = """You are an AI assistant that provides actionable suggestions for insurance agents.
Based on the provided context, generate specific, actionable recommendations.
Focus on:
- Next best actions for deals
- Client outreach opportunities
- Performance improvement tips
- Risk identification

Format your response as a JSON array of suggestion objects with 'title', 'description', 'priority' (high/medium/low), and 'category' fields."""

ANALYTICS_SYSTEM_PROMPT = """You are an AI analyst for an insurance agency platform.
Analyze the provided metrics and generate insights.
Focus on:
- Trends and patterns
- Anomalies or concerns
- Opportunities for improvement
- Comparisons to benchmarks

Format your response as a JSON object with 'summary', 'insights' (array), 'recommendations' (array), and 'key_metrics' (object) fields."""


@dataclass
class AIResponse:
    """Response from AI service."""
    content: str
    role: str = 'assistant'
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    tool_calls: Optional[dict] = None
    error: Optional[str] = None


def get_openai_client() -> Optional[OpenAI]:
    """Get configured OpenAI client."""
    if not OPENAI_API_KEY:
        logger.warning('OPENAI_API_KEY not configured')
        return None
    return OpenAI(api_key=OPENAI_API_KEY)


def generate_chat_response(
    user: AuthenticatedUser,
    conversation_id: uuid.UUID,
    user_message: str,
    context: Optional[dict] = None,
) -> AIResponse:
    """
    Generate AI response to user message.

    Args:
        user: The authenticated user
        conversation_id: The conversation ID
        user_message: The user's message
        context: Optional context data (analytics, deals, etc.)

    Returns:
        AIResponse with generated content and token usage
    """
    client = get_openai_client()
    if not client:
        return AIResponse(
            content="AI features are not configured. Please contact support.",
            error="OPENAI_API_KEY not configured"
        )

    try:
        # Get conversation history
        messages = _get_conversation_history(conversation_id, limit=20)

        # Build message list
        openai_messages = [
            {"role": "system", "content": CHAT_SYSTEM_PROMPT}
        ]

        # Add context if provided
        if context:
            context_message = f"User context:\n{json.dumps(context, indent=2)}"
            openai_messages.append({
                "role": "system",
                "content": context_message
            })

        # Add conversation history
        for msg in messages:
            openai_messages.append({
                "role": msg['role'],
                "content": msg['content']
            })

        # Add current user message
        openai_messages.append({
            "role": "user",
            "content": user_message
        })

        # Call OpenAI API
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=openai_messages,
            max_tokens=OPENAI_MAX_TOKENS,
            temperature=0.7,
        )

        # Extract response
        choice = response.choices[0]
        usage = response.usage

        return AIResponse(
            content=choice.message.content or "",
            role="assistant",
            input_tokens=usage.prompt_tokens if usage else 0,
            output_tokens=usage.completion_tokens if usage else 0,
            total_tokens=usage.total_tokens if usage else 0,
        )

    except Exception as e:
        logger.error(f"OpenAI API error: {e}")
        return AIResponse(
            content="I encountered an error processing your request. Please try again.",
            error=str(e)
        )


def generate_suggestions(
    user: AuthenticatedUser,
    suggestion_type: str = 'general',
    context: Optional[dict] = None,
) -> list[dict]:
    """
    Generate AI-powered suggestions for the user.

    Args:
        user: The authenticated user
        suggestion_type: Type of suggestions ('deals', 'clients', 'performance', 'general')
        context: Context data for generating suggestions

    Returns:
        List of suggestion dictionaries
    """
    client = get_openai_client()
    if not client:
        return []

    try:
        # Build context message
        context_parts = [f"User: {user.first_name} {user.last_name}"]
        context_parts.append(f"Agency ID: {user.agency_id}")
        context_parts.append(f"Suggestion type requested: {suggestion_type}")

        if context:
            context_parts.append(f"Additional context:\n{json.dumps(context, indent=2)}")

        # Call OpenAI API
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": SUGGESTIONS_SYSTEM_PROMPT},
                {"role": "user", "content": "\n".join(context_parts)}
            ],
            max_tokens=2048,
            temperature=0.7,
            response_format={"type": "json_object"}
        )

        # Parse response
        content = response.choices[0].message.content or "{}"
        result = json.loads(content)

        # Handle array or object response
        if isinstance(result, list):
            return result
        if 'suggestions' in result:
            return result['suggestions']
        return []

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse suggestions JSON: {e}")
        return []
    except Exception as e:
        logger.error(f"OpenAI API error for suggestions: {e}")
        return []


def generate_analytics_insights(
    user: AuthenticatedUser,
    analytics_data: dict,
    insight_type: str = 'general',
) -> dict:
    """
    Generate AI-powered insights from analytics data.

    Args:
        user: The authenticated user
        analytics_data: Analytics data to analyze
        insight_type: Type of insights ('performance', 'revenue', 'team', 'general')

    Returns:
        Dictionary with insights, recommendations, and key metrics
    """
    client = get_openai_client()
    if not client:
        return {
            "summary": "AI insights are not available.",
            "insights": [],
            "recommendations": [],
            "key_metrics": {}
        }

    try:
        # Build prompt
        prompt_parts = [
            f"Analyze the following {insight_type} data for an insurance agent:",
            f"User: {user.first_name} {user.last_name}",
            f"Analytics Data:\n{json.dumps(analytics_data, indent=2)}",
            "Generate insights, recommendations, and highlight key metrics."
        ]

        # Call OpenAI API
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": ANALYTICS_SYSTEM_PROMPT},
                {"role": "user", "content": "\n\n".join(prompt_parts)}
            ],
            max_tokens=2048,
            temperature=0.5,
            response_format={"type": "json_object"}
        )

        # Parse response
        content = response.choices[0].message.content or "{}"
        result = json.loads(content)

        # Ensure expected structure
        return {
            "summary": result.get("summary", ""),
            "insights": result.get("insights", []),
            "recommendations": result.get("recommendations", []),
            "key_metrics": result.get("key_metrics", {})
        }

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse analytics insights JSON: {e}")
        return {
            "summary": "Failed to generate insights.",
            "insights": [],
            "recommendations": [],
            "key_metrics": {}
        }
    except Exception as e:
        logger.error(f"OpenAI API error for analytics: {e}")
        return {
            "summary": "An error occurred generating insights.",
            "insights": [],
            "recommendations": [],
            "key_metrics": {}
        }


def save_ai_message(
    conversation_id: uuid.UUID,
    role: str,
    content: str,
    input_tokens: int = 0,
    output_tokens: int = 0,
    tokens_used: int = 0,
    tool_calls: Optional[dict] = None,
    tool_results: Optional[dict] = None,
) -> Optional[dict]:
    """
    Save an AI message to the database.

    Args:
        conversation_id: The conversation ID
        role: Message role (user, assistant, system)
        content: Message content
        input_tokens: Number of input tokens
        output_tokens: Number of output tokens
        tokens_used: Total tokens used
        tool_calls: Optional tool calls JSON
        tool_results: Optional tool results JSON

    Returns:
        Saved message dict or None on error
    """
    try:
        message_id = uuid.uuid4()

        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO public.ai_messages
                (id, conversation_id, role, content, tool_calls, tool_results,
                 input_tokens, output_tokens, tokens_used)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, role, content, created_at
            """, [
                str(message_id),
                str(conversation_id),
                role,
                content,
                json.dumps(tool_calls) if tool_calls else None,
                json.dumps(tool_results) if tool_results else None,
                input_tokens,
                output_tokens,
                tokens_used
            ])
            row = cursor.fetchone()

            # Update conversation updated_at
            cursor.execute("""
                UPDATE public.ai_conversations
                SET updated_at = NOW()
                WHERE id = %s
            """, [str(conversation_id)])

        if row:
            return {
                'id': str(row[0]),
                'role': row[1],
                'content': row[2],
                'created_at': row[3].isoformat() if row[3] else None,
            }
        return None

    except Exception as e:
        logger.error(f"Failed to save AI message: {e}")
        return None


def _get_conversation_history(
    conversation_id: uuid.UUID,
    limit: int = 20,
) -> list[dict]:
    """Get recent messages from a conversation for context."""
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT role, content
                FROM public.ai_messages
                WHERE conversation_id = %s
                ORDER BY created_at DESC
                LIMIT %s
            """, [str(conversation_id), limit])
            rows = cursor.fetchall()

        # Reverse to get chronological order
        messages = []
        for row in reversed(rows):
            messages.append({
                'role': row[0],
                'content': row[1]
            })
        return messages

    except Exception as e:
        logger.error(f"Failed to get conversation history: {e}")
        return []


def get_user_context(user: AuthenticatedUser) -> dict:
    """
    Get contextual data about the user for AI conversations.

    Args:
        user: The authenticated user

    Returns:
        Dictionary with user context data
    """
    try:
        with connection.cursor() as cursor:
            # Get basic user stats
            cursor.execute("""
                SELECT
                    (SELECT COUNT(*) FROM public.deals WHERE agent_id = %s) as deal_count,
                    (SELECT COUNT(*) FROM public.clients WHERE agent_id = %s) as client_count,
                    (SELECT COALESCE(SUM(annual_premium), 0) FROM public.deals
                     WHERE agent_id = %s AND status_standardized = 'active') as total_premium
            """, [str(user.id), str(user.id), str(user.id)])
            row = cursor.fetchone()

        return {
            'user_name': f"{user.first_name} {user.last_name}",
            'role': user.role,
            'is_admin': user.is_admin,
            'subscription_tier': user.subscription_tier,
            'deal_count': row[0] if row else 0,
            'client_count': row[1] if row else 0,
            'total_active_premium': float(row[2]) if row and row[2] else 0.0,
        }

    except Exception as e:
        logger.error(f"Failed to get user context: {e}")
        return {
            'user_name': f"{user.first_name} {user.last_name}",
            'role': user.role,
        }
