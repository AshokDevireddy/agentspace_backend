# CLAUDE.md - Django 6.0 + Django REST Framework - January 2026

> Standards and best practices for Django 6.0 with DRF, async support, and built-in Tasks framework.

---

## Django 6.0 Key Changes

### Python 3.12+ Required
Django 6.0 drops Python 3.10/3.11 support. Upgrade to Python 3.12+ for:
- 10-25% performance gains
- Improved asyncio performance
- Better error messages

### New Built-in Features
- **Tasks Framework** — native background jobs (replaces Celery for simple cases)
- **CSP Support** — built-in Content Security Policy
- **Template Partials** — reusable template fragments
- **AsyncPaginator** — first-class async pagination

---

## N+1 Query Prevention - CRITICAL

### ✅ ALWAYS Optimize Queries
```python
# views.py
class PostViewSet(viewsets.ModelViewSet):
    def get_queryset(self):
        return Post.objects.select_related(
            'author',           # ForeignKey - single query
            'author__company',  # Nested FK
        ).prefetch_related(
            'tags',             # ManyToMany - separate query
            'comments',         # Reverse FK
            Prefetch(           # Filtered prefetch
                'comments',
                queryset=Comment.objects.filter(is_approved=True),
                to_attr='approved_comments'
            ),
        )
```

### When to Use Each
| Relationship | Method | Query Type |
|-------------|--------|------------|
| ForeignKey (forward) | `select_related` | SQL JOIN |
| OneToOneField | `select_related` | SQL JOIN |
| ManyToMany | `prefetch_related` | Separate query |
| Reverse ForeignKey | `prefetch_related` | Separate query |

### ❌ DO NOT
- ❌ Access related objects without prefetching
- ❌ Call `.count()` in loops — use `annotate()`
- ❌ Filter in Python instead of database
- ❌ Use `fields = '__all__'` in serializers

---

## Async Support in Django 6.0

### ✅ Async Views and ORM
```python
# views.py
from django.http import JsonResponse

async def get_user_profile(request, user_id):
    # ✅ Native async ORM methods
    user = await User.objects.aget(id=user_id)
    post_count = await Post.objects.filter(author=user).acount()
    posts = [p async for p in Post.objects.filter(author=user)[:10]]
    
    return JsonResponse({
        'user': user.username,
        'post_count': post_count,
    })
```

### Async ORM Methods (Django 6.0)
```python
# Single object
user = await User.objects.aget(id=1)
user = await User.objects.afirst()

# Existence/Count
exists = await Post.objects.filter(author=user).aexists()
count = await Post.objects.filter(author=user).acount()

# Iteration
async for post in Post.objects.filter(author=user):
    print(post.title)

# Lists
posts = [p async for p in Post.objects.all()[:10]]
```

### ✅ AsyncPaginator (New in 6.0)
```python
from django.core.paginator import AsyncPaginator

async def list_posts(request):
    paginator = AsyncPaginator(Post.objects.all(), per_page=25)
    page = await paginator.aget_page(request.GET.get('page', 1))
    return render(request, 'posts.html', {'page': page})
```

---

## Background Tasks (Django 6.0)

### ✅ Built-in Tasks Framework
```python
# tasks.py
from django.tasks import task
from django.core.mail import send_mail

@task
def email_users(emails: list[str], subject: str, message: str):
    return send_mail(subject, message, None, emails)

# Usage
email_users.enqueue(
    emails=["user@example.com"],
    subject="Welcome",
    message="Hello there!",
)
```

### ❌ DO NOT
- ❌ Use Celery for simple background tasks — use built-in framework
- ❌ Forget that you still need a worker process
- ❌ Put heavy computation in tasks without timeouts

---

## Code Organization - CRITICAL

### Service Layer Pattern
```
app/
├── models.py        # Data models only
├── serializers.py   # DRF serializers
├── views.py         # Thin views, delegate to services
├── services.py      # Business logic, create/update operations
├── selectors.py     # Complex queries, filtering, annotations
├── tasks.py         # Background tasks
└── signals.py       # Signal handlers
```

### ✅ Services Example
```python
# services.py
from django.db import transaction

class PostService:
    @staticmethod
    @transaction.atomic
    def create_post(*, author: User, title: str, content: str, tags: list[int]) -> Post:
        post = Post.objects.create(
            author=author,
            title=title,
            content=content,
        )
        post.tags.set(tags)
        
        # Side effects
        notify_followers.enqueue(author_id=author.id, post_id=post.id)
        
        return post
```

### ✅ Selectors Example
```python
# selectors.py
from django.db.models import Count, Prefetch

class PostSelectors:
    @staticmethod
    def get_posts_with_stats(*, author_id: int | None = None):
        qs = Post.objects.select_related('author').prefetch_related(
            'tags',
            Prefetch('comments', queryset=Comment.objects.filter(is_approved=True)),
        ).annotate(
            comment_count=Count('comments'),
        )
        
        if author_id:
            qs = qs.filter(author_id=author_id)
        
        return qs
```

### ❌ DO NOT
- ❌ Put business logic in views
- ❌ Put queries in views — use selectors
- ❌ Create files over 300 lines — split them

---

## Custom QuerySet Managers - CRITICAL

### ✅ Chainable Query Methods
```python
# managers.py
from django.db import models

class PostQuerySet(models.QuerySet):
    def published(self):
        return self.filter(status='published', published_at__lte=timezone.now())

    def draft(self):
        return self.filter(status='draft')

    def by_author(self, author):
        return self.filter(author=author)

    def with_stats(self):
        return self.annotate(
            comment_count=Count('comments'),
            like_count=Count('likes'),
        )

    def popular(self, min_likes=10):
        return self.with_stats().filter(like_count__gte=min_likes)


class PostManager(models.Manager):
    def get_queryset(self):
        return PostQuerySet(self.model, using=self._db)

    def published(self):
        return self.get_queryset().published()

    def popular(self, min_likes=10):
        return self.get_queryset().popular(min_likes)


# models.py
class Post(models.Model):
    objects = PostManager()

    # Or simpler with as_manager()
    # objects = PostQuerySet.as_manager()
```

### ✅ Usage Examples
```python
# Clean, chainable queries
Post.objects.published().by_author(user).with_stats()
Post.objects.draft().filter(created_at__gte=last_week)
Post.objects.popular(min_likes=50).order_by('-like_count')
```

---

## DRF Throttling

### ✅ Rate Limiting Configuration
```python
# settings.py
REST_FRAMEWORK = {
    'DEFAULT_THROTTLE_CLASSES': [
        'rest_framework.throttling.AnonRateThrottle',
        'rest_framework.throttling.UserRateThrottle',
    ],
    'DEFAULT_THROTTLE_RATES': {
        'anon': '100/hour',      # Anonymous users
        'user': '1000/hour',     # Authenticated users
        'burst': '60/minute',    # For burst-limited endpoints
        'uploads': '20/day',     # For file uploads
    },
}
```

### ✅ Custom Throttle Classes
```python
# throttles.py
from rest_framework.throttling import UserRateThrottle

class BurstRateThrottle(UserRateThrottle):
    scope = 'burst'

class UploadRateThrottle(UserRateThrottle):
    scope = 'uploads'


# views.py
class FileUploadView(APIView):
    throttle_classes = [UploadRateThrottle]

    def post(self, request):
        ...
```

---

## Custom ViewSet Actions

### ✅ @action Decorator for Custom Endpoints
```python
from rest_framework.decorators import action
from rest_framework.response import Response

class PostViewSet(viewsets.ModelViewSet):
    queryset = Post.objects.all()
    serializer_class = PostSerializer

    # POST /posts/{id}/publish/
    @action(detail=True, methods=['post'])
    def publish(self, request, pk=None):
        post = self.get_object()
        PostService.publish(post, user=request.user)
        return Response({'status': 'published'})

    # POST /posts/{id}/archive/
    @action(detail=True, methods=['post'], permission_classes=[IsAdminUser])
    def archive(self, request, pk=None):
        post = self.get_object()
        post.archive()
        return Response({'status': 'archived'})

    # GET /posts/trending/
    @action(detail=False, methods=['get'])
    def trending(self, request):
        trending = PostSelectors.get_trending_posts()
        serializer = PostListSerializer(trending, many=True)
        return Response(serializer.data)

    # GET /posts/my-posts/
    @action(detail=False, methods=['get'], url_path='my-posts')
    def my_posts(self, request):
        posts = Post.objects.filter(author=request.user)
        serializer = PostListSerializer(posts, many=True)
        return Response(serializer.data)
```

### Action Parameters
| Parameter | Purpose |
|-----------|---------|
| `detail=True` | Operates on single object (`/posts/{id}/action/`) |
| `detail=False` | Operates on collection (`/posts/action/`) |
| `methods=['post']` | HTTP methods allowed |
| `url_path='custom-path'` | Override URL segment |
| `url_name='custom-name'` | Override URL name for reverse() |
| `permission_classes` | Override viewset permissions |
| `serializer_class` | Override viewset serializer |

---

## Bulk Operations - CRITICAL for Performance

### ✅ bulk_create() for Multiple Inserts
```python
# ❌ SLOW: N queries
for item in items:
    Post.objects.create(**item)

# ✅ FAST: 1 query
Post.objects.bulk_create([
    Post(title=item['title'], content=item['content'])
    for item in items
])

# With ignore_conflicts for upsert-like behavior
Post.objects.bulk_create(
    posts,
    ignore_conflicts=True,  # Skip duplicates
)

# With update_conflicts (PostgreSQL)
Post.objects.bulk_create(
    posts,
    update_conflicts=True,
    update_fields=['title', 'content'],
    unique_fields=['slug'],
)
```

### ✅ bulk_update() for Multiple Updates
```python
# ❌ SLOW: N queries
for post in posts:
    post.status = 'published'
    post.save()

# ✅ FAST: 1 query
for post in posts:
    post.status = 'published'

Post.objects.bulk_update(posts, fields=['status'])

# With batch_size for large datasets
Post.objects.bulk_update(posts, fields=['status'], batch_size=1000)
```

### ✅ update() for Conditional Bulk Updates
```python
# Update all matching rows in single query
Post.objects.filter(author=user, status='draft').update(
    status='archived',
    archived_at=timezone.now(),
)
```

### ✅ delete() for Bulk Deletion
```python
# Delete all matching rows
Post.objects.filter(status='spam').delete()

# Get count before delete
deleted_count, _ = Post.objects.filter(is_expired=True).delete()
```

---

## Serializers

### ✅ Separate Read/Write Serializers
```python
# serializers.py

class PostListSerializer(serializers.ModelSerializer):
    """Lightweight for list views"""
    author_name = serializers.CharField(source='author.username', read_only=True)
    
    class Meta:
        model = Post
        fields = ['id', 'title', 'author_name', 'created_at']


class PostDetailSerializer(serializers.ModelSerializer):
    """Full data for detail views"""
    author = UserSerializer(read_only=True)
    tags = TagSerializer(many=True, read_only=True)
    comment_count = serializers.IntegerField(read_only=True)
    
    class Meta:
        model = Post
        fields = ['id', 'title', 'content', 'author', 'tags', 'comment_count', 'created_at']


class PostCreateSerializer(serializers.ModelSerializer):
    """Write-only fields for creation"""
    tag_ids = serializers.PrimaryKeyRelatedField(
        queryset=Tag.objects.all(),
        many=True,
        write_only=True,
        source='tags',
    )
    
    class Meta:
        model = Post
        fields = ['title', 'content', 'tag_ids']
```

### ❌ DO NOT
- ❌ Use `fields = '__all__'` — explicitly list fields
- ❌ Use same serializer for list and detail views
- ❌ Access related objects in serializers without prefetching in viewset

---

## ViewSets

### ✅ Required Overrides
```python
class PostViewSet(viewsets.ModelViewSet):
    permission_classes = [IsAuthenticated]
    
    def get_queryset(self):
        # ALWAYS optimize queries
        return PostSelectors.get_posts_with_stats()
    
    def get_serializer_class(self):
        if self.action == 'list':
            return PostListSerializer
        if self.action in ['create', 'update', 'partial_update']:
            return PostCreateSerializer
        return PostDetailSerializer
    
    def perform_create(self, serializer):
        # Delegate to service layer
        PostService.create_post(
            author=self.request.user,
            **serializer.validated_data,
        )
```

---

## Content Security Policy (Django 6.0)

### ✅ Built-in CSP Support
```python
# settings.py
from django.utils.csp import CSP

SECURE_CSP = {
    "default-src": [CSP.SELF],
    "script-src": [CSP.SELF, CSP.NONCE],
    "style-src": [CSP.SELF, CSP.NONCE],
    "img-src": [CSP.SELF, "https:"],
}

MIDDLEWARE = [
    # ...
    'django.middleware.security.ContentSecurityPolicyMiddleware',
]
```

### ✅ Template Nonces
```html
{% load csp %}
<script nonce="{% csp_nonce %}">
  // Safe inline script
</script>
```

---

## Template Partials (Django 6.0)

### ✅ Define Reusable Fragments
```html
<!-- templates/components.html -->
{% partialdef card %}
<div class="card">
  <h3>{{ title }}</h3>
  <p>{{ content }}</p>
</div>
{% endpartialdef %}

<!-- templates/page.html -->
{% partial card title="Hello" content="World" %}
```

---

## Testing

### ✅ What to Test
```python
# tests/test_services.py
from django.test import TestCase

class PostServiceTests(TestCase):
    def test_create_post_with_tags(self):
        user = UserFactory()
        tags = TagFactory.create_batch(3)
        
        post = PostService.create_post(
            author=user,
            title="Test",
            content="Content",
            tags=[t.id for t in tags],
        )
        
        self.assertEqual(post.author, user)
        self.assertEqual(post.tags.count(), 3)
    
    def test_create_post_notifies_followers(self):
        # Test side effects
        ...
```

### ✅ Test Query Count
```python
def test_list_posts_query_count(self):
    PostFactory.create_batch(10, tags=5)  # 10 posts, 5 tags each
    
    with self.assertNumQueries(3):  # posts + authors + tags
        response = self.client.get('/api/posts/')
        self.assertEqual(len(response.data), 10)
```

---

## Performance Checklist

- [ ] `get_queryset()` uses `select_related`/`prefetch_related`
- [ ] No queries in serializer methods without prefetch
- [ ] Database indexes on filtered/ordered fields
- [ ] Pagination on all list endpoints
- [ ] `only()`/`defer()` used for specific field needs
- [ ] `annotate()` used instead of Python aggregation
- [ ] Bulk operations for multiple creates/updates
- [ ] Background tasks for slow operations

---

## Common Pitfalls to Avoid

- ❌ Accessing related objects without `select_related`/`prefetch_related`
- ❌ Business logic in views instead of services
- ❌ Using `fields = '__all__'` in serializers
- ❌ Missing pagination on list endpoints
- ❌ Filtering in Python instead of database queries
- ❌ Getting counts in loops instead of using `annotate()`
- ❌ Not indexing frequently filtered fields
- ❌ Duplicating query logic instead of using selectors
- ❌ Large files that should be split into services/selectors
- ❌ Not using async ORM methods in async views
- ❌ Using Celery for simple tasks when built-in Tasks work
