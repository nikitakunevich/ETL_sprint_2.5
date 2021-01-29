import logging
from dataclasses import dataclass, asdict
from typing import Optional, List

from django.contrib.postgres.aggregates import ArrayAgg
from django.core.exceptions import ValidationError
from django.db.models import QuerySet, Q
from django.http import JsonResponse, Http404
from django.views.generic.detail import BaseDetailView
from django.views.generic.list import BaseListView

from movie_admin.models import FilmWork

logger = logging.getLogger(__name__)


@dataclass
class MovieListResult:
    count: int
    total_pages: int
    prev: Optional[int]
    next: Optional[int]
    results: List


class MoviesApiMixin:
    model = FilmWork

    def get_queryset(self):
        actors = ArrayAgg('persons__full_name', filter=Q(personfilmwork__role__exact='actor'))
        directors = ArrayAgg('persons__full_name', filter=Q(personfilmwork__role__exact='director'))
        writers = ArrayAgg('persons__full_name', filter=Q(personfilmwork__role__exact='writer'))

        # noinspection PyUnresolvedReferences
        queryset = super().get_queryset().prefetch_related('genres', 'personfilmwork_set', 'persons') \
            .annotate(actors=actors, writers=writers, directors=directors)
        return queryset.values()

    def render_to_response(self, context):
        return JsonResponse(context, json_dumps_params={'ensure_ascii': False}, safe=False)


class MoviesListApi(MoviesApiMixin, BaseListView):
    http_method_names = ['get']
    ordering = ['title']
    paginate_by = 50

    def get_context_data(self, **kwargs):
        context = super().get_context_data()

        paginator = context['paginator']
        page = context['page_obj']

        prev_page = page.previous_page_number() if page.has_previous() else None
        next_page = page.next_page_number() if page.has_next() else None

        film_works: QuerySet = context['object_list']

        return asdict(MovieListResult(count=paginator.count,
                                      total_pages=paginator.num_pages,
                                      prev=prev_page,
                                      next=next_page,
                                      results=list(film_works)))


class MovieDetailApi(MoviesApiMixin, BaseDetailView):
    http_method_names = ['get']

    def get_object(self, queryset=None):
        try:
            return super().get_object(queryset)
        except ValidationError:
            raise Http404()

    def get_context_data(self, **kwargs):
        return super().get_context_data(**kwargs)['object']
