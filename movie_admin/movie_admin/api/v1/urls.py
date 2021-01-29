from django.urls import path

from movie_admin.api.v1 import views

urlpatterns = [
    path('movies/', views.MoviesListApi.as_view()),
    path('movies/<str:pk>', views.MovieDetailApi.as_view())
]
