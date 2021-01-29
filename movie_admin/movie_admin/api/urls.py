from django.urls import path, include

urlpatterns = [
    path('v1/', include('movie_admin.api.v1.urls')),
]
