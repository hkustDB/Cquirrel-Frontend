from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    # path('', views.upload_json_file, name='upload_json_file'),
]