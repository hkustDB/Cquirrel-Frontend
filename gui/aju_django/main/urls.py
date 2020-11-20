from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('download_codegen_log', views.download_codegen_log, name='download_codegen_log'),
    path('download_generated_jar', views.download_generated_jar, name='download_generated_jar'),
]