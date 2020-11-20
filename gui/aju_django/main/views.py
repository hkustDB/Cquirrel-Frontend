import logging

from django.shortcuts import render
from django.http import HttpResponse, Http404
from django.http import HttpResponseRedirect
from django.urls import reverse
from django.http import FileResponse
from .forms import UploadJsonFileForm
from .utils import handle_uploaded_json_file, init_dir
from aju_django import config


logger = logging.getLogger(__name__)


def index(request):
    init_dir()
    if request.method == "POST":
        return upload_json_file(request)
    else:
        return render(request, 'index.html')


def upload_json_file(request):
    form = UploadJsonFileForm(request.POST, request.FILES)
    if form.is_valid():
        uploaded_json_file = request.FILES['uploaded_json_file']
        result = handle_uploaded_json_file(uploaded_json_file)
        return render(request, 'index.html', result)
    else:
        return render(request, 'index.html', {'upload_file_result': 'upload form is not valid.'})


def download_codegen_log(request):
    codegen_log_path = config.CODEGEN_LOG
    if codegen_log_path.exists():
        response = FileResponse(open(codegen_log_path, 'rb'))
        return response
    else:
        raise Http404("codegen log does not exist.")


def download_generated_jar(request):
    logger.info('here is view.')
    generated_jar_path = config.GENERATED_JAR
    if generated_jar_path.exists():
        response = FileResponse(open(generated_jar_path, 'rb'))
        return response
    else:
        raise Http404("generated jar does not exist.")