import logging

from django.shortcuts import render
from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.urls import reverse
from .forms import UploadJsonFileForm
from .utils import handle_uploaded_json_file, init_dir

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
