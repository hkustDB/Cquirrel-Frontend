from django import forms


class UploadJsonFileForm(forms.Form):
    uploaded_json_file = forms.FileField()
